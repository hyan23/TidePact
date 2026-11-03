// Package server implements the TidePact server: a single global log of
// accepted transactions, per-key current state, and per-client push chains.
package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"

	"tidepact/internal/sqlitex"
	"tidepact/wire"
)

// Store is the server-side database.
type Store struct {
	db  *sql.DB
	now func() time.Time
}

// Open opens or creates the server log at path.
func Open(path string) (*Store, error) {
	db, err := sqlitex.Open(path)
	if err != nil {
		return nil, err
	}
	err = sqlitex.Exec(db,
		`CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v INTEGER NOT NULL)`,
		`INSERT OR IGNORE INTO meta(k, v) VALUES ('last_seq', 0), ('compacted_seq', 0)`,
		`CREATE TABLE IF NOT EXISTS clients(
			client_id    TEXT PRIMARY KEY,
			activated    INTEGER NOT NULL,
			created_at   INTEGER NOT NULL,
			prev_push_id TEXT NOT NULL DEFAULT '',
			push_id      TEXT NOT NULL DEFAULT '',
			result       BLOB)`,
		// Current state of every key name.
		`CREATE TABLE IF NOT EXISTS kv(
			key     TEXT PRIMARY KEY,
			key_id  TEXT NOT NULL,
			version INTEGER NOT NULL,
			deleted INTEGER NOT NULL,
			value   BLOB,
			seq     INTEGER NOT NULL)`,
		// Global log: accepted transactions and their nodes.
		`CREATE TABLE IF NOT EXISTS txns(seq INTEGER PRIMARY KEY, client_id TEXT NOT NULL, push_id TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS nodes(
			seq     INTEGER NOT NULL,
			idx     INTEGER NOT NULL,
			key     TEXT NOT NULL,
			key_id  TEXT NOT NULL,
			version INTEGER NOT NULL,
			op      INTEGER NOT NULL,
			value   BLOB,
			PRIMARY KEY (seq, idx))`,
	)
	if err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db, now: time.Now}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func getMeta(tx *sql.Tx, k string) (int64, error) {
	var v int64
	err := tx.QueryRow(`SELECT v FROM meta WHERE k=?`, k).Scan(&v)
	return v, err
}

func setMeta(tx *sql.Tx, k string, v int64) error {
	_, err := tx.Exec(`UPDATE meta SET v=? WHERE k=?`, v, k)
	return err
}

// Register creates a new, not yet activated client id.
func (s *Store) Register(ctx context.Context) (string, error) {
	id := uuid.NewString()
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO clients(client_id, activated, created_at) VALUES (?, 0, ?)`,
		id, s.now().UnixNano())
	return id, err
}

// Activate marks a client id active. Idempotent.
func (s *Store) Activate(ctx context.Context, clientID string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE clients SET activated=1 WHERE client_id=?`, clientID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return wire.Errorf(wire.CodeUnknownClient, "client %s not found", clientID)
	}
	return nil
}

// CleanupInactive removes client ids that were never activated and are
// older than ttl. Activated clients never expire.
func (s *Store) CleanupInactive(ctx context.Context, ttl time.Duration) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM clients WHERE activated=0 AND created_at < ?`,
		s.now().Add(-ttl).UnixNano())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

type keyState struct {
	exists  bool
	keyID   string
	version int64
	deleted bool
	value   []byte
}

func (st keyState) live() bool { return st.exists && !st.deleted }

// Push applies a client push. See docs/design.md §3.
func (s *Store) Push(ctx context.Context, req *wire.PushRequest) (*wire.PushResponse, error) {
	if req.ClientID == "" || req.PushID == "" || req.PushID == req.PrevPushID {
		return nil, wire.Errorf(wire.CodeBadRequest, "client_id and a fresh push_id are required")
	}
	var resp *wire.PushResponse
	err := sqlitex.WithTx(ctx, s.db, func(tx *sql.Tx) error {
		var activated int
		var prev, cur string
		var cached []byte
		err := tx.QueryRow(`SELECT activated, prev_push_id, push_id, result FROM clients WHERE client_id=?`,
			req.ClientID).Scan(&activated, &prev, &cur, &cached)
		if errors.Is(err, sql.ErrNoRows) {
			return wire.Errorf(wire.CodeUnknownClient, "client %s not found", req.ClientID)
		}
		if err != nil {
			return err
		}
		if activated == 0 {
			return wire.Errorf(wire.CodeNotActivated, "client %s not activated", req.ClientID)
		}
		// Retry of the last push: return the stored result verbatim.
		if cur != "" && req.PushID == cur && req.PrevPushID == prev {
			resp = &wire.PushResponse{}
			return json.Unmarshal(cached, resp)
		}
		// The push chain must continue from the server's last push.
		if req.PrevPushID != cur {
			return wire.Errorf(wire.CodeForked, "push chain mismatch for client %s", req.ClientID)
		}

		lastSeq, err := getMeta(tx, "last_seq")
		if err != nil {
			return err
		}
		resp = &wire.PushResponse{PushID: req.PushID, Results: make([]wire.TxnResult, 0, len(req.Txns))}
		conflicted := map[string]bool{}
		for _, t := range req.Txns {
			ok := len(t.Nodes) > 0
			for _, n := range t.Nodes {
				if conflicted[n.Key] {
					ok = false // infected by an earlier conflict in this push
				}
			}
			var staged map[string]*keyState
			if ok {
				staged, ok, err = checkTxn(tx, t.Nodes)
				if err != nil {
					return err
				}
			}
			if !ok {
				for _, n := range t.Nodes {
					conflicted[n.Key] = true
				}
				resp.Results = append(resp.Results, wire.TxnResult{ID: t.ID})
				continue
			}
			lastSeq++
			if err := applyTxn(tx, lastSeq, req, t.Nodes, staged); err != nil {
				return err
			}
			resp.Results = append(resp.Results, wire.TxnResult{ID: t.ID, Accepted: true, Seq: lastSeq})
		}
		if err := setMeta(tx, "last_seq", lastSeq); err != nil {
			return err
		}
		blob, err := json.Marshal(resp)
		if err != nil {
			return err
		}
		_, err = tx.Exec(`UPDATE clients SET prev_push_id=?, push_id=?, result=? WHERE client_id=?`,
			req.PrevPushID, req.PushID, blob, req.ClientID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func loadKey(tx *sql.Tx, key string) (keyState, error) {
	var st keyState
	var deleted int
	err := tx.QueryRow(`SELECT key_id, version, deleted, value FROM kv WHERE key=?`, key).
		Scan(&st.keyID, &st.version, &deleted, &st.value)
	if errors.Is(err, sql.ErrNoRows) {
		return keyState{}, nil
	}
	if err != nil {
		return keyState{}, err
	}
	st.exists, st.deleted = true, deleted != 0
	return st, nil
}

// checkTxn validates the nodes of one transaction in order against the
// current state, returning the resulting key states if all are accepted.
func checkTxn(tx *sql.Tx, nodes []wire.Node) (map[string]*keyState, bool, error) {
	staged := map[string]*keyState{}
	for _, n := range nodes {
		st, ok := staged[n.Key]
		if !ok {
			cur, err := loadKey(tx, n.Key)
			if err != nil {
				return nil, false, err
			}
			st = &cur
			staged[n.Key] = st
		}
		switch n.Op {
		case wire.OpCreate:
			if st.live() || n.Version != 0 || n.KeyID == "" {
				return nil, false, nil
			}
			*st = keyState{exists: true, keyID: n.KeyID, version: 0, value: n.Value}
		case wire.OpUpdate, wire.OpDelete:
			if !st.live() || st.keyID != n.KeyID || n.Version != st.version+1 {
				return nil, false, nil
			}
			st.version = n.Version
			if n.Op == wire.OpDelete {
				st.deleted, st.value = true, nil
			} else {
				st.value = n.Value
			}
		default:
			return nil, false, nil
		}
	}
	return staged, true, nil
}

func applyTxn(tx *sql.Tx, seq int64, req *wire.PushRequest, nodes []wire.Node, staged map[string]*keyState) error {
	if _, err := tx.Exec(`INSERT INTO txns(seq, client_id, push_id) VALUES (?, ?, ?)`,
		seq, req.ClientID, req.PushID); err != nil {
		return err
	}
	for i, n := range nodes {
		if _, err := tx.Exec(`INSERT INTO nodes(seq, idx, key, key_id, version, op, value) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			seq, i, n.Key, n.KeyID, n.Version, int(n.Op), n.Value); err != nil {
			return err
		}
	}
	for key, st := range staged {
		deleted := 0
		if st.deleted {
			deleted = 1
		}
		if _, err := tx.Exec(`INSERT INTO kv(key, key_id, version, deleted, value, seq) VALUES (?, ?, ?, ?, ?, ?)
			ON CONFLICT(key) DO UPDATE SET key_id=excluded.key_id, version=excluded.version,
				deleted=excluded.deleted, value=excluded.value, seq=excluded.seq`,
			key, st.keyID, st.version, deleted, st.value, seq); err != nil {
			return err
		}
	}
	return nil
}

// Pull returns up to limit accepted transactions after seq `after`. It fails
// with CodeFullRequired if part of that range has been compacted away.
func (s *Store) Pull(ctx context.Context, after int64, limit int) (*wire.PullResponse, error) {
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	resp := &wire.PullResponse{Txns: []wire.ServerTxn{}}
	err := sqlitex.WithTx(ctx, s.db, func(tx *sql.Tx) error {
		compacted, err := getMeta(tx, "compacted_seq")
		if err != nil {
			return err
		}
		if after < compacted {
			return wire.Errorf(wire.CodeFullRequired, "log before seq %d was compacted", compacted)
		}
		rows, err := tx.Query(`SELECT seq, push_id FROM txns WHERE seq > ? ORDER BY seq LIMIT ?`, after, limit)
		if err != nil {
			return err
		}
		for rows.Next() {
			var t wire.ServerTxn
			if err := rows.Scan(&t.Seq, &t.PushID); err != nil {
				rows.Close()
				return err
			}
			resp.Txns = append(resp.Txns, t)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for i := range resp.Txns {
			t := &resp.Txns[i]
			rows, err := tx.Query(`SELECT key, key_id, version, op, value FROM nodes WHERE seq=? ORDER BY idx`, t.Seq)
			if err != nil {
				return err
			}
			for rows.Next() {
				var n wire.Node
				if err := rows.Scan(&n.Key, &n.KeyID, &n.Version, &n.Op, &n.Value); err != nil {
					rows.Close()
					return err
				}
				t.Nodes = append(t.Nodes, n)
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// Snapshot returns every live key's leaf node as of the current seq.
func (s *Store) Snapshot(ctx context.Context) (*wire.SnapshotResponse, error) {
	resp := &wire.SnapshotResponse{Nodes: []wire.SnapshotNode{}}
	err := sqlitex.WithTx(ctx, s.db, func(tx *sql.Tx) error {
		var err error
		if resp.Seq, err = getMeta(tx, "last_seq"); err != nil {
			return err
		}
		rows, err := tx.Query(`SELECT key, key_id, version, value, seq FROM kv WHERE deleted=0 ORDER BY key`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var n wire.SnapshotNode
			if err := rows.Scan(&n.Key, &n.KeyID, &n.Version, &n.Value, &n.Seq); err != nil {
				return err
			}
			n.Op = wire.OpUpdate
			if n.Version == 0 {
				n.Op = wire.OpCreate
			}
			resp.Nodes = append(resp.Nodes, n)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// Compact drops everything except live leaf nodes and client push progress.
// Clients whose pull cursor falls before this point must pull a snapshot.
func (s *Store) Compact(ctx context.Context) error {
	return sqlitex.WithTx(ctx, s.db, func(tx *sql.Tx) error {
		for _, q := range []string{
			`DELETE FROM nodes WHERE NOT EXISTS (SELECT 1 FROM kv
				WHERE kv.key=nodes.key AND kv.key_id=nodes.key_id AND kv.version=nodes.version AND kv.deleted=0)`,
			`DELETE FROM kv WHERE deleted=1`,
			`DELETE FROM txns WHERE NOT EXISTS (SELECT 1 FROM nodes WHERE nodes.seq=txns.seq)`,
			`UPDATE meta SET v=(SELECT v FROM meta WHERE k='last_seq') WHERE k='compacted_seq'`,
		} {
			if _, err := tx.Exec(q); err != nil {
				return err
			}
		}
		return nil
	})
}
