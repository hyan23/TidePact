// Package client implements the TidePact client: a local append-only KV log
// that syncs with the server in the background. See docs/design.md.
package client

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"sync"

	"github.com/google/uuid"

	"tidepact/internal/sqlitex"
	"tidepact/wire"
)

// Node operations stored locally.
const (
	opCreate   = int(wire.OpCreate)
	opUpdate   = int(wire.OpUpdate)
	opDelete   = int(wire.OpDelete)
	opConflict = 4
)

// Transaction status.
const (
	stPending  = 1 // local, not yet in a push
	stPushing  = 2 // frozen into a prepared push
	stAccepted = 3
	stConflict = 4
	stSynced   = 5 // came from the server (merge or snapshot)
)

// Transaction origin.
const (
	originLocal    = 1
	originMerged   = 2
	originSnapshot = 3
)

// Inbox entry kinds.
const (
	inboxTxn      = 1
	inboxSnapshot = 2
)

var (
	ErrNotFound     = errors.New("tidepact: key not found")
	ErrExists       = errors.New("tidepact: key already exists")
	ErrStale        = errors.New("tidepact: stale token")
	ErrForked       = errors.New("tidepact: this database copy was forked and is permanently rejected")
	ErrNotActivated = errors.New("tidepact: client not activated")
	ErrNeedPush     = errors.New("tidepact: merge blocked by unpushed changes")
)

// Client is one TidePact client database.
type Client struct {
	db     *sql.DB
	tr     Transport
	syncMu sync.Mutex // serializes push / pull / merge / compact

	bgStop context.CancelFunc
	bgDone chan struct{}
}

// Open opens (or creates) the local database at path and makes sure the
// client is registered and activated with the server.
func Open(ctx context.Context, path string, tr Transport) (*Client, error) {
	db, err := sqlitex.Open(path)
	if err != nil {
		return nil, err
	}
	err = sqlitex.Exec(db,
		`CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS txns(
			txn_id     INTEGER PRIMARY KEY AUTOINCREMENT,
			origin     INTEGER NOT NULL,
			status     INTEGER NOT NULL,
			push_id    TEXT,
			server_seq INTEGER)`,
		`CREATE INDEX IF NOT EXISTS txns_status ON txns(status, txn_id)`,
		`CREATE TABLE IF NOT EXISTS nodes(
			node_id   INTEGER PRIMARY KEY AUTOINCREMENT,
			txn_id    INTEGER NOT NULL,
			key       TEXT NOT NULL,
			key_id    TEXT NOT NULL,
			version   INTEGER NOT NULL,
			parent_id INTEGER,
			op        INTEGER NOT NULL,
			value     BLOB,
			eff       INTEGER NOT NULL, -- 1: effective (accepted / merged / synced)
			seq       INTEGER NOT NULL  -- server_seq for effective nodes
		)`,
		`CREATE INDEX IF NOT EXISTS nodes_key_id ON nodes(key_id, version)`,
		`CREATE INDEX IF NOT EXISTS nodes_key ON nodes(key)`,
		`CREATE INDEX IF NOT EXISTS nodes_txn ON nodes(txn_id)`,
		// Per key name: latest effective node and currently visible node.
		`CREATE TABLE IF NOT EXISTS heads(key TEXT PRIMARY KEY, eff_node INTEGER, head_node INTEGER)`,
		`CREATE TABLE IF NOT EXISTS pushes(
			push_id      TEXT PRIMARY KEY,
			prev_push_id TEXT NOT NULL,
			status       INTEGER NOT NULL, -- 1 prepared, 2 pushed
			payload      BLOB NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS inbox(seq INTEGER PRIMARY KEY, kind INTEGER NOT NULL, payload BLOB NOT NULL)`,
	)
	if err != nil {
		db.Close()
		return nil, err
	}
	c := &Client{db: db, tr: tr}
	if err := c.activate(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return c, nil
}

// Close stops background sync and closes the database.
func (c *Client) Close() error {
	c.StopBackgroundSync()
	return c.db.Close()
}

func (c *Client) activate(ctx context.Context) error {
	for attempt := 0; attempt < 3; attempt++ {
		if v, _, err := getMeta(c.db, "activated"); err != nil || v == "1" {
			return err
		}
		id, ok, err := getMeta(c.db, "client_id")
		if err != nil {
			return err
		}
		if !ok {
			if id, err = c.tr.Register(ctx); err != nil {
				return err
			}
			if err := setMeta(c.db, "client_id", id); err != nil {
				return err
			}
		}
		err = c.tr.Activate(ctx, id)
		if wire.IsCode(err, wire.CodeUnknownClient) {
			// The server cleaned it up before we activated: start over.
			if _, err := c.db.Exec(`DELETE FROM meta WHERE k='client_id'`); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		return setMeta(c.db, "activated", "1")
	}
	return ErrNotActivated
}

// ClientID returns the server-assigned client id.
func (c *Client) ClientID() string {
	id, _, _ := getMeta(c.db, "client_id")
	return id
}

// Forked reports whether this database copy was rejected by the server.
func (c *Client) Forked() bool {
	v, _, _ := getMeta(c.db, "forked")
	return v == "1"
}

// ---- meta helpers ----

type execer interface {
	Exec(query string, args ...any) (sql.Result, error)
	QueryRow(query string, args ...any) *sql.Row
}

func getMeta(q execer, k string) (string, bool, error) {
	var v string
	err := q.QueryRow(`SELECT v FROM meta WHERE k=?`, k).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return v, err == nil, err
}

func setMeta(q execer, k, v string) error {
	_, err := q.Exec(`INSERT INTO meta(k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v`, k, v)
	return err
}

func getMetaInt(q execer, k string) (int64, bool, error) {
	v, ok, err := getMeta(q, k)
	if err != nil || !ok {
		return 0, ok, err
	}
	n, err := strconv.ParseInt(v, 10, 64)
	return n, true, err
}

func setMetaInt(q execer, k string, v int64) error {
	return setMeta(q, k, strconv.FormatInt(v, 10))
}

func checkWritable(q execer) error {
	if v, _, err := getMeta(q, "forked"); err != nil {
		return err
	} else if v == "1" {
		return ErrForked
	}
	if v, _, err := getMeta(q, "activated"); err != nil {
		return err
	} else if v != "1" {
		return ErrNotActivated
	}
	return nil
}

// ---- nodes and heads ----

type node struct {
	id      int64
	txnID   int64
	key     string
	keyID   string
	version int64
	parent  sql.NullInt64
	op      int
	value   []byte
	eff     bool
	seq     int64
}

func loadNode(tx *sql.Tx, id int64) (*node, error) {
	n := &node{id: id}
	var eff int
	err := tx.QueryRow(`SELECT txn_id, key, key_id, version, parent_id, op, value, eff, seq FROM nodes WHERE node_id=?`, id).
		Scan(&n.txnID, &n.key, &n.keyID, &n.version, &n.parent, &n.op, &n.value, &eff, &n.seq)
	n.eff = eff == 1
	return n, err
}

func insertNode(tx *sql.Tx, n *node) (int64, error) {
	eff := 0
	if n.eff {
		eff = 1
	}
	res, err := tx.Exec(`INSERT INTO nodes(txn_id, key, key_id, version, parent_id, op, value, eff, seq)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		n.txnID, n.key, n.keyID, n.version, n.parent, n.op, n.value, eff, n.seq)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func insertTxn(tx *sql.Tx, origin, status int, serverSeq sql.NullInt64) (int64, error) {
	res, err := tx.Exec(`INSERT INTO txns(origin, status, server_seq) VALUES (?, ?, ?)`, origin, status, serverSeq)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// headNode returns the visible node of key, or nil.
func headNode(tx *sql.Tx, key string) (*node, error) {
	var id sql.NullInt64
	err := tx.QueryRow(`SELECT head_node FROM heads WHERE key=?`, key).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && !id.Valid) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return loadNode(tx, id.Int64)
}

// setEff records node id as the latest effective node of key.
func setEff(tx *sql.Tx, key string, id int64) error {
	_, err := tx.Exec(`INSERT INTO heads(key, eff_node, head_node) VALUES (?, ?, ?)
		ON CONFLICT(key) DO UPDATE SET eff_node=excluded.eff_node`, key, id, id)
	return err
}

// recomputeHead sets the visible node of key: the tip of its unpushed
// branch if any, else its latest effective node.
func recomputeHead(tx *sql.Tx, key string) error {
	var tip, eff sql.NullInt64
	if err := tx.QueryRow(`SELECT MAX(n.node_id) FROM nodes n JOIN txns t ON t.txn_id=n.txn_id
		WHERE n.key=? AND t.status IN (?, ?) AND n.op != ?`, key, stPending, stPushing, opConflict).Scan(&tip); err != nil {
		return err
	}
	err := tx.QueryRow(`SELECT eff_node FROM heads WHERE key=?`, key).Scan(&eff)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	head := eff
	if tip.Valid {
		head = tip
	}
	if !head.Valid {
		_, err = tx.Exec(`DELETE FROM heads WHERE key=?`, key)
		return err
	}
	_, err = tx.Exec(`INSERT INTO heads(key, eff_node, head_node) VALUES (?, ?, ?)
		ON CONFLICT(key) DO UPDATE SET head_node=excluded.head_node`, key, eff, head)
	return err
}

// keyIDState reports whether key_id exists locally, whether it has unpushed
// nodes, and its latest effective version (-1 if none).
func keyIDState(tx *sql.Tx, keyID string) (exists, unpushed bool, effVersion int64, err error) {
	var count int64
	var ver sql.NullInt64
	if err = tx.QueryRow(`SELECT COUNT(*), MAX(CASE WHEN eff=1 THEN version END) FROM nodes WHERE key_id=?`, keyID).
		Scan(&count, &ver); err != nil {
		return
	}
	exists = count > 0
	effVersion = -1
	if ver.Valid {
		effVersion = ver.Int64
	}
	var up int64
	if err = tx.QueryRow(`SELECT COUNT(*) FROM nodes n JOIN txns t ON t.txn_id=n.txn_id
		WHERE n.key_id=? AND t.status IN (?, ?) AND n.op != ?`, keyID, stPending, stPushing, opConflict).Scan(&up); err != nil {
		return
	}
	unpushed = up > 0
	return
}

// effNodeOfKeyID returns the latest effective node id of a chain.
func effNodeOfKeyID(tx *sql.Tx, keyID string) (sql.NullInt64, error) {
	var id sql.NullInt64
	err := tx.QueryRow(`SELECT node_id FROM nodes WHERE key_id=? AND eff=1 ORDER BY version DESC, node_id DESC LIMIT 1`, keyID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return sql.NullInt64{}, nil
	}
	return id, err
}

// ---- reads ----

// Item is a visible key. Token identifies the exact node (and therefore the
// branch) that was read; pass it to Update / Delete.
type Item struct {
	Key     string
	KeyID   string
	Version int64
	Token   int64
	Value   []byte
}

// Get returns the visible value of key.
func (c *Client) Get(ctx context.Context, key string) (*Item, error) {
	var item *Item
	err := sqlitex.WithTx(ctx, c.db, func(tx *sql.Tx) error {
		n, err := headNode(tx, key)
		if err != nil {
			return err
		}
		if n == nil || n.op == opDelete {
			return ErrNotFound
		}
		item = &Item{Key: key, KeyID: n.keyID, Version: n.version, Token: n.id, Value: n.value}
		return nil
	})
	return item, err
}

// Scan returns all visible keys with the given prefix, ordered by key.
func (c *Client) Scan(ctx context.Context, prefix string) ([]Item, error) {
	var items []Item
	err := sqlitex.WithTx(ctx, c.db, func(tx *sql.Tx) error {
		rows, err := tx.Query(`SELECT h.key, n.key_id, n.version, n.node_id, n.value
			FROM heads h JOIN nodes n ON n.node_id=h.head_node
			WHERE n.op != ? AND substr(h.key, 1, ?) = ? ORDER BY h.key`, opDelete, len(prefix), prefix)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var it Item
			if err := rows.Scan(&it.Key, &it.KeyID, &it.Version, &it.Token, &it.Value); err != nil {
				return err
			}
			items = append(items, it)
		}
		return rows.Err()
	})
	return items, err
}

// ---- writes ----

// Op is one write in a transaction.
type Op struct {
	kind  int
	key   string
	token int64
	value []byte
}

// Create adds key; it must not currently exist.
func Create(key string, value []byte) Op { return Op{kind: opCreate, key: key, value: value} }

// Update replaces the value of key; token must be the visible node's token.
// Within one transaction all ops on a key carry the token read before it
// (0 if the key was created earlier in the same transaction).
func Update(key string, token int64, value []byte) Op {
	return Op{kind: opUpdate, key: key, token: token, value: value}
}

// Delete removes key; token as for Update.
func Delete(key string, token int64) Op { return Op{kind: opDelete, key: key, token: token} }

// Commit atomically applies ops as one transaction.
func (c *Client) Commit(ctx context.Context, ops ...Op) error {
	if len(ops) == 0 {
		return nil
	}
	return sqlitex.WithTx(ctx, c.db, func(tx *sql.Tx) error {
		if err := checkWritable(tx); err != nil {
			return err
		}
		txnID, err := insertTxn(tx, originLocal, stPending, sql.NullInt64{})
		if err != nil {
			return err
		}

		type pnode struct {
			node
			parentIdx int // index into pending nodes, or -1
		}
		type kstate struct {
			base      int64 // visible node before this txn (0 if absent)
			present   bool
			keyID     string
			version   int64
			parentID  int64 // existing node id of chain tip, or 0
			parentIdx int   // pending node index of chain tip, or -1
			last      int   // last pending node for this key, or -1
		}
		var pn []pnode
		states := map[string]*kstate{}
		var order []string

		for _, op := range ops {
			s, ok := states[op.key]
			if !ok {
				s = &kstate{parentIdx: -1, last: -1}
				h, err := headNode(tx, op.key)
				if err != nil {
					return err
				}
				if h != nil && h.op != opDelete {
					s.base, s.present, s.keyID, s.version, s.parentID = h.id, true, h.keyID, h.version, h.id
				}
				states[op.key] = s
				order = append(order, op.key)
			}
			appendNode := func(kind int, keyID string, version int64, value []byte) {
				p := pnode{node: node{txnID: txnID, key: op.key, keyID: keyID, version: version, op: kind, value: value}, parentIdx: s.parentIdx}
				if s.parentIdx < 0 && s.parentID != 0 {
					p.parent = sql.NullInt64{Int64: s.parentID, Valid: true}
				}
				pn = append(pn, p)
				s.last, s.parentIdx, s.parentID = len(pn)-1, len(pn)-1, 0
				s.keyID, s.version = keyID, version
			}
			switch op.kind {
			case opCreate:
				if s.present {
					return ErrExists
				}
				s.parentIdx, s.parentID = -1, 0 // a new chain
				appendNode(opCreate, uuid.NewString(), 0, op.value)
				s.present = true
			case opUpdate, opDelete:
				if !s.present {
					return ErrNotFound
				}
				if op.token != s.base {
					return ErrStale
				}
				if s.last >= 0 && pn[s.last].keyID == s.keyID && (pn[s.last].op == opUpdate || (op.kind == opUpdate && pn[s.last].op == opCreate)) {
					// Collapse into this txn's node for the same chain.
					if op.kind == opDelete {
						pn[s.last].op, pn[s.last].value = opDelete, nil
					} else {
						pn[s.last].value = op.value
					}
				} else {
					appendNode(op.kind, s.keyID, s.version+1, op.value)
				}
				s.present = op.kind == opUpdate
			default:
				return errors.New("tidepact: invalid op")
			}
		}

		ids := make([]int64, len(pn))
		for i := range pn {
			if pn[i].parentIdx >= 0 {
				pn[i].parent = sql.NullInt64{Int64: ids[pn[i].parentIdx], Valid: true}
			}
			if ids[i], err = insertNode(tx, &pn[i].node); err != nil {
				return err
			}
		}
		for _, k := range order {
			if err := recomputeHead(tx, k); err != nil {
				return err
			}
		}
		return nil
	})
}
