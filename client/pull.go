package client

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"tidepact/internal/sqlitex"
	"tidepact/wire"
)

const pullLimit = 256

// Pull fetches new server transactions into the local inbox (or a full
// snapshot if this is a new client or the server compacted past our cursor)
// and records the pulled position. It does not change visible data.
func (c *Client) Pull(ctx context.Context) error {
	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	return c.pullLocked(ctx)
}

func (c *Client) pullLocked(ctx context.Context) error {
	if c.Forked() {
		return ErrForked
	}
	after, ok, err := getMetaInt(c.db, "pulled_seq")
	if err != nil {
		return err
	}
	if ok {
		for {
			resp, err := c.tr.Pull(ctx, after, pullLimit)
			if wire.IsCode(err, wire.CodeFullRequired) {
				break
			}
			if err != nil {
				return err
			}
			if len(resp.Txns) == 0 {
				return nil
			}
			err = sqlitex.WithTx(ctx, c.db, func(tx *sql.Tx) error {
				for _, t := range resp.Txns {
					payload, err := json.Marshal(t)
					if err != nil {
						return err
					}
					if _, err := tx.Exec(`INSERT OR IGNORE INTO inbox(seq, kind, payload) VALUES (?, ?, ?)`, t.Seq, inboxTxn, payload); err != nil {
						return err
					}
				}
				after = resp.Txns[len(resp.Txns)-1].Seq
				return setMetaInt(tx, "pulled_seq", after)
			})
			if err != nil {
				return err
			}
			if len(resp.Txns) < pullLimit {
				return nil
			}
		}
	}

	snap, err := c.tr.Snapshot(ctx)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	return sqlitex.WithTx(ctx, c.db, func(tx *sql.Tx) error {
		// The snapshot supersedes anything not yet merged.
		if _, err := tx.Exec(`DELETE FROM inbox`); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO inbox(seq, kind, payload) VALUES (?, ?, ?)`, snap.Seq, inboxSnapshot, payload); err != nil {
			return err
		}
		return setMetaInt(tx, "pulled_seq", snap.Seq)
	})
}

// Merge applies pulled entries in server order, each atomically. It stops
// with ErrNeedPush when an entry touches a key that has unpushed changes;
// push and merge again.
func (c *Client) Merge(ctx context.Context) error {
	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	return c.mergeLocked(ctx)
}

func (c *Client) mergeLocked(ctx context.Context) error {
	for {
		done := false
		err := sqlitex.WithTx(ctx, c.db, func(tx *sql.Tx) error {
			var seq int64
			var kind int
			var payload []byte
			err := tx.QueryRow(`SELECT seq, kind, payload FROM inbox ORDER BY seq LIMIT 1`).Scan(&seq, &kind, &payload)
			if errors.Is(err, sql.ErrNoRows) {
				done = true
				return nil
			}
			if err != nil {
				return err
			}
			switch kind {
			case inboxTxn:
				var t wire.ServerTxn
				if err := json.Unmarshal(payload, &t); err != nil {
					return err
				}
				err = mergeTxn(tx, &t)
			case inboxSnapshot:
				var s wire.SnapshotResponse
				if err := json.Unmarshal(payload, &s); err != nil {
					return err
				}
				err = mergeSnapshot(tx, &s)
			default:
				err = fmt.Errorf("tidepact: unknown inbox kind %d", kind)
			}
			if err != nil {
				return err
			}
			if _, err := tx.Exec(`DELETE FROM inbox WHERE seq=?`, seq); err != nil {
				return err
			}
			return setMetaInt(tx, "merged_seq", seq)
		})
		if err != nil || done {
			return err
		}
	}
}

// insertSynced inserts a server node as effective on top of its chain.
func insertSynced(tx *sql.Tx, txnID int64, n *wire.Node, seq int64) error {
	parent, err := effNodeOfKeyID(tx, n.KeyID)
	if err != nil {
		return err
	}
	id, err := insertNode(tx, &node{
		txnID: txnID, key: n.Key, keyID: n.KeyID, version: n.Version,
		parent: parent, op: int(n.Op), value: n.Value, eff: true, seq: seq,
	})
	if err != nil {
		return err
	}
	return setEff(tx, n.Key, id)
}

// needsMerge decides one server node: merge, ignore, or block on push.
func needsMerge(tx *sql.Tx, n *wire.Node) (bool, error) {
	exists, unpushed, effVer, err := keyIDState(tx, n.KeyID)
	if err != nil {
		return false, err
	}
	if exists && n.Version <= effVer {
		return false, nil // already have it (e.g. our own accepted push)
	}
	if unpushed {
		return false, ErrNeedPush
	}
	return true, nil
}

func mergeTxn(tx *sql.Tx, t *wire.ServerTxn) error {
	var todo []*wire.Node
	for i := range t.Nodes {
		ok, err := needsMerge(tx, &t.Nodes[i])
		if err != nil {
			return err // ErrNeedPush blocks the whole transaction
		}
		if ok {
			todo = append(todo, &t.Nodes[i])
		}
	}
	if len(todo) == 0 {
		return nil
	}
	txnID, err := insertTxn(tx, originMerged, stSynced, sql.NullInt64{Int64: t.Seq, Valid: true})
	if err != nil {
		return err
	}
	touched := map[string]bool{}
	for _, n := range todo {
		if err := insertSynced(tx, txnID, n, t.Seq); err != nil {
			return err
		}
		touched[n.Key] = true
	}
	for k := range touched {
		if err := recomputeHead(tx, k); err != nil {
			return err
		}
	}
	return nil
}

// mergeSnapshot applies a full snapshot atomically: first delete local
// synced keys (as of the snapshot seq) whose key_id the snapshot lacks,
// then merge the snapshot's leaf nodes, allowing version jumps.
func mergeSnapshot(tx *sql.Tx, s *wire.SnapshotResponse) error {
	inSnap := make(map[string]string, len(s.Nodes)) // key -> key_id
	for _, n := range s.Nodes {
		inSnap[n.Key] = n.KeyID
	}

	var dels []*node
	rows, err := tx.Query(`SELECT n.node_id FROM heads h JOIN nodes n ON n.node_id=h.eff_node
		WHERE n.op != ? AND n.seq <= ?`, opDelete, s.Seq)
	if err != nil {
		return err
	}
	var effIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		effIDs = append(effIDs, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, id := range effIDs {
		n, err := loadNode(tx, id)
		if err != nil {
			return err
		}
		if inSnap[n.key] != n.keyID {
			if _, unpushed, _, err := keyIDState(tx, n.keyID); err != nil {
				return err
			} else if unpushed {
				return ErrNeedPush
			}
			dels = append(dels, n)
		}
	}

	var merges []*wire.Node
	for i := range s.Nodes {
		ok, err := needsMerge(tx, &s.Nodes[i].Node)
		if err != nil {
			return err
		}
		if ok {
			merges = append(merges, &s.Nodes[i].Node)
		}
	}
	if len(dels) == 0 && len(merges) == 0 {
		return nil
	}

	txnID, err := insertTxn(tx, originSnapshot, stSynced, sql.NullInt64{Int64: s.Seq, Valid: true})
	if err != nil {
		return err
	}
	touched := map[string]bool{}
	for _, d := range dels {
		id, err := insertNode(tx, &node{
			txnID: txnID, key: d.key, keyID: d.keyID, version: d.version + 1,
			parent: sql.NullInt64{Int64: d.id, Valid: true}, op: opDelete, eff: true, seq: s.Seq,
		})
		if err != nil {
			return err
		}
		if err := setEff(tx, d.key, id); err != nil {
			return err
		}
		touched[d.key] = true
	}
	seqOf := make(map[*wire.Node]int64, len(s.Nodes))
	for i := range s.Nodes {
		seqOf[&s.Nodes[i].Node] = s.Nodes[i].Seq
	}
	for _, n := range merges {
		if err := insertSynced(tx, txnID, n, seqOf[n]); err != nil {
			return err
		}
		touched[n.Key] = true
	}
	for k := range touched {
		if err := recomputeHead(tx, k); err != nil {
			return err
		}
	}
	return nil
}

// Sync runs push, pull and merge until merge is no longer blocked by
// unpushed changes (bounded number of rounds).
func (c *Client) Sync(ctx context.Context) error {
	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	for round := 0; round < 8; round++ {
		if err := c.pushLocked(ctx); err != nil {
			return err
		}
		if err := c.pullLocked(ctx); err != nil {
			return err
		}
		err := c.mergeLocked(ctx)
		if errors.Is(err, ErrNeedPush) {
			continue
		}
		return err
	}
	return ErrNeedPush
}

// StartBackgroundSync runs Sync every interval until StopBackgroundSync or
// Close. Errors are passed to onErr (may be nil). It stops on ErrForked.
func (c *Client) StartBackgroundSync(interval time.Duration, onErr func(error)) {
	c.StopBackgroundSync()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	c.bgStop, c.bgDone = cancel, done
	go func() {
		defer close(done)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			err := c.Sync(ctx)
			if err != nil && onErr != nil && ctx.Err() == nil {
				onErr(err)
			}
			if errors.Is(err, ErrForked) {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
}

// StopBackgroundSync stops a running background sync and waits for it.
func (c *Client) StopBackgroundSync() {
	if c.bgStop != nil {
		c.bgStop()
		<-c.bgDone
		c.bgStop, c.bgDone = nil, nil
	}
}
