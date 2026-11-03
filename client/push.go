package client

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"tidepact/internal/sqlitex"
	"tidepact/wire"
)

// Push sends the pending push (or prepares a new one from all unpushed
// transactions) and records the result. A prepared push is retried with
// the same push_id until it succeeds.
func (c *Client) Push(ctx context.Context) error {
	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	return c.pushLocked(ctx)
}

// pushLocked completes a previously prepared push (if any), then pushes the
// transactions that were pending behind it.
func (c *Client) pushLocked(ctx context.Context) error {
	for i := 0; i < 2; i++ {
		pushed, err := c.pushOnce(ctx)
		if err != nil || !pushed {
			return err
		}
	}
	return nil
}

func (c *Client) pushOnce(ctx context.Context) (bool, error) {
	var req *wire.PushRequest
	err := sqlitex.WithTx(ctx, c.db, func(tx *sql.Tx) error {
		if err := checkWritable(tx); err != nil {
			return err
		}
		var err error
		req, err = preparePush(tx)
		return err
	})
	if err != nil || req == nil {
		return false, err
	}
	resp, err := c.tr.Push(ctx, req)
	if wire.IsCode(err, wire.CodeForked) {
		if err := setMeta(c.db, "forked", "1"); err != nil {
			return false, err
		}
		return false, ErrForked
	}
	if err != nil {
		return false, err
	}
	return true, sqlitex.WithTx(ctx, c.db, func(tx *sql.Tx) error { return applyPushed(tx, req, resp) })
}

// preparePush returns the already prepared push, or freezes all pending
// transactions into a new one. Returns nil if there is nothing to push.
func preparePush(tx *sql.Tx) (*wire.PushRequest, error) {
	var payload []byte
	err := tx.QueryRow(`SELECT payload FROM pushes WHERE status=1`).Scan(&payload)
	if err == nil {
		req := &wire.PushRequest{}
		return req, json.Unmarshal(payload, req)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}

	ids, err := txnIDs(tx, stPending)
	if err != nil || len(ids) == 0 {
		return nil, err
	}
	clientID, _, err := getMeta(tx, "client_id")
	if err != nil {
		return nil, err
	}
	prev, _, err := getMeta(tx, "last_push_id")
	if err != nil {
		return nil, err
	}
	req := &wire.PushRequest{ClientID: clientID, PrevPushID: prev, PushID: uuid.NewString()}
	for _, id := range ids {
		t := wire.PushTxn{ID: id}
		rows, err := tx.Query(`SELECT key, key_id, version, op, value FROM nodes WHERE txn_id=? AND op != ? ORDER BY node_id`, id, opConflict)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var n wire.Node
			if err := rows.Scan(&n.Key, &n.KeyID, &n.Version, &n.Op, &n.Value); err != nil {
				rows.Close()
				return nil, err
			}
			t.Nodes = append(t.Nodes, n)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
		req.Txns = append(req.Txns, t)
		if _, err := tx.Exec(`UPDATE txns SET status=?, push_id=? WHERE txn_id=?`, stPushing, req.PushID, id); err != nil {
			return nil, err
		}
	}
	if payload, err = json.Marshal(req); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(`INSERT INTO pushes(push_id, prev_push_id, status, payload) VALUES (?, ?, 1, ?)`,
		req.PushID, req.PrevPushID, payload); err != nil {
		return nil, err
	}
	return req, nil
}

func txnIDs(tx *sql.Tx, status int) ([]int64, error) {
	rows, err := tx.Query(`SELECT txn_id FROM txns WHERE status=? ORDER BY txn_id`, status)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func txnKeys(tx *sql.Tx, txnID int64) ([]string, error) {
	rows, err := tx.Query(`SELECT DISTINCT key FROM nodes WHERE txn_id=? AND op != ?`, txnID, opConflict)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	return keys, rows.Err()
}

// lastNodeOf returns the last node txnID wrote for key.
func lastNodeOf(tx *sql.Tx, txnID int64, key string) (*node, error) {
	var id int64
	if err := tx.QueryRow(`SELECT node_id FROM nodes WHERE txn_id=? AND key=? AND op != ? ORDER BY node_id DESC LIMIT 1`,
		txnID, key, opConflict).Scan(&id); err != nil {
		return nil, err
	}
	return loadNode(tx, id)
}

// markConflict marks txnID conflicted and appends a conflict node after its
// last node of every key it touched. Returns those keys.
func markConflict(tx *sql.Tx, txnID int64) ([]string, error) {
	if _, err := tx.Exec(`UPDATE txns SET status=? WHERE txn_id=?`, stConflict, txnID); err != nil {
		return nil, err
	}
	keys, err := txnKeys(tx, txnID)
	if err != nil {
		return nil, err
	}
	for _, k := range keys {
		last, err := lastNodeOf(tx, txnID, k)
		if err != nil {
			return nil, err
		}
		if _, err := insertNode(tx, &node{
			txnID: txnID, key: k, keyID: last.keyID, version: last.version + 1,
			parent: sql.NullInt64{Int64: last.id, Valid: true}, op: opConflict,
		}); err != nil {
			return nil, err
		}
	}
	return keys, nil
}

// applyPushed atomically records the push result: accepted transactions
// become effective, conflicted ones get conflict nodes, and the conflict
// spreads by key name to every later pending transaction.
func applyPushed(tx *sql.Tx, req *wire.PushRequest, resp *wire.PushResponse) error {
	var status int
	if err := tx.QueryRow(`SELECT status FROM pushes WHERE push_id=?`, req.PushID).Scan(&status); err != nil {
		return err
	}
	if status != 1 {
		return nil // already applied
	}
	results := make(map[int64]wire.TxnResult, len(resp.Results))
	for _, r := range resp.Results {
		results[r.ID] = r
	}
	conflicted := map[string]bool{}
	touched := map[string]bool{}

	for _, t := range req.Txns {
		r, ok := results[t.ID]
		if !ok {
			return fmt.Errorf("tidepact: push result missing txn %d", t.ID)
		}
		if !r.Accepted {
			keys, err := markConflict(tx, t.ID)
			if err != nil {
				return err
			}
			for _, k := range keys {
				conflicted[k], touched[k] = true, true
			}
			continue
		}
		if _, err := tx.Exec(`UPDATE txns SET status=?, server_seq=? WHERE txn_id=?`, stAccepted, r.Seq, t.ID); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE nodes SET eff=1, seq=? WHERE txn_id=? AND op != ?`, r.Seq, t.ID, opConflict); err != nil {
			return err
		}
		keys, err := txnKeys(tx, t.ID)
		if err != nil {
			return err
		}
		for _, k := range keys {
			last, err := lastNodeOf(tx, t.ID, k)
			if err != nil {
				return err
			}
			if err := setEff(tx, k, last.id); err != nil {
				return err
			}
			touched[k] = true
		}
	}

	// Transactions written while the push was in flight.
	if len(conflicted) > 0 {
		ids, err := txnIDs(tx, stPending)
		if err != nil {
			return err
		}
		for _, id := range ids {
			keys, err := txnKeys(tx, id)
			if err != nil {
				return err
			}
			infected := false
			for _, k := range keys {
				infected = infected || conflicted[k]
			}
			if !infected {
				continue
			}
			if _, err := markConflict(tx, id); err != nil {
				return err
			}
			for _, k := range keys {
				conflicted[k], touched[k] = true, true
			}
		}
	}

	for k := range touched {
		if err := recomputeHead(tx, k); err != nil {
			return err
		}
	}
	if err := setMeta(tx, "last_push_id", req.PushID); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE pushes SET status=2 WHERE push_id=?`, req.PushID); err != nil {
		return err
	}
	_, err := tx.Exec(`DELETE FROM pushes WHERE status=2 AND push_id != ?`, req.PushID)
	return err
}
