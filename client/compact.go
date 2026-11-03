package client

import (
	"context"
	"database/sql"

	"tidepact/internal/sqlitex"
)

// Compact prunes the local log (docs/design.md §5):
//   - unpushed nodes are kept, with their ancestors back to the first
//     effective node;
//   - each key's latest effective node is kept unless it is a delete;
//   - conflict branches are kept from the conflict leaf back to the fork
//     point (first effective node), or entirely if there is none;
//   - everything else is dropped.
func (c *Client) Compact(ctx context.Context) error {
	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	return sqlitex.WithTx(ctx, c.db, func(tx *sql.Tx) error {
		type info struct {
			parent   int64
			op       int
			eff      bool
			unpushed bool
		}
		all := map[int64]*info{}
		rows, err := tx.Query(`SELECT n.node_id, n.parent_id, n.op, n.eff, t.status
			FROM nodes n JOIN txns t ON t.txn_id=n.txn_id`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var id int64
			var parent sql.NullInt64
			var op, eff, status int
			if err := rows.Scan(&id, &parent, &op, &eff, &status); err != nil {
				rows.Close()
				return err
			}
			all[id] = &info{parent: parent.Int64, op: op, eff: eff == 1,
				unpushed: status == stPending || status == stPushing}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}

		keep := map[int64]bool{}
		walk := func(id int64) { // keep id and its ancestors up to the first effective node
			for id != 0 {
				n, ok := all[id]
				if !ok || keep[id] {
					return
				}
				keep[id] = true
				if n.eff {
					return
				}
				id = n.parent
			}
		}
		for id, n := range all {
			if (n.unpushed && n.op != opConflict) || n.op == opConflict {
				walk(id)
			}
		}

		type head struct {
			key       string
			eff, head sql.NullInt64
		}
		var heads []head
		rows, err = tx.Query(`SELECT key, eff_node, head_node FROM heads`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var h head
			if err := rows.Scan(&h.key, &h.eff, &h.head); err != nil {
				rows.Close()
				return err
			}
			heads = append(heads, h)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, h := range heads {
			if h.eff.Valid {
				if n, ok := all[h.eff.Int64]; ok && n.op != opDelete {
					keep[h.eff.Int64] = true
				}
			}
		}

		for _, h := range heads {
			eff, hd := h.eff, h.head
			if eff.Valid && !keep[eff.Int64] {
				eff = sql.NullInt64{}
			}
			if hd.Valid && !keep[hd.Int64] {
				hd = sql.NullInt64{}
			}
			if !eff.Valid && !hd.Valid {
				if _, err := tx.Exec(`DELETE FROM heads WHERE key=?`, h.key); err != nil {
					return err
				}
			} else if eff != h.eff || hd != h.head {
				if _, err := tx.Exec(`UPDATE heads SET eff_node=?, head_node=? WHERE key=?`, eff, hd, h.key); err != nil {
					return err
				}
			}
		}

		for id := range all {
			if !keep[id] {
				if _, err := tx.Exec(`DELETE FROM nodes WHERE node_id=?`, id); err != nil {
					return err
				}
			}
		}
		if _, err := tx.Exec(`DELETE FROM txns WHERE txn_id NOT IN (SELECT DISTINCT txn_id FROM nodes)`); err != nil {
			return err
		}
		_, err = tx.Exec(`DELETE FROM pushes WHERE status=2 AND push_id NOT IN (SELECT v FROM meta WHERE k='last_push_id')`)
		return err
	})
}
