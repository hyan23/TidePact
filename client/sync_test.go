package client

import (
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"tidepact/server"
	"tidepact/wire"
)

var ctx = context.Background()

type env struct {
	t     *testing.T
	dir   string
	store *server.Store
	url   string
}

func newEnv(t *testing.T) *env {
	t.Helper()
	dir := t.TempDir()
	store, err := server.Open(filepath.Join(dir, "server.db"))
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(server.NewHandler(store))
	t.Cleanup(func() { srv.Close(); store.Close() })
	return &env{t: t, dir: dir, store: store, url: srv.URL}
}

// hookTransport lets tests intercept calls.
type hookTransport struct {
	Transport
	beforePush func(*wire.PushRequest)
	dropPush   atomic.Int32 // number of push responses to lose after the server applied them
	failActive atomic.Int32 // number of Activate calls to fail with unknown_client
}

func (h *hookTransport) Push(ctx context.Context, req *wire.PushRequest) (*wire.PushResponse, error) {
	if h.beforePush != nil {
		h.beforePush(req)
	}
	resp, err := h.Transport.Push(ctx, req)
	if err == nil && h.dropPush.Load() > 0 {
		h.dropPush.Add(-1)
		return nil, errors.New("connection lost")
	}
	return resp, err
}

func (h *hookTransport) Activate(ctx context.Context, id string) error {
	if h.failActive.Load() > 0 {
		h.failActive.Add(-1)
		return wire.Errorf(wire.CodeUnknownClient, "gone")
	}
	return h.Transport.Activate(ctx, id)
}

func (e *env) open(name string) (*Client, *hookTransport) {
	e.t.Helper()
	tr := &hookTransport{Transport: NewHTTPTransport(e.url)}
	c, err := Open(ctx, filepath.Join(e.dir, name), tr)
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { c.Close() })
	return c, tr
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func get(t *testing.T, c *Client, key string) *Item {
	t.Helper()
	it, err := c.Get(ctx, key)
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	return it
}

func wantValue(t *testing.T, c *Client, key, want string) {
	t.Helper()
	if got := string(get(t, c, key).Value); got != want {
		t.Fatalf("%s = %q, want %q", key, got, want)
	}
}

func wantAbsent(t *testing.T, c *Client, key string) {
	t.Helper()
	if it, err := c.Get(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("%s: want not found, got %+v %v", key, it, err)
	}
}

func put(t *testing.T, c *Client, key, value string) {
	t.Helper()
	it, err := c.Get(ctx, key)
	if errors.Is(err, ErrNotFound) {
		must(t, c.Commit(ctx, Create(key, []byte(value))))
		return
	}
	must(t, err)
	must(t, c.Commit(ctx, Update(key, it.Token, []byte(value))))
}

func TestLocalWrites(t *testing.T) {
	e := newEnv(t)
	c, _ := e.open("c.db")

	must(t, c.Commit(ctx, Create("a", []byte("1"))))
	a := get(t, c, "a")
	if a.Version != 0 {
		t.Fatalf("version %d", a.Version)
	}
	if err := c.Commit(ctx, Create("a", []byte("x"))); !errors.Is(err, ErrExists) {
		t.Fatalf("want ErrExists, got %v", err)
	}
	// Several writes to one key in one txn collapse into one version.
	must(t, c.Commit(ctx, Update("a", a.Token, []byte("2")), Update("a", a.Token, []byte("3"))))
	a2 := get(t, c, "a")
	if a2.Version != 1 || string(a2.Value) != "3" {
		t.Fatalf("got %+v", a2)
	}
	// Old token is stale; the whole txn fails atomically.
	err := c.Commit(ctx, Create("b", []byte("b")), Update("a", a.Token, []byte("bad")))
	if !errors.Is(err, ErrStale) {
		t.Fatalf("want ErrStale, got %v", err)
	}
	wantAbsent(t, c, "b")
	// Delete then recreate in one txn: new key_id, version 0.
	must(t, c.Commit(ctx, Delete("a", a2.Token), Create("a", []byte("new"))))
	a3 := get(t, c, "a")
	if a3.KeyID == a.KeyID || a3.Version != 0 || string(a3.Value) != "new" {
		t.Fatalf("got %+v", a3)
	}
	must(t, c.Commit(ctx, Delete("a", a3.Token)))
	wantAbsent(t, c, "a")
}

func TestSyncAndConflict(t *testing.T) {
	e := newEnv(t)
	c1, _ := e.open("c1.db")
	c2, _ := e.open("c2.db")

	put(t, c1, "a", "v0")
	must(t, c1.Sync(ctx))
	must(t, c2.Sync(ctx))
	wantValue(t, c2, "a", "v0")

	put(t, c1, "a", "c1")
	put(t, c2, "a", "c2")
	stale := get(t, c2, "a")
	must(t, c1.Sync(ctx))
	must(t, c2.Sync(ctx)) // c2's update conflicts, then c1's is merged
	wantValue(t, c2, "a", "c1")
	if err := c2.Commit(ctx, Update("a", stale.Token, []byte("x"))); !errors.Is(err, ErrStale) {
		t.Fatalf("want ErrStale, got %v", err)
	}
	put(t, c2, "a", "c2-again")
	must(t, c2.Sync(ctx))
	must(t, c1.Sync(ctx))
	wantValue(t, c1, "a", "c2-again")
}

func TestConflictInfection(t *testing.T) {
	e := newEnv(t)
	c1, _ := e.open("c1.db")
	c2, _ := e.open("c2.db")
	put(t, c1, "a", "a0")
	put(t, c1, "b", "b0")
	must(t, c1.Sync(ctx))
	must(t, c2.Sync(ctx))

	put(t, c1, "a", "a1")
	must(t, c1.Sync(ctx))

	put(t, c2, "a", "a-c2") // T1: will conflict
	a, b := get(t, c2, "a"), get(t, c2, "b")
	must(t, c2.Commit(ctx, Update("a", a.Token, []byte("a-c2b")), Update("b", b.Token, []byte("b-c2")))) // T2: infected via a
	put(t, c2, "c", "c-c2")                                                                             // T3: independent
	must(t, c2.Sync(ctx))

	wantValue(t, c2, "a", "a1")
	wantValue(t, c2, "b", "b0") // reverted: infected by name
	wantValue(t, c2, "c", "c-c2")
	must(t, c1.Sync(ctx))
	wantValue(t, c1, "b", "b0")
	wantValue(t, c1, "c", "c-c2")
}

func TestInfectionOfWritesDuringPush(t *testing.T) {
	e := newEnv(t)
	c1, _ := e.open("c1.db")
	c2, tr2 := e.open("c2.db")
	put(t, c1, "a", "a0")
	put(t, c1, "b", "b0")
	must(t, c1.Sync(ctx))
	must(t, c2.Sync(ctx))
	put(t, c1, "a", "a1")
	must(t, c1.Sync(ctx))

	put(t, c2, "a", "a-c2")
	tr2.beforePush = func(*wire.PushRequest) {
		tr2.beforePush = nil
		// Written after prepare, based on the in-flight (soon conflicting) node.
		a, b := get(t, c2, "a"), get(t, c2, "b")
		must(t, c2.Commit(ctx, Update("a", a.Token, []byte("a-more")), Update("b", b.Token, []byte("b-more"))))
		put(t, c2, "d", "d-ok")
	}
	must(t, c2.Push(ctx))
	wantValue(t, c2, "b", "b0")
	wantValue(t, c2, "d", "d-ok")
	must(t, c2.Sync(ctx))
	wantValue(t, c2, "a", "a1")
	must(t, c1.Sync(ctx))
	wantValue(t, c1, "b", "b0")
	wantValue(t, c1, "d", "d-ok")
}

func TestServerInfectsWithinPush(t *testing.T) {
	e := newEnv(t)
	c1, _ := e.open("c1.db")
	c2, _ := e.open("c2.db")
	put(t, c1, "a", "a0")
	must(t, c1.Sync(ctx))
	must(t, c2.Sync(ctx))

	must(t, c1.Commit(ctx, Delete("a", get(t, c1, "a").Token)))
	must(t, c1.Sync(ctx))

	// c2: T1 delete a (conflicts: already deleted on server); T2 recreate a.
	// The server would accept T2's create on its own, but it is infected.
	must(t, c2.Commit(ctx, Delete("a", get(t, c2, "a").Token)))
	must(t, c2.Commit(ctx, Create("a", []byte("recreated"))))
	must(t, c2.Sync(ctx))
	wantAbsent(t, c2, "a")
	must(t, c1.Sync(ctx))
	wantAbsent(t, c1, "a")

	put(t, c2, "a", "fresh")
	must(t, c2.Sync(ctx))
	must(t, c1.Sync(ctx))
	wantValue(t, c1, "a", "fresh")
}

func TestPushRetryIsIdempotent(t *testing.T) {
	e := newEnv(t)
	c1, tr1 := e.open("c1.db")
	c2, _ := e.open("c2.db")

	put(t, c1, "a", "a0")
	tr1.dropPush.Store(1)
	if err := c1.Push(ctx); err == nil {
		t.Fatal("want lost response")
	}
	put(t, c1, "b", "b0") // goes into the next push
	// Our own accepted txn is pulled while the push is unconfirmed: merge
	// blocks, Sync retries the same push_id and then ignores the echo.
	must(t, c1.Pull(ctx))
	if err := c1.Merge(ctx); !errors.Is(err, ErrNeedPush) {
		t.Fatalf("want ErrNeedPush, got %v", err)
	}
	must(t, c1.Sync(ctx))
	wantValue(t, c1, "a", "a0")
	if it := get(t, c1, "a"); it.Version != 0 {
		t.Fatalf("version %d", it.Version)
	}

	must(t, c2.Sync(ctx))
	wantValue(t, c2, "a", "a0")
	wantValue(t, c2, "b", "b0")
	resp, err := e.store.Pull(ctx, 0, 100)
	must(t, err)
	if len(resp.Txns) != 2 {
		t.Fatalf("server has %d txns, want 2", len(resp.Txns))
	}
}

func TestFullPullAfterCompaction(t *testing.T) {
	e := newEnv(t)
	c1, _ := e.open("c1.db")
	c2, _ := e.open("c2.db")
	put(t, c1, "a", "a0")
	put(t, c1, "b", "b0")
	put(t, c1, "c", "c0")
	put(t, c1, "e", "e0")
	must(t, c1.Sync(ctx))
	must(t, c2.Sync(ctx))
	oldA := get(t, c2, "a")

	must(t, c1.Commit(ctx, Delete("b", get(t, c1, "b").Token)))
	must(t, c1.Commit(ctx, Delete("a", get(t, c1, "a").Token), Create("a", []byte("a-new"))))
	for i := 0; i < 5; i++ {
		put(t, c1, "c", "c-final")
	}
	must(t, c1.Commit(ctx, Delete("e", get(t, c1, "e").Token)))
	must(t, c1.Sync(ctx))
	must(t, e.store.Compact(ctx))

	put(t, c2, "e", "e-c2") // unpushed change on a key deleted (and compacted) on the server
	must(t, c2.Sync(ctx))
	wantAbsent(t, c2, "b")
	wantAbsent(t, c2, "e")
	a := get(t, c2, "a")
	if a.KeyID == oldA.KeyID || string(a.Value) != "a-new" {
		t.Fatalf("a = %+v", a)
	}
	if c := get(t, c2, "c"); c.Version != 5 || string(c.Value) != "c-final" {
		t.Fatalf("c = %+v (want version jump to 5)", c)
	}

	// Incremental pull keeps working after the snapshot.
	put(t, c1, "z", "z0")
	must(t, c1.Sync(ctx))
	must(t, c2.Sync(ctx))
	wantValue(t, c2, "z", "z0")

	// A brand-new client gets everything via snapshot.
	c3, _ := e.open("c3.db")
	must(t, c3.Sync(ctx))
	items, err := c3.Scan(ctx, "")
	must(t, err)
	if len(items) != 3 {
		t.Fatalf("c3 has %d keys: %+v", len(items), items)
	}
}

func copyDB(t *testing.T, from, to string) {
	t.Helper()
	for _, suffix := range []string{"", "-wal", "-shm"} {
		src, err := os.Open(from + suffix)
		if os.IsNotExist(err) {
			continue
		}
		must(t, err)
		dst, err := os.Create(to + suffix)
		must(t, err)
		_, err = io.Copy(dst, src)
		must(t, err)
		src.Close()
		dst.Close()
	}
}

func TestForkedCopyIsRejected(t *testing.T) {
	e := newEnv(t)
	tr := &hookTransport{Transport: NewHTTPTransport(e.url)}
	path := filepath.Join(e.dir, "orig.db")
	orig, err := Open(ctx, path, tr)
	must(t, err)
	put(t, orig, "a", "a0")
	must(t, orig.Sync(ctx))
	must(t, orig.Close())

	copyDB(t, path, filepath.Join(e.dir, "copy.db"))
	orig, _ = e.open("orig.db")
	cp, _ := e.open("copy.db")
	if orig.ClientID() != cp.ClientID() {
		t.Fatal("copy should share client id")
	}

	put(t, orig, "a", "from-orig")
	put(t, cp, "a", "from-copy")
	must(t, orig.Sync(ctx))
	if err := cp.Sync(ctx); !errors.Is(err, ErrForked) {
		t.Fatalf("want ErrForked, got %v", err)
	}
	if err := cp.Commit(ctx, Create("x", nil)); !errors.Is(err, ErrForked) {
		t.Fatalf("want ErrForked on write, got %v", err)
	}
	if err := cp.Pull(ctx); !errors.Is(err, ErrForked) {
		t.Fatalf("want ErrForked on pull, got %v", err)
	}
	put(t, orig, "a", "still-fine")
	must(t, orig.Sync(ctx))
}

func TestCopyDuringPreparedPush(t *testing.T) {
	e := newEnv(t)
	tr := &hookTransport{Transport: NewHTTPTransport(e.url)}
	path := filepath.Join(e.dir, "orig.db")
	orig, err := Open(ctx, path, tr)
	must(t, err)
	put(t, orig, "a", "a0")
	tr.dropPush.Store(1)
	if err := orig.Push(ctx); err == nil {
		t.Fatal("want lost response")
	}
	must(t, orig.Close())
	copyDB(t, path, filepath.Join(e.dir, "copy.db"))

	orig, _ = e.open("orig.db")
	cp, _ := e.open("copy.db")
	// Both retry the same prepared push: both get the stored result.
	must(t, orig.Push(ctx))
	must(t, cp.Push(ctx))
	wantValue(t, orig, "a", "a0")
	wantValue(t, cp, "a", "a0")

	put(t, cp, "b", "copy-first")
	put(t, orig, "b", "orig")
	must(t, cp.Sync(ctx))
	if err := orig.Sync(ctx); !errors.Is(err, ErrForked) {
		t.Fatalf("want ErrForked, got %v", err)
	}
}

func TestStaleTokenAcrossConflictFork(t *testing.T) {
	e := newEnv(t)
	c1, _ := e.open("c1.db")
	c2, _ := e.open("c2.db")
	put(t, c1, "a", "v4")
	must(t, c1.Sync(ctx))
	must(t, c2.Sync(ctx))
	put(t, c1, "a", "server")
	must(t, c1.Sync(ctx))

	put(t, c2, "a", "x") // conflict branch node, version 1
	y := get(t, c2, "a") // Y reads the doomed node
	must(t, c2.Push(ctx))
	put(t, c2, "a", "x2") // new branch from the old effective node: also version 1
	if got := get(t, c2, "a"); got.Version != y.Version {
		t.Fatalf("versions should collide: %d vs %d", got.Version, y.Version)
	}
	if err := c2.Commit(ctx, Update("a", y.Token, []byte("y"))); !errors.Is(err, ErrStale) {
		t.Fatalf("want ErrStale, got %v", err)
	}
}

func TestLocalCompaction(t *testing.T) {
	e := newEnv(t)
	c1, _ := e.open("c1.db")
	c2, _ := e.open("c2.db")
	must(t, c2.Sync(ctx)) // start c2 incrementally so it receives every version
	for i := 0; i < 5; i++ {
		put(t, c1, "a", "a-final")
		put(t, c1, "b", "b-final")
	}
	put(t, c1, "gone", "x")
	must(t, c1.Commit(ctx, Delete("gone", get(t, c1, "gone").Token)))
	must(t, c1.Sync(ctx))
	must(t, c2.Sync(ctx))
	put(t, c1, "a", "server")
	must(t, c1.Sync(ctx))
	put(t, c2, "a", "doomed")
	must(t, c2.Push(ctx)) // conflict branch on a
	put(t, c2, "b", "unpushed")

	count := func(c *Client) (n int) {
		must(t, c.db.QueryRow(`SELECT COUNT(*) FROM nodes`).Scan(&n))
		return
	}
	before := count(c2)
	must(t, c2.Compact(ctx))
	must(t, c1.Compact(ctx))
	after := count(c2)
	// Kept: a's effective node + conflict branch (2 nodes), b's effective + unpushed.
	if after != 5 || after >= before {
		t.Fatalf("nodes before=%d after=%d", before, after)
	}
	wantValue(t, c2, "a", "a-final")
	wantValue(t, c2, "b", "unpushed")
	wantAbsent(t, c2, "gone")
	if n := count(c1); n != 2 {
		t.Fatalf("c1 nodes after compaction = %d, want 2", n)
	}

	must(t, c2.Sync(ctx))
	must(t, c1.Sync(ctx))
	wantValue(t, c2, "a", "server")
	wantValue(t, c1, "b", "unpushed")
}

func TestActivationRetry(t *testing.T) {
	e := newEnv(t)
	tr := &hookTransport{Transport: NewHTTPTransport(e.url)}
	tr.failActive.Store(1)
	c, err := Open(ctx, filepath.Join(e.dir, "c.db"), tr)
	must(t, err)
	defer c.Close()
	put(t, c, "a", "ok")
	must(t, c.Sync(ctx))
}
