// Command tidepact-server runs a TidePact server over HTTP.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"tidepact/server"
)

func main() {
	addr := flag.String("addr", ":7070", "listen address")
	dbPath := flag.String("db", "tidepact-server.db", "server database path")
	activationTTL := flag.Duration("activation-ttl", 24*time.Hour, "delete client ids not activated within this time")
	compactEvery := flag.Duration("compact-every", 0, "compact the log at this interval (0 disables)")
	flag.Parse()

	store, err := server.Open(*dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go every(ctx, time.Minute, func() {
		if n, err := store.CleanupInactive(ctx, *activationTTL); err != nil {
			log.Printf("cleanup: %v", err)
		} else if n > 0 {
			log.Printf("cleanup: removed %d inactive client ids", n)
		}
	})
	if *compactEvery > 0 {
		go every(ctx, *compactEvery, func() {
			if err := store.Compact(ctx); err != nil {
				log.Printf("compact: %v", err)
			}
		})
	}

	srv := &http.Server{Addr: *addr, Handler: server.NewHandler(store)}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	log.Printf("tidepact-server listening on %s (db %s)", *addr, *dbPath)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func every(ctx context.Context, d time.Duration, fn func()) {
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			fn()
		}
	}
}
