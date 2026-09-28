package main

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestCleanComponentExitStopsSiblings(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := runAll(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)),
		named{"finished", func(context.Context) error { return nil }},
		named{"waiting", func(ctx context.Context) error { <-ctx.Done(); return nil }})
	if err != nil {
		t.Fatal(err)
	}
	if ctx.Err() != nil {
		t.Fatal("clean exit did not cancel sibling")
	}
}
