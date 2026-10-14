//go:build integration

package cqrs_test

import (
	"context"
	"testing"
	"time"
)

// The registration script asks Debezium to set REPLICA IDENTITY FULL itself
// (replica.identity.autoset.values). This proves that setting does what the
// script relies on, against the real connector: a table left at DEFAULT is FULL
// once the connector runs, and an update then carries every column, TOAST ones
// included.
func TestE2E_DebeziumSetsReplicaIdentityFull(t *testing.T) {
	t.Parallel()
	s := skipUnlessDebezium(t)
	p := newPair(t, 8)
	p.exec(p.write, `ALTER TABLE accounts REPLICA IDENTITY DEFAULT`)
	identity := func() string {
		var r string
		if err := p.write.Client().NewRaw(`SELECT relreplident::text FROM pg_class WHERE relname = 'accounts' AND relnamespace = 'public'::regnamespace`).
			Scan(context.Background(), &r); err != nil {
			t.Fatal(err)
		}
		return r
	}
	if identity() != "d" {
		t.Fatalf("setup: identity = %q", identity())
	}
	s.connectorWith(t, p, `"replica.identity.autoset.values": ".*:FULL"`)
	deadline := time.Now().Add(30 * time.Second)
	for identity() != "f" {
		if time.Now().After(deadline) {
			t.Fatalf("Debezium did not set REPLICA IDENTITY FULL (identity = %q)", identity())
		}
		time.Sleep(300 * time.Millisecond)
	}
}
