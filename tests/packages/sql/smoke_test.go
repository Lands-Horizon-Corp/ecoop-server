package sql_test

import (
	"context"
	"testing"
)

// Smoke: start, migrate from models, write and read a row, stop.
func TestSmoke(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	s := e.service((*MemberV1)(nil))
	ctx := ctxT(t)
	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := s.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	m := &MemberV1{Name: "Ana"}
	if _, err := s.Client().NewInsert().Model(m).Exec(ctx); err != nil {
		t.Fatalf("insert: %v", err)
	}
	var got MemberV1
	if err := s.Client().NewSelect().Model(&got).Where("id = ?", m.ID).Scan(ctx); err != nil || got.Name != "Ana" {
		t.Fatalf("select = %+v, %v", got, err)
	}
	if err := s.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}
