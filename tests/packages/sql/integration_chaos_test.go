package sql_test

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	ecsql "github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sql"
)

// Integration chaos: several instances of the app (one service each, as
// behind a load balancer) sharing one database and one migrations folder.

const replicas = 6

func fleet(t *testing.T, e *env, auto bool, models ...any) []ecsql.SQLServices {
	t.Helper()
	out := make([]ecsql.SQLServices, replicas)
	for i := range out {
		out[i] = ecsql.NewSQLService(e.dsn, 2, 4, e.dir, auto, e.out, models, nil)
		if err := out[i].Start(ctxT(t)); err != nil {
			t.Fatal(err)
		}
		s := out[i]
		t.Cleanup(func() { _ = s.Stop(context.Background()) })
	}
	return out
}

func applied(t *testing.T, s ecsql.SQLServices) int {
	t.Helper()
	n, err := s.Client().NewSelect().Table(ecsql.GooseTable).Where("version_id > 0").Count(ctxT(t))
	if err != nil {
		t.Fatal(err)
	}
	return int(n)
}

// Replicas booting at once each Run; every migration is applied once.
func TestIntegrationChaos_SimultaneousBootsMigrateOnce(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	for v := int64(1); v <= 8; v++ {
		e.write(t, v, "m", "CREATE TABLE IF NOT EXISTS log (v int);\nINSERT INTO log VALUES ("+string(rune('0'+v))+");", "DELETE FROM log;")
	}
	ss := fleet(t, e, true)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for _, s := range ss {
		wg.Go(func() {
			<-start
			if err := s.Run(ctxT(t)); err != nil {
				t.Error(err)
			}
		})
	}
	close(start)
	wg.Wait()
	if n := applied(t, ss[0]); n != 8 {
		t.Fatalf("applied rows = %d, want 8", n)
	}
	n, _ := ss[0].Client().NewSelect().Table("log").Count(ctxT(t))
	if n != 8 {
		t.Fatalf("log rows = %d, want 8 (a migration ran twice)", n)
	}
}

// Replicas running Migrate with the same models write one file between them.
func TestIntegrationChaos_ConcurrentMigrateWritesOneFile(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ss := fleet(t, e, false, (*MemberV1)(nil), (*Loan)(nil), (*Branch)(nil))
	var wg sync.WaitGroup
	for _, s := range ss {
		wg.Go(func() {
			if err := s.Migrate(ctxT(t)); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if got := e.files("*.sql*"); len(got) != 1 {
		t.Fatalf("files = %v, want 1", got)
	}
	if got := tables(t, ss[0]); len(got) != 3 {
		t.Fatalf("tables = %v", got)
	}
}

// A replica dying while it holds the migration lock does not block the rest.
func TestIntegrationChaos_ReplicaDiesHoldingTheLock(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.write(t, 1, "slow", "CREATE TABLE slow (id int);\nSELECT pg_sleep(12); -- longer than pgdriver's 10s read timeout", "DROP TABLE slow;")
	p := newProxy(t)
	victim := proxied(t, e, p)
	ctx := ctxT(t)
	errc := make(chan error, 1)
	go func() { errc <- victim.Migrate(ctx) }()
	time.Sleep(500 * time.Millisecond)
	p.DropAll() // the replica's host goes away

	if err := <-errc; err == nil {
		t.Fatal("victim's migration finished")
	}
	e.write(t, 1, "slow", "CREATE TABLE slow (id int);", "DROP TABLE slow;")
	other := e.started(t)
	limit, cancel := context.WithTimeout(ctx, 40*time.Second)
	defer cancel()
	if err := other.Migrate(limit); err != nil {
		t.Fatalf("other replica blocked by a dead one: %v", err)
	}
	if applied(t, other) != 1 {
		t.Fatal("migration not applied exactly once")
	}
}

// Random commands from every replica at once leave goose's bookkeeping
// consistent with the schema.
func TestIntegrationChaos_RandomCommandsStayConsistent(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	const n = 6
	for v := int64(1); v <= n; v++ {
		name := "t" + string(rune('0'+v))
		e.write(t, v, name, "CREATE TABLE "+name+" (id int);", "DROP TABLE "+name+";")
	}
	ss := fleet(t, e, true)
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	var wg sync.WaitGroup
	for i, s := range ss {
		wg.Go(func() {
			r := rand.New(rand.NewPCG(seed, uint64(i)))
			ctx := ctxT(t)
			for range 15 {
				var err error
				switch r.IntN(6) {
				case 0:
					err = s.Run(ctx)
				case 1:
					err = s.Rollback(ctx)
				case 2:
					err = s.UpSteps(ctx, 1+r.IntN(3))
				case 3:
					err = s.RollbackSteps(ctx, 1+r.IntN(3))
				case 4:
					err = s.Status(ctx)
				case 5:
					err = s.Redo(ctx)
				}
				if err != nil && !errors.Is(err, ecsql.ErrTooManySteps) {
					t.Errorf("replica %d: %v", i, err)
				}
			}
		})
	}
	wg.Wait()
	v := version(t, ss[0], e)
	want := 0
	for x := range n {
		if v != "0" && x < int(v[0]-'0') {
			want++
		}
	}
	if got := len(tables(t, ss[0])); got != want || applied(t, ss[0]) != want {
		t.Fatalf("version %s but %d tables and %d applied rows", v, got, applied(t, ss[0]))
	}
}
