package fromenv_test

import (
	"os"
	"testing"

	"go.uber.org/fx"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sql"
)

// These are the two ways docs/modules.md says the database is wired. They build
// the services without starting them (fx.New runs constructors, not hooks), so
// no Postgres is needed; the hooks are only registered.

type user struct{ ID int64 }

func pgEnv(t *testing.T) {
	t.Helper()
	for k, v := range map[string]string{
		"POSTGRES_USER": "u", "POSTGRES_PASSWORD": "p", "POSTGRES_DB": "main",
		"POSTGRES_READ_USER": "r", "POSTGRES_READ_PASSWORD": "rp", "POSTGRES_READ_DB": "read",
	} {
		t.Setenv(k, v)
	}
}

// Models supplied with fx.Supply reach the main database service.
func TestModelsAreSuppliedWithFxSupply(t *testing.T) {
	pgEnv(t)
	var got sql.ISQLService
	app := fx.New(fx.NopLogger,
		sql.Module,
		fx.Supply(sql.Models{&user{}}),
		fx.Populate(&got),
	)
	if err := app.Err(); err != nil {
		t.Fatalf("the module with supplied models did not build: %v", err)
	}
	if got == nil {
		t.Fatal("no service was provided")
	}
}

// The module works with no models: the service just cannot generate migrations.
func TestTheModuleBuildsWithoutModels(t *testing.T) {
	pgEnv(t)
	var got sql.ISQLService
	if err := fx.New(fx.NopLogger, sql.Module, fx.Populate(&got)).Err(); err != nil {
		t.Fatalf("the module without models did not build: %v", err)
	}
}

// writer and reader are distinct types, so fx can hold both next to each other.
type writer struct{ sql.ISQLService }
type reader struct{ sql.ISQLService }

// The read model is built beside the main database from ReadFromEnv, exactly as
// the documentation shows, and the two are different services.
func TestAReadDatabaseCanBeBuiltBesideTheMainOne(t *testing.T) {
	pgEnv(t)
	var w writer
	var r reader
	app := fx.New(fx.NopLogger,
		sql.Module,
		fx.Provide(
			func(main sql.ISQLService) writer { return writer{main} },
			func(lc fx.Lifecycle) (reader, error) {
				cfg, err := sql.ReadFromEnv(os.LookupEnv)
				if err != nil {
					return reader{}, err
				}
				svc, err := sql.NewFromConfig(lc, cfg, nil, nil, nil, nil)
				return reader{svc}, err
			},
		),
		fx.Populate(&w, &r),
	)
	if err := app.Err(); err != nil {
		t.Fatalf("the two databases did not build side by side: %v", err)
	}
	if w.ISQLService == nil || r.ISQLService == nil || w.ISQLService == r.ISQLService {
		t.Fatalf("writer %v and reader %v are not two services", w.ISQLService, r.ISQLService)
	}
}

// A bad setting stops the application at construction, with the variable named.
func TestABadDatabaseSettingStopsTheApplication(t *testing.T) {
	t.Setenv("POSTGRES_USER", "u")
	t.Setenv("POSTGRES_PASSWORD", "p")
	t.Setenv("POSTGRES_DB", "d")
	t.Setenv("POSTGRES_MAX_CONNECTIONS", "lots")
	var got sql.ISQLService
	err := fx.New(fx.NopLogger, sql.Module, fx.Populate(&got)).Err()
	if err == nil {
		t.Fatal("a malformed POSTGRES_MAX_CONNECTIONS was accepted")
	}
	if msg := err.Error(); !contains(msg, "POSTGRES_MAX_CONNECTIONS") || contains(msg, "lots") {
		t.Errorf("the error should name the variable and never the value: %v", err)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
