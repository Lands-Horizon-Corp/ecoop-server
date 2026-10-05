// Command migrate manages database migrations from the command line.
//
//	go run ./cmd/migrate <command> [args]
//
// It reads DATABASE_URL (a .env file is loaded if present) and never applies migrations implicitly.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/sql"
	"github.com/Lands-Horizon-Corp/ecoop-server/src/models"
	"github.com/joho/godotenv"
)

const usage = `usage: migrate <command> [args]

  up                 apply all pending migrations
  up-by <n>          apply the next n migrations
  down               roll back the latest migration
  down-by <n>        roll back the latest n migrations
  down-to <version>  roll back to a version (0 = everything)
  redo               roll back and re-apply the latest migration
  fresh --yes        roll back everything and re-apply (destroys data)
  status             list migrations and whether they are applied
  version            print the current version
  create <name>      scaffold an empty goose migration
  diff <name>        apply pending migrations, then write a migration for model changes
                     (models are listed in src/models/models.go)`

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, usage)
		return 2
	}
	_ = godotenv.Load()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	cmd, rest := args[0], args[1:]
	if cmd == "create" {
		if len(rest) != 1 {
			return fail("create needs a name")
		}
		return report(sql.NewSQLService("", 1, 1, nil).Create(ctx, rest[0]))
	}

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return fail("DATABASE_URL is not set")
	}
	svc := sql.NewSQLService(dsn, 2, 5, os.Stdout, sql.WithAutoMigrate(false))
	if err := svc.Run(ctx); err != nil {
		return fail("connect: %v", err)
	}
	defer func() { _ = svc.Stop(ctx) }()

	switch cmd {
	case "up":
		return report(svc.Migrate(ctx))
	case "down":
		return report(svc.Rollback(ctx))
	case "redo":
		return report(svc.Redo(ctx))
	case "status":
		return report(svc.Status(ctx))
	case "version":
		return report(svc.Version(ctx))
	case "fresh":
		if len(rest) != 1 || rest[0] != "--yes" {
			return fail("fresh destroys all data; pass --yes to confirm")
		}
		return report(svc.Fresh(ctx))
	case "up-by", "down-by":
		n, ok := intArg(rest)
		if !ok {
			return fail("%s needs a number", cmd)
		}
		if cmd == "up-by" {
			return report(svc.UpSteps(ctx, n))
		}
		return report(svc.RollbackSteps(ctx, n))
	case "down-to":
		v, ok := intArg(rest)
		if !ok {
			return fail("down-to needs a version")
		}
		return report(svc.RollbackTo(ctx, int64(v)))
	case "diff":
		if len(rest) != 1 {
			return fail("diff needs a name")
		}
		return diff(ctx, svc, rest[0])
	default:
		fmt.Fprintln(os.Stderr, usage)
		return 2
	}
}

func diff(ctx context.Context, svc sql.SQLServices, name string) int {
	// The diff is computed against the live database, so it must be fully migrated first.
	if err := svc.Migrate(ctx); err != nil {
		return fail("applying pending migrations: %v", err)
	}
	path, err := svc.Diff(ctx, name, models.All()...)
	switch {
	case errors.Is(err, sql.ErrNoModels):
		return fail("no models registered: add them to src/models/models.go")
	case err != nil:
		return fail("%v", err)
	case path == "":
		fmt.Println("no changes: the database already matches the models")
	default:
		fmt.Println("created", path, "- review it, then run: migrate up")
	}
	return 0
}

func intArg(args []string) (int, bool) {
	if len(args) != 1 {
		return 0, false
	}
	n, err := strconv.Atoi(args[0])
	return n, err == nil
}

func report(err error) int {
	if err != nil {
		return fail("%v", err)
	}
	return 0
}

func fail(format string, a ...any) int {
	fmt.Fprintf(os.Stderr, "migrate: "+format+"\n", a...)
	return 1
}
