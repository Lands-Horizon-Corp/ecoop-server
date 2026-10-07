package regressions

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/cqrs"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/pagination"
	"github.com/go-playground/validator/v10"
	"github.com/uptrace/bun"
)

// 28 CRUD cookbook: the end-to-end model and every CRUD pattern documented in
// .agents/skills/ecoop-database/references/crud.md, executed against real Postgres.

// --- the model, exactly as the skill shows it ---

type crudMember struct {
	bun.BaseModel `bun:"table:crud_members"`
	ID            string    `bun:"id,pk" json:"id"`
	Name          string    `bun:"name,notnull" json:"name" validate:"required,max=120"`
	Email         string    `bun:"email,notnull" json:"email" validate:"required,email"`
	Tier          string    `bun:"tier,nullzero,notnull,default:'basic'" json:"tier" validate:"omitempty,oneof=basic silver gold"`
	Version       int64     `bun:"version,nullzero,notnull,default:1" json:"version"`
	UpdatedAt     time.Time `bun:"updated_at,nullzero,notnull,default:current_timestamp" json:"updated_at"`
	DeletedAt     time.Time `bun:"deleted_at,soft_delete,nullzero" json:"deleted_at"`
}

type crudCreateRequest struct {
	ID    string `json:"id" validate:"required"`
	Name  string `json:"name" validate:"required,max=120"`
	Email string `json:"email" validate:"required,email"`
}

// crudPatchRequest: nil means "leave unchanged".
type crudPatchRequest struct {
	Name  *string `json:"name" validate:"omitempty,max=120"`
	Email *string `json:"email" validate:"omitempty,email"`
	Tier  *string `json:"tier" validate:"omitempty,oneof=basic silver gold"`
}

type crudMemberResponse struct {
	ID, Name, Email, Tier string
	Version               int64
}

type crudMembers = cqrs.CQRSServices[crudMember, crudMemberResponse, crudCreateRequest, string]

const crudMigration = `CREATE TABLE crud_members (
	id         text PRIMARY KEY,
	name       text NOT NULL,
	email      text NOT NULL UNIQUE,
	tier       text NOT NULL DEFAULT 'basic',
	version    bigint NOT NULL DEFAULT 1,
	updated_at timestamptz NOT NULL DEFAULT now(),
	deleted_at timestamptz
);`

var errStaleVersion = errors.New("member was changed by someone else; reload and retry")

func crudService(t *testing.T) (*dbHarness, *database.DatabaseService, crudMembers) {
	t.Helper()
	h := newDBHarness(t)
	must(t, os.WriteFile(filepath.Join("src", "database", "migrations", "00002_crud.sql"),
		[]byte(gooseBody(crudMigration, "DROP TABLE crud_members;")), 0o644))
	svc := h.newService(h.writerDSN, h.readerDSN)
	must(t, database.Register(svc, database.Registration[crudMember, crudMemberResponse, crudCreateRequest, string]{
		Channel:       "crud_members",
		ColumnVersion: "version",
		ToResource: func(m *crudMember) *crudMemberResponse {
			return &crudMemberResponse{ID: m.ID, Name: m.Name, Email: m.Email, Tier: m.Tier, Version: m.Version}
		},
		FromRequest: func(r *crudCreateRequest) *crudMember {
			return &crudMember{ID: r.ID, Name: r.Name, Email: r.Email}
		},
	}))
	must(t, svc.Start(bg))
	t.Cleanup(func() { _ = svc.Stop(bg) })
	members, err := database.Get[crudMember, crudMemberResponse, crudCreateRequest, string](svc)
	must(t, err)
	return h, svc, members
}

// patchMember is the PATCH pattern: lock, apply only the fields sent, bump the version, write back.
// expectedVersion > 0 adds optimistic concurrency (the client sends the version it saw).
func patchMember(ctx context.Context, db *database.DatabaseService, members crudMembers,
	id string, expectedVersion int64, p crudPatchRequest) (*crudMemberResponse, error) {
	if err := validator.New().StructCtx(ctx, p); err != nil {
		return nil, database.MapError(err)
	}
	var out *crudMemberResponse
	err := database.RunInTx(ctx, db, nil, func(ctx context.Context, tx bun.Tx) error {
		m, err := members.GetByIDWithTx(ctx, &tx, id) // FOR UPDATE: concurrent PATCHes queue up
		if err != nil {
			return err
		}
		if expectedVersion > 0 && m.Version != expectedVersion {
			return errStaleVersion
		}
		if p.Name != nil {
			m.Name = *p.Name
		}
		if p.Email != nil {
			m.Email = *p.Email
		}
		if p.Tier != nil {
			m.Tier = *p.Tier
		}
		m.Version++
		m.UpdatedAt = time.Now()
		out, err = members.UpdateByIDWithTxFormat(ctx, tx, id, *m)
		return err
	})
	if errors.Is(err, errStaleVersion) {
		return nil, err
	}
	return out, database.MapError(err)
}

func TestBankDBCrud_FullLifecycle(t *testing.T) {
	h, db, members := crudService(t)
	ctx := withDeadline(t, 10*time.Second)

	// CREATE from a request DTO: validated, mapped, defaults returned (RETURNING *).
	created, err := members.CreateWithValidationFormat(ctx, crudCreateRequest{ID: "m1", Name: "Ann Cruz", Email: "ann@coop.ph"})
	if err != nil || created.Tier != "basic" || created.Version != 1 {
		t.Fatalf("create = %+v, %v; want tier basic, version 1 from the database defaults", created, err)
	}

	// READ your own write on the writer (the reader catches up through CDC).
	err = database.RunInTx(ctx, db, nil, func(ctx context.Context, tx bun.Tx) error {
		got, err := members.GetByIDWithTxFormat(database.WithoutRowLocks(ctx), &tx, "m1")
		if err == nil && got.Email != "ann@coop.ph" {
			t.Errorf("read back %+v", got)
		}
		return err
	})
	must(t, err)

	// UPDATE (PATCH): only the sent fields change.
	tier := "gold"
	patched, err := patchMember(ctx, db, members, "m1", 1, crudPatchRequest{Tier: &tier})
	if err != nil || patched.Tier != "gold" || patched.Name != "Ann Cruz" || patched.Version != 2 {
		t.Fatalf("patch = %+v, %v; want tier gold, name kept, version 2", patched, err)
	}

	// DELETE (soft): the row stays for history, every service read stops seeing it.
	must(t, members.DeleteByID(ctx, "m1"))
	err = database.RunInTx(ctx, db, nil, func(ctx context.Context, tx bun.Tx) error {
		_, err := members.GetByIDWithTx(ctx, &tx, "m1")
		return err
	})
	requireKind(t, err, database.ErrNotFound)
	if n := count(t, h.writer, `SELECT count(*) FROM crud_members WHERE id = 'm1' AND deleted_at IS NOT NULL`); n != 1 {
		t.Fatal("soft delete removed the row instead of stamping deleted_at")
	}
	requireKind(t, members.DeleteByID(ctx, "m1"), database.ErrNotFound) // deleting twice
}

func TestBankDBCrud_ValidationAndConflictsMapToClientErrors(t *testing.T) {
	_, db, members := crudService(t)
	ctx := withDeadline(t, 10*time.Second)

	_, err := members.CreateWithValidation(ctx, crudCreateRequest{ID: "m1", Name: "Ann", Email: "not-an-email"})
	requireKind(t, err, database.ErrInvalidInput) // 400, not 500
	var fields validator.ValidationErrors
	if !errors.As(err, &fields) || fields[0].Field() != "Email" {
		t.Fatalf("validation details not reachable: %v", err)
	}

	must(t, second(members.CreateWithValidation(ctx, crudCreateRequest{ID: "m1", Name: "Ann", Email: "ann@coop.ph"})))
	_, err = members.CreateWithValidation(ctx, crudCreateRequest{ID: "m2", Name: "Ann 2", Email: "ann@coop.ph"})
	requireKind(t, err, database.ErrDuplicate) // 409: email is unique

	_, err = patchMember(ctx, db, members, "ghost", 0, crudPatchRequest{})
	requireKind(t, err, database.ErrNotFound) // 404

	bad := "x@"
	_, err = patchMember(ctx, db, members, "m1", 0, crudPatchRequest{Email: &bad})
	requireKind(t, err, database.ErrInvalidInput)
}

func TestBankDBCrud_OptimisticConcurrencyRejectsAStaleEdit(t *testing.T) {
	h, db, members := crudService(t)
	ctx := withDeadline(t, 10*time.Second)
	must(t, second(members.CreateWithValidation(ctx, crudCreateRequest{ID: "m1", Name: "Ann", Email: "ann@coop.ph"})))

	// Two clients loaded version 1. The first save wins; the second must not overwrite it blindly.
	a, b := "Ann (by teller A)", "Ann (by teller B)"
	if _, err := patchMember(ctx, db, members, "m1", 1, crudPatchRequest{Name: &a}); err != nil {
		t.Fatal(err)
	}
	if _, err := patchMember(ctx, db, members, "m1", 1, crudPatchRequest{Name: &b}); !errors.Is(err, errStaleVersion) {
		t.Fatalf("stale edit = %v; want errStaleVersion (409)", err)
	}
	var name string
	must(t, h.writer.QueryRow(`SELECT name FROM crud_members WHERE id = 'm1'`).Scan(&name))
	if name != a {
		t.Fatalf("name = %q; the stale edit overwrote the first one", name)
	}
}

func TestBankDBCrud_BulkOperations(t *testing.T) {
	h, _, members := crudService(t)
	ctx := withDeadline(t, 10*time.Second)
	batch := make([]crudMember, 5)
	for i := range batch {
		batch[i] = crudMember{ID: fmt.Sprintf("b%d", i), Name: fmt.Sprintf("Member %d", i), Email: fmt.Sprintf("m%d@coop.ph", i)}
	}
	created, err := members.CreateMany(ctx, batch) // one INSERT, validated per row
	if err != nil || len(created) != 5 {
		t.Fatalf("CreateMany = %d, %v", len(created), err)
	}

	for _, m := range created {
		m.Tier, m.Version = "silver", m.Version+1
	}
	updated := make([]crudMember, len(created))
	for i, m := range created {
		updated[i] = *m
	}
	if _, err := members.UpdateMany(ctx, updated); err != nil { // one UPDATE … FROM (VALUES …) by primary key
		t.Fatal(err)
	}
	must(t, members.DeleteMany(ctx, []string{"b0", "b1"}))

	inspect := h.writer
	for q, want := range map[string]int64{
		`SELECT count(*) FROM crud_members WHERE tier = 'silver'`:        5,
		`SELECT count(*) FROM crud_members WHERE deleted_at IS NULL`:     3,
		`SELECT count(*) FROM crud_members WHERE deleted_at IS NOT NULL`: 2,
	} {
		if got := count(t, inspect, q); got != want {
			t.Errorf("%s = %d; want %d", q, got, want)
		}
	}
	// A whole batch is one statement: one invalid row rejects all of it.
	_, err = members.CreateMany(ctx, []crudMember{{ID: "ok", Name: "Ok", Email: "ok@coop.ph"}, {ID: "bad", Name: "", Email: "bad@coop.ph"}})
	requireKind(t, err, database.ErrInvalidInput)
	if n := count(t, inspect, `SELECT count(*) FROM crud_members WHERE id = 'ok'`); n != 0 {
		t.Fatal("a rejected batch was partly written")
	}
}

func TestBankDBCrud_ListEndpointReadsTheReadModel(t *testing.T) {
	h, db, members := crudService(t)
	ctx := withDeadline(t, 10*time.Second)
	for i := range 3 {
		must(t, second(members.CreateWithValidation(ctx, crudCreateRequest{ID: fmt.Sprintf("l%d", i), Name: fmt.Sprintf("L %d", i), Email: fmt.Sprintf("l%d@coop.ph", i)})))
	}
	// Feed the read model the way CDC does, then list from it.
	db.Run(bg)
	handler := h.broker.await(t, "crud_members")
	all := make([]crudMember, 0, 3)
	must(t, db.Writer().Client().NewSelect().Model(&all).Scan(ctx))
	for _, m := range all {
		dbPublish(t, handler, "crud:"+m.ID, cqrs.ChangeTypeCreated, m)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		page, err := members.PaginateFormat(ctx, pagination.Pagination{PageSize: 10, Filter: pagination.StructuredFilter{
			SortFields: []pagination.SortField{{Field: "id", Order: pagination.SortOrderAsc}}}})
		must(t, err)
		if len(page.Data) == 3 {
			if page.Data[0].ID != "l0" || page.Data[0].Tier != "basic" {
				t.Fatalf("first item = %+v", page.Data[0])
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("read model has %d of 3 members", len(page.Data))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Documented hazard: a PUT through FromRequest resets the columns the request does not carry,
// including server-owned ones such as version. Models with server-owned fields use the PATCH pattern.
func TestBankDBCrud_PutResetsFieldsTheRequestDoesNotCarry(t *testing.T) {
	h, _, members := crudService(t)
	ctx := withDeadline(t, 10*time.Second)
	must(t, second(members.CreateWithValidation(ctx, crudCreateRequest{ID: "m1", Name: "Ann", Email: "ann@coop.ph"})))
	_, err := h.writer.Exec(`UPDATE crud_members SET tier = 'gold', version = 5 WHERE id = 'm1'`)
	must(t, err)

	res, err := members.UpdateByIDWithValidationFormat(ctx, "m1", crudCreateRequest{ID: "m1", Name: "Ann B", Email: "annb@coop.ph"})
	must(t, err)
	if res.Name != "Ann B" || res.Tier != "basic" || res.Version != 1 {
		t.Fatalf("PUT = %+v; the documented behavior is name replaced, tier and version reset to defaults", res)
	}
}
