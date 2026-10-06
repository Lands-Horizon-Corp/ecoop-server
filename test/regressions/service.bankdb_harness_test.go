package regressions

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/cqrs"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/pagination"
	"github.com/uptrace/bun"
	"go.uber.org/goleak"
)

// Banking test architecture for the database service (files service.bankdb_*_test.go).
//
// Every test runs against real Postgres: two throwaway databases per test (writer and reader) on the
// docker-compose instance (`make test-up`, or SQL_TEST_DSN), migrated by the service itself on Start.
// The repo's convention is a shared compose stack rather than testcontainers-go, so a test never pays
// container start-up and CI runs the same stack as developers.
//
// The "application" under test is bdLedger: a transfer service written only against the three
// database packages (sql transactions, cqrs writes, pagination locking reads). The CDC stream that
// feeds the read model is simulated by replicate, which publishes writer rows to the outbox runners.
//
// Each category lives in its own file:
//   01 smoke · 02 happy · 03 sad · 04 poison · 05 zero/nil · 06 concurrency
//   07 chaos · 08 cross-package · 09 latency/leaks · 10 idempotency/audit

type bdAccount struct {
	bun.BaseModel `bun:"table:bank_accounts"`
	ID            string         `bun:"id,pk" json:"id"`
	Owner         string         `bun:"owner,notnull" json:"owner"`
	Currency      string         `bun:"currency,nullzero,notnull,default:'PHP'" json:"currency"`
	Balance       int64          `bun:"balance,notnull" json:"balance"`
	Version       int64          `bun:"version,nullzero,notnull,default:1" json:"version"`
	Profile       map[string]any `bun:"profile,type:jsonb,nullzero" json:"profile"`
	Signature     []byte         `bun:"signature,type:bytea" json:"signature"`
	ClosedAt      *time.Time     `bun:"closed_at" json:"closed_at"`
	UpdatedAt     time.Time      `bun:"updated_at,nullzero,notnull,default:current_timestamp" json:"updated_at"`
}

type bdTransfer struct {
	bun.BaseModel  `bun:"table:bank_transfers"`
	ID             string    `bun:"id,pk" json:"id"`
	IdempotencyKey string    `bun:"idempotency_key,notnull" json:"idempotency_key"`
	FromAccount    string    `bun:"from_account,notnull" json:"from_account"`
	ToAccount      string    `bun:"to_account,notnull" json:"to_account"`
	Amount         int64     `bun:"amount,notnull" json:"amount"`
	Kind           string    `bun:"kind,type:bank_txn_kind,nullzero,notnull,default:'transfer'" json:"kind"`
	RequestHash    string    `bun:"request_hash,notnull" json:"request_hash"`
	UpdatedAt      time.Time `bun:"updated_at,nullzero,notnull,default:current_timestamp" json:"updated_at"`
}

type bdAudit struct {
	bun.BaseModel `bun:"table:bank_audit"`
	ID            string    `bun:"id,pk" json:"id"`
	TransferID    string    `bun:"transfer_id,notnull" json:"transfer_id"`
	AccountID     string    `bun:"account_id,notnull" json:"account_id"`
	Delta         int64     `bun:"delta,notnull" json:"delta"`
	BalanceAfter  int64     `bun:"balance_after,notnull" json:"balance_after"`
	UpdatedAt     time.Time `bun:"updated_at,nullzero,notnull,default:current_timestamp" json:"updated_at"`
}

type (
	bdAccountRes struct {
		ID, Owner, Currency string
		Balance             int64
		Closed              bool
	}
	bdNoRequest   struct{}
	bdAccountSvc  = cqrs.CQRSServices[bdAccount, bdAccountRes, bdNoRequest, string]
	bdTransferSvc = cqrs.CQRSServices[bdTransfer, bdTransfer, bdNoRequest, string]
	bdAuditSvc    = cqrs.CQRSServices[bdAudit, bdAudit, bdNoRequest, string]
)

// The audit table is append-only: a trigger rejects UPDATE and DELETE with SQLSTATE P0001.
const bdMigrationUp = `CREATE TYPE bank_txn_kind AS ENUM ('transfer', 'deposit', 'withdrawal');
CREATE TABLE bank_accounts (
	id         text PRIMARY KEY,
	owner      text NOT NULL,
	currency   char(3) NOT NULL DEFAULT 'PHP',
	balance    bigint NOT NULL CHECK (balance >= 0),
	version    bigint NOT NULL DEFAULT 1,
	profile    jsonb,
	signature  bytea,
	closed_at  timestamptz,
	updated_at timestamptz NOT NULL DEFAULT now(),
	UNIQUE (owner, currency)
);
CREATE TABLE bank_transfers (
	id              text PRIMARY KEY,
	idempotency_key text NOT NULL UNIQUE,
	from_account    text NOT NULL REFERENCES bank_accounts (id),
	to_account      text NOT NULL REFERENCES bank_accounts (id),
	amount          bigint NOT NULL CHECK (amount > 0),
	kind            bank_txn_kind NOT NULL DEFAULT 'transfer',
	request_hash    text NOT NULL,
	updated_at      timestamptz NOT NULL DEFAULT now(),
	CHECK (from_account <> to_account)
);
CREATE TABLE bank_audit (
	id            text PRIMARY KEY,
	transfer_id   text NOT NULL REFERENCES bank_transfers (id),
	account_id    text NOT NULL REFERENCES bank_accounts (id),
	delta         bigint NOT NULL CHECK (delta <> 0),
	balance_after bigint NOT NULL,
	updated_at    timestamptz NOT NULL DEFAULT now()
);
-- +goose StatementBegin
CREATE FUNCTION bank_audit_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	RAISE EXCEPTION 'bank_audit is append-only';
END $$;
-- +goose StatementEnd
CREATE TRIGGER bank_audit_immutable BEFORE UPDATE OR DELETE ON bank_audit
	FOR EACH ROW EXECUTE FUNCTION bank_audit_append_only();`

const bdMigrationDown = `DROP TABLE bank_audit;
DROP FUNCTION bank_audit_append_only;
DROP TABLE bank_transfers;
DROP TABLE bank_accounts;
DROP TYPE bank_txn_kind;`

var (
	errBankNoFunds   = errors.New("bank: insufficient funds")
	errBankKeyReused = errors.New("bank: idempotency key reused with a different request")
)

type bdOpts struct {
	maxOpen int                     // pool size per database (default 8)
	app     string                  // application_name prefix (default "bank")
	route   func(dsn string) string // rewrites both DSNs, e.g. through a fault proxy
	noRun   bool                    // do not start the outbox runners
}

// bdLedger is the service under test plus the handles a test needs to inspect it.
type bdLedger struct {
	t         testing.TB
	h         *dbHarness
	svc       *database.DatabaseService
	accounts  bdAccountSvc
	transfers bdTransferSvc
	audit     bdAuditSvc
	app       string
	events    map[string]func(key, value []byte) error
}

// newBDBank creates fresh writer/reader databases, registers the banking models, starts the service
// (migrating both databases) and its runners. At cleanup it fails the test if either pool still has a
// connection checked out, then stops the service.
func newBDBank(t testing.TB, o bdOpts) *bdLedger {
	t.Helper()
	if o.maxOpen == 0 {
		o.maxOpen = 8
	}
	if o.app == "" {
		o.app = "bank"
	}
	if o.route == nil {
		o.route = func(dsn string) string { return dsn }
	}
	h := newDBHarness(t)
	body := "-- +goose Up\n" + bdMigrationUp + "\n\n-- +goose Down\n" + bdMigrationDown + "\n"
	if err := os.WriteFile(filepath.Join("src", "database", "migrations", "00002_bank.sql"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	svc := database.NewDatabaseService(
		o.route(h.writerDSN)+"&application_name="+o.app+"-w", o.route(h.readerDSN)+"&application_name="+o.app+"-r",
		o.maxOpen, o.maxOpen,
		nil, nil, nil,
		h.migrations, true, io.Discard, nil,
		nil, h.broker, nil,
		100, 10*time.Millisecond,
	)
	must(t, database.Register(svc, database.Registration[bdAccount, bdAccountRes, bdNoRequest, string]{
		Channel:       "bank_accounts",
		ColumnVersion: "version",
		ToResource: func(a *bdAccount) *bdAccountRes {
			return &bdAccountRes{ID: a.ID, Owner: a.Owner, Currency: a.Currency, Balance: a.Balance, Closed: a.ClosedAt != nil}
		},
	}))
	must(t, database.Register(svc, database.Registration[bdTransfer, bdTransfer, bdNoRequest, string]{
		Channel: "bank_transfers", ToResource: func(x *bdTransfer) *bdTransfer { return x },
	}))
	must(t, database.Register(svc, database.Registration[bdAudit, bdAudit, bdNoRequest, string]{
		Channel: "bank_audit", ToResource: func(x *bdAudit) *bdAudit { return x },
	}))
	if err := svc.Start(bg); err != nil {
		t.Fatalf("Start: %v", err)
	}
	b := &bdLedger{t: t, h: h, svc: svc, app: o.app, events: map[string]func(key, value []byte) error{}}
	t.Cleanup(func() { _ = svc.Stop(bg) })
	t.Cleanup(b.assertPoolsDrained) // runs before Stop
	b.accounts = mustGet[bdAccount, bdAccountRes](t, svc)
	b.transfers = mustGet[bdTransfer, bdTransfer](t, svc)
	b.audit = mustGet[bdAudit, bdAudit](t, svc)
	if !o.noRun {
		svc.Run(bg)
		for _, ch := range []string{"bank_accounts", "bank_transfers", "bank_audit"} {
			b.events[ch] = h.broker.await(t, ch)
		}
	}
	return b
}

func must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func mustGet[TData, TRes any](t testing.TB, svc *database.DatabaseService) cqrs.CQRSServices[TData, TRes, bdNoRequest, string] {
	t.Helper()
	s, err := database.Get[TData, TRes, bdNoRequest, string](svc)
	if err != nil {
		t.Fatalf("Get %T: %v", *new(TData), err)
	}
	return s
}

// open creates an account with an opening balance on the writer.
func (b *bdLedger) open(id string, balance int64) *bdAccount {
	b.t.Helper()
	a, err := b.accounts.Create(bg, bdAccount{ID: id, Owner: "owner-" + id, Balance: balance})
	if err != nil {
		b.t.Fatalf("open %s: %v", id, err)
	}
	return a
}

// writerBalance reads a balance straight from the writer.
func (b *bdLedger) writerBalance(id string) int64 {
	b.t.Helper()
	return count(b.t, b.h.writer, `SELECT balance FROM bank_accounts WHERE id = $1`, id)
}

// writerTotal is the sum of all balances on the writer; transfers must never change it.
func (b *bdLedger) writerTotal() int64 {
	b.t.Helper()
	return count(b.t, b.h.writer, `SELECT COALESCE(SUM(balance), 0) FROM bank_accounts`)
}

// assertPoolsDrained fails the test when a pool still has a connection checked out: a leaked rows,
// transaction or conn.
func (b *bdLedger) assertPoolsDrained() {
	b.t.Helper()
	for name, s := range map[string]interface{ Client() *bun.DB }{"writer": b.svc.Writer(), "reader": b.svc.Reader()} {
		if s == nil || s.Client() == nil {
			continue
		}
		deadline := time.Now().Add(3 * time.Second)
		for s.Client().DB.Stats().InUse > 0 && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if n := s.Client().DB.Stats().InUse; n > 0 {
			b.t.Errorf("%s pool still has %d connection(s) checked out at the end of the test", name, n)
		}
	}
}

// --- the transfer service under test ---------------------------------------------------------------

type transferReq struct {
	Key      string
	From, To string
	Amount   int64
}

func (r transferReq) hash() string {
	sum := sha256.Sum256(fmt.Appendf(nil, "%s|%s|%d", r.From, r.To, r.Amount))
	return hex.EncodeToString(sum[:])
}

func eqFilter(field string, value any) pagination.StructuredFilter {
	return pagination.StructuredFilter{Filters: []pagination.Filter{{Field: field, Mode: pagination.ModeEqual, Value: value}}}
}

// Transfer moves money between two accounts exactly once per idempotency key, in one ACID
// transaction: lock both accounts in id order (pagination FOR UPDATE), check funds, update both
// balances and versions, insert the transfer and two audit rows (cqrs). It returns the stored
// transfer and whether this call was a replay of an earlier one with the same key. Errors are
// mapped to database.Err* kinds, or errBankNoFunds / errBankKeyReused.
func (b *bdLedger) Transfer(ctx context.Context, r transferReq) (*bdTransfer, bool, error) {
	var (
		out      *bdTransfer
		replayed bool
	)
	err := b.svc.Writer().Client().RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		existing, err := b.transfers.FindOneWithTx(ctx, &tx, eqFilter("idempotency_key", r.Key))
		switch {
		case err == nil:
			out, replayed = existing, true
			return nil
		case !errors.Is(err, sql.ErrNoRows):
			return err
		}
		return b.applyTransfer(ctx, tx, r, &out)
	})
	if err != nil {
		if errors.Is(err, errBankNoFunds) {
			return nil, false, err
		}
		mapped := database.MapError(err)
		if !errors.Is(mapped, database.ErrDuplicate) {
			return nil, false, mapped
		}
		// Lost the race with a concurrent request carrying the same key: its transfer is the answer.
		if out, err = b.transferByKey(ctx, r.Key); err != nil {
			return nil, false, database.MapError(err)
		}
		replayed = true
	}
	if replayed && out.RequestHash != r.hash() {
		return nil, true, errBankKeyReused
	}
	return out, replayed, nil
}

func (b *bdLedger) applyTransfer(ctx context.Context, tx bun.Tx, r transferReq, out **bdTransfer) error {
	locked, err := b.accounts.FindWithTx(ctx, &tx, pagination.StructuredFilter{
		Filters:    []pagination.Filter{{Field: "id", Mode: pagination.ModeInside, Value: []string{r.From, r.To}}},
		SortFields: []pagination.SortField{{Field: "id", Order: pagination.SortOrderAsc}},
	})
	if err != nil {
		return err
	}
	byID := map[string]*bdAccount{}
	for _, a := range locked {
		byID[a.ID] = a
	}
	from, to := byID[r.From], byID[r.To]
	if from == nil || to == nil {
		return sql.ErrNoRows
	}
	if from.Balance < r.Amount {
		return errBankNoFunds
	}
	now := time.Now()
	from.Balance, from.Version, from.UpdatedAt = from.Balance-r.Amount, from.Version+1, now
	to.Balance, to.Version, to.UpdatedAt = to.Balance+r.Amount, to.Version+1, now
	for _, a := range []*bdAccount{from, to} {
		if _, err := b.accounts.UpdateByIDWithTx(ctx, tx, a.ID, *a); err != nil {
			return err
		}
	}
	id := "t-" + r.Key
	if *out, err = b.transfers.CreateWithTx(ctx, tx, bdTransfer{
		ID: id, IdempotencyKey: r.Key, FromAccount: r.From, ToAccount: r.To, Amount: r.Amount, RequestHash: r.hash(),
	}); err != nil {
		return err
	}
	_, err = b.audit.CreateManyWithTx(ctx, tx, []bdAudit{
		{ID: id + "-dr", TransferID: id, AccountID: r.From, Delta: -r.Amount, BalanceAfter: from.Balance},
		{ID: id + "-cr", TransferID: id, AccountID: r.To, Delta: r.Amount, BalanceAfter: to.Balance},
	})
	return err
}

// transferByKey reads a transfer from the writer (the reader may not have it yet). The transaction is
// read-write because pagination's in-transaction reads take row locks.
func (b *bdLedger) transferByKey(ctx context.Context, key string) (*bdTransfer, error) {
	var out *bdTransfer
	err := b.svc.Writer().Client().RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		var err error
		out, err = b.transfers.FindOneWithTx(ctx, &tx, eqFilter("idempotency_key", key))
		return err
	})
	return out, err
}

// --- CDC simulation --------------------------------------------------------------------------------

// replicate publishes every writer row to the outbox runners (parents before children, as a CDC
// stream ordered by commit would) and waits until the reader holds the same rows.
func (b *bdLedger) replicate() {
	b.t.Helper()
	var accounts []bdAccount
	var transfers []bdTransfer
	var audit []bdAudit
	w := b.svc.Writer().Client()
	must(b.t, w.NewSelect().Model(&accounts).Scan(bg))
	must(b.t, w.NewSelect().Model(&transfers).Scan(bg))
	must(b.t, w.NewSelect().Model(&audit).Scan(bg))
	for _, a := range accounts {
		dbPublish(b.t, b.events["bank_accounts"], fmt.Sprintf("acc:%s:v%d", a.ID, a.Version), cqrs.ChangeTypeUpdated, a)
	}
	b.awaitReader(`SELECT count(*) FROM bank_accounts`, int64(len(accounts)))
	for _, x := range transfers {
		dbPublish(b.t, b.events["bank_transfers"], "trf:"+x.ID, cqrs.ChangeTypeCreated, x)
	}
	b.awaitReader(`SELECT count(*) FROM bank_transfers`, int64(len(transfers)))
	for _, x := range audit {
		dbPublish(b.t, b.events["bank_audit"], "aud:"+x.ID, cqrs.ChangeTypeCreated, x)
	}
	b.awaitReader(`SELECT count(*) FROM bank_audit`, int64(len(audit)))
	b.awaitReader(`SELECT COALESCE(SUM(balance), 0) FROM bank_accounts`, b.writerTotal())
}

func (b *bdLedger) awaitReader(query string, want int64) {
	b.t.Helper()
	awaitCount(b.t, b.h.reader, want, query)
}

// --- assertions and context helpers ----------------------------------------------------------------

// requireKind fails unless err maps to the application error kind want, and checks the mapped
// message does not leak schema details.
func requireKind(t testing.TB, err, want error) {
	t.Helper()
	mapped := database.MapError(err)
	if !errors.Is(mapped, want) {
		t.Fatalf("error = %v (mapped %v); want %v", err, mapped, want)
	}
	if msg := mapped.Error(); msg != want.Error() {
		t.Fatalf("mapped message %q leaks details; want exactly %q", msg, want.Error())
	}
}

// withDeadline returns a context that expires after d and is cancelled at cleanup at the latest.
// Every network call in this suite runs under some deadline so a hang fails instead of stalling CI.
func withDeadline(t testing.TB, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(bg, d)
	t.Cleanup(cancel)
	return ctx
}

// retrySerializable retries fn while it fails with a serialization or deadlock error, the standard
// client-side contract for SERIALIZABLE transactions.
func retrySerializable(ctx context.Context, attempts int, fn func() error) error {
	var err error
	for range attempts {
		if err = fn(); !errors.Is(database.MapError(err), database.ErrSerialization) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
	return err
}

// verifyNoDBLeaks fails if goroutines started after baseline are still running. Background
// goroutines of other services exercised elsewhere in this package (the Redis client keeps redialing
// after its own tests end) are ignored, so the check stays strict for database code without
// flaking on unrelated suites.
func verifyNoDBLeaks(t testing.TB, baseline goleak.Option) {
	t.Helper()
	goleak.VerifyNone(t, baseline,
		goleak.IgnoreAnyFunction("github.com/redis/go-redis/v9/internal/pool.(*ConnPool).dialConn"),
		goleak.IgnoreAnyFunction("github.com/redis/go-redis/v9/internal/pool.(*ConnPool).tryDial"),
		goleak.IgnoreAnyFunction("github.com/redis/go-redis/v9.(*sentinelFailover).listen"),
	)
}

// --- fault injection -------------------------------------------------------------------------------

// faultProxy is a TCP proxy in front of Postgres that can reset every live connection (RST), refuse
// new ones, or add latency to each chunk of traffic.
type faultProxy struct {
	ln      net.Listener
	target  string
	refuse  atomic.Bool
	latency atomic.Int64 // nanoseconds per chunk
	mu      sync.Mutex
	conns   map[net.Conn]struct{}
	wg      sync.WaitGroup
}

func newFaultProxy(t testing.TB) *faultProxy {
	t.Helper()
	u, err := url.Parse(envOr("SQL_TEST_DSN", defaultPostgresDSN))
	must(t, err)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	must(t, err)
	p := &faultProxy{ln: ln, target: u.Host, conns: map[net.Conn]struct{}{}}
	p.wg.Go(p.accept)
	t.Cleanup(p.close)
	return p
}

// route rewrites a DSN to go through the proxy.
func (p *faultProxy) route(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		panic(err)
	}
	u.Host = p.ln.Addr().String()
	return u.String()
}

func (p *faultProxy) accept() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		if p.refuse.Load() {
			resetConn(c)
			continue
		}
		up, err := net.Dial("tcp", p.target)
		if err != nil {
			resetConn(c)
			continue
		}
		p.track(c, up)
		p.wg.Go(func() { p.pipe(up, c) })
		p.wg.Go(func() { p.pipe(c, up) })
	}
}

func (p *faultProxy) pipe(dst, src net.Conn) {
	defer func() { _ = dst.Close(); _ = src.Close() }()
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if d := p.latency.Load(); d > 0 {
				time.Sleep(time.Duration(d))
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (p *faultProxy) track(cs ...net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range cs {
		p.conns[c] = struct{}{}
	}
}

// resetAll drops every live connection with a TCP RST, like a crashed load balancer.
func (p *faultProxy) resetAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for c := range p.conns {
		resetConn(c)
		delete(p.conns, c)
	}
}

func (p *faultProxy) close() {
	_ = p.ln.Close()
	p.resetAll()
	p.wg.Wait()
}

func resetConn(c net.Conn) {
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetLinger(0) // close with RST instead of FIN
	}
	_ = c.Close()
}
