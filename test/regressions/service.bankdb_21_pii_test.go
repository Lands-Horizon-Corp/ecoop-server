package regressions

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/pagination"
	sqlsvc "github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/sql"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/logger"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/attribute"
)

// 21 PII in logs: database errors can quote customer data (a duplicate owner's name, a malformed
// account number). Mapped errors, their safe log fields and everything the database packages log
// must carry the SQLSTATE and schema names an operator needs, and never the values.

const pii = "Ann Cruz 123-45-6789"

func TestBankDBPII_SafeFieldsCarryNoValues(t *testing.T) {
	b := newBDBank(t, bdOpts{noRun: true})
	ctx := withDeadline(t, 5*time.Second)
	must(t, second(b.accounts.Create(ctx, bdAccount{ID: "a", Owner: pii, Balance: 1})))
	_, err := b.accounts.Create(ctx, bdAccount{ID: "b", Owner: pii, Balance: 1}) // duplicate owner: Detail quotes it

	m, ok := errors.AsType[*database.MappedError](database.MapError(err))
	if !ok {
		t.Fatalf("not mapped: %v", err)
	}
	fields := fmt.Sprint(m.SafeFields())
	for _, want := range []string{"23505", "bank_accounts", "bank_accounts_tenant_id_owner_currency_key"} {
		if !strings.Contains(fields, want) {
			t.Errorf("safe fields %s lack %q", fields, want)
		}
	}
	for _, leak := range []string{"Ann", "123-45-6789"} {
		if strings.Contains(fields, leak) || strings.Contains(m.Error(), leak) {
			t.Fatalf("customer data %q leaked into %s / %s", leak, fields, m.Error())
		}
	}
}

func TestBankDBPII_RejectedFilterValuesAreNotLogged(t *testing.T) {
	log := &recordingLog{Context: bg}
	b := newBDBank(t, bdOpts{noRun: true, log: log})
	// A malformed number filtered against a bigint column: Postgres quotes the input in its message.
	_, err := b.accounts.Count(withDeadline(t, 5*time.Second), pagination.StructuredFilter{Filters: []pagination.Filter{
		{Field: "balance", Mode: pagination.ModeEqual, Value: pii},
	}})
	requireKind(t, err, database.ErrInvalidInput)

	line := log.await(t, "rejected request", withMsg("pagination request rejected"))
	if line.fields["reason"] != "invalid_value" {
		t.Fatalf("rejection line = %+v; want reason invalid_value", line)
	}
	for _, e := range log.snapshot() {
		blob := fmt.Sprint(e.msg, e.fields)
		if e.err != nil {
			blob += e.err.Error()
		}
		if strings.Contains(blob, "123-45-6789") {
			t.Fatalf("customer data leaked into a log line: %+v", e)
		}
	}
}

func TestBankDBPII_RedactKeepsTheCodeAndDropsQuotedInput(t *testing.T) {
	dataErr := &pgconn.PgError{Severity: "ERROR", Code: "22P02", ColumnName: "balance",
		Message: `invalid input syntax for type bigint: "` + pii + `"`}
	red := sqlsvc.Redact(fmt.Errorf("applying change: %w", dataErr))
	if strings.Contains(red.Error(), "123-45-6789") || !strings.Contains(red.Error(), "22P02") || !strings.Contains(red.Error(), "balance") {
		t.Fatalf("redacted = %q; want the SQLSTATE and column, not the input", red.Error())
	}
	if _, ok := errors.AsType[*pgconn.PgError](red); !ok {
		t.Fatal("redaction hid the cause from errors.As")
	}
	integrity := &pgconn.PgError{Severity: "ERROR", Code: "23514", Message: `new row for relation "bank_accounts" violates check constraint "bank_accounts_balance_check"`}
	if got := sqlsvc.Redact(integrity); got.Error() != integrity.Error() {
		t.Fatalf("integrity messages name only schema objects and should be kept; got %q", got.Error())
	}
}

func TestBankDBPII_RealLoggerOutputContainsNoCustomerData(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1") // exporters connect lazily; nothing listens
	r, w, err := os.Pipe()
	must(t, err)
	stderr := os.Stderr
	os.Stderr = w // the logger writes to the stderr it finds at Start
	log := logger.NewLogContextService("ecoop", "json", "debug", attribute.String("service", "db"))
	startErr := log.Start(bg)
	os.Stderr = stderr
	must(t, startErr)

	b := newBDBank(t, bdOpts{noRun: true, logger: log})
	ctx := withDeadline(t, 5*time.Second)
	must(t, second(b.accounts.Create(ctx, bdAccount{ID: "a", Owner: pii, Balance: 1})))
	_, dupErr := b.accounts.Create(ctx, bdAccount{ID: "b", Owner: pii, Balance: 1})
	_, _ = b.accounts.Count(ctx, pagination.StructuredFilter{Filters: []pagination.Filter{{Field: "balance", Mode: pagination.ModeEqual, Value: pii}}})
	// What an application handler does with a failed call: log the mapped error and its safe fields.
	if m, ok := errors.AsType[*database.MappedError](database.MapError(dupErr)); ok {
		log.Emit("transfer", func(l logger.LoggerLevel) { l.Error(m, "account creation failed", m.SafeFields()...) })
	}

	stopCtx, cancel := context.WithTimeout(bg, time.Second)
	defer cancel()
	_ = log.Stop(stopCtx) // flushes
	_ = w.Close()
	out, err := io.ReadAll(r)
	must(t, err)
	text := string(out)
	if !strings.Contains(text, "23505") || !strings.Contains(text, "invalid_value") {
		t.Fatalf("the logs lost the SQLSTATEs operators need:\n%s", text)
	}
	if strings.Contains(text, "123-45-6789") || strings.Contains(text, "Ann Cruz") {
		t.Fatalf("customer data reached the log output:\n%s", text)
	}
}

func TestBankDBPII_RedactToleratesMalformedErrors(t *testing.T) {
	for _, code := range []string{"", "2", "XX"} {
		noPanic(t, "Redact with code "+code, func() {
			_ = sqlsvc.Redact(&pgconn.PgError{Code: code, Message: "secret " + pii}).Error()
		})
	}
}
