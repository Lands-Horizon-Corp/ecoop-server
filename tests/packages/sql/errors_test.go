package sql_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/uptrace/bun/driver/pgdriver"

	ecsql "github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sql"
)

// No build tag: classifying an error needs no database.

func pgErr(code string) error {
	return pgdriver.NewError(map[byte]string{
		'C': code, 'S': "ERROR", 'M': "secret row value 12345",
		't': "accounts", 'c': "balance", 'n': "accounts_check",
	})
}

// Every class of error lands in exactly one of: transient (retry unchanged),
// permanent (the data is the problem), or neither (wait for another change or a
// migration). Dropping a change is only ever allowed for permanent.
func TestErrors_Classification(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		err       error
		transient bool
		permanent bool
	}{
		{"nil", nil, false, false},
		{"eof", io.EOF, true, false},
		{"unexpected eof", io.ErrUnexpectedEOF, true, false},
		{"context canceled", context.Canceled, true, false},
		{"deadline", context.DeadlineExceeded, true, false},
		{"plain network error", errors.New("connection reset by peer"), true, false},
		{"serialization", pgErr("40001"), true, false},
		{"deadlock", pgErr("40P01"), true, false},
		{"read only after failover", pgErr("25006"), true, false},
		{"admin shutdown", pgErr("57P01"), true, false},
		{"crash shutdown", pgErr("57P02"), true, false},
		{"cannot connect now", pgErr("57P03"), true, false},
		{"statement timeout", pgErr("57014"), true, false},
		{"connection failure", pgErr("08006"), true, false},
		{"too many connections", pgErr("53300"), true, false},
		{"out of memory", pgErr("53200"), true, false},
		{"lock not available", pgErr("55P03"), true, false},
		{"bad text encoding", pgErr("22021"), false, true},
		{"numeric out of range", pgErr("22003"), false, true},
		{"invalid datetime", pgErr("22007"), false, true},
		{"not null violation", pgErr("23502"), false, true},
		{"check violation", pgErr("23514"), false, true},
		{"row too big", pgErr("54000"), false, true},
		{"foreign key: a parent may still arrive", pgErr("23503"), false, false},
		{"unique: the order of changes may resolve it", pgErr("23505"), false, false},
		{"undefined column: a migration may resolve it", pgErr("42703"), false, false},
		{"undefined table", pgErr("42P01"), false, false},
		{"pgconn error is read too", &pgconn.PgError{Code: "22003"}, false, true},
		{"wrapped pg error is found", fmt.Errorf("upserting: %w", pgErr("40001")), true, false},
		{"joined errors are found", errors.Join(errors.New("a"), pgErr("22003")), false, true},
	}
	for _, tc := range cases {
		transient, permanent := ecsql.IsTransient(tc.err), ecsql.IsPermanent(tc.err)
		if transient != tc.transient {
			t.Errorf("%s: IsTransient = %v, want %v", tc.name, transient, tc.transient)
		}
		if permanent != tc.permanent {
			t.Errorf("%s: IsPermanent = %v, want %v", tc.name, permanent, tc.permanent)
		}
		if transient && permanent {
			t.Errorf("%s: both transient and permanent", tc.name)
		}
	}
}

// Describe never leaks the message of a PostgreSQL error, which can quote the row.
func TestErrors_DescribeHidesValues(t *testing.T) {
	t.Parallel()
	got := ecsql.Describe(pgErr("22003"))
	for _, leak := range []string{"secret", "12345"} {
		if strings.Contains(got, leak) {
			t.Fatalf("Describe leaked %q: %s", leak, got)
		}
	}
	for _, want := range []string{"22003", "accounts", "balance"} {
		if !strings.Contains(got, want) {
			t.Fatalf("Describe lost %q: %s", want, got)
		}
	}
	if ecsql.Describe(nil) != "" || ecsql.Describe(errors.New("plain")) != "plain" {
		t.Fatal("a non-pg error is described by its own text, nil by nothing")
	}
	if ecsql.SQLState(errors.New("x")) != "" || ecsql.SQLState(pgErr("40001")) != "40001" {
		t.Fatal("SQLState")
	}
}
