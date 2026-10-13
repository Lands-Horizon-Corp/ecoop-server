package utils

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/schema"
	"golang.org/x/text/unicode/norm"
)

var (
	ErrNilTx       = errors.New("sql: transaction is nil or was never started")
	ErrUnsafeText  = errors.New("sql: text contains a replacement, control or BiDi override character")
	ErrInvalidText = errors.New("sql: text is not valid UTF-8 or contains a NUL byte")
)

type (
	tenantKey struct{}
	noLockKey struct{}

	// TextPolicy decides how CleanText treats valid text. Invalid UTF-8 (including truncated and overlong
	// sequences) and NUL bytes are always refused: Postgres cannot store them and bun inlines values into
	// the SQL text, where a NUL cuts the statement short.
	TextPolicy struct {
		// Normalize rewrites strings to NFC, so "Müller" typed as u + U+0308 and as U+00FC is one value.
		Normalize bool
		// RejectUnsafe refuses U+FFFD (the trace of a lossy conversion), C0/C1 control characters other
		// than tab, newline and carriage return, and the BiDi embedding, override and isolate characters
		// (U+202A..U+202E, U+2066..U+2069). Other format characters, such as the zero-width joiner inside
		// emoji sequences, stay allowed.
		RejectUnsafe bool
	}
)

func ResolvePreload(preload []string, defaultPreloads []string) []string {
	if preload == nil {
		preload = defaultPreloads
	}
	if len(preload) == 0 {
		preload = defaultPreloads
	}
	if len(preload) == 1 && preload[0] == "" {
		preload = []string{}
	}
	return preload
}

func ValidPreloads[TData any](db bun.IDB, preload []string) (valid, dropped []string) {
	table := db.Dialect().Tables().Get(reflect.TypeFor[TData]())
	for _, raw := range preload {
		if raw == "" {
			continue
		}
		resolved, ok := resolveRelationPath(table, raw)
		if !ok {
			dropped = append(dropped, raw)
			continue
		}
		valid = append(valid, resolved)
	}
	return valid, dropped
}

func resolveRelationPath(table *schema.Table, raw string) (resolved string, ok bool) {
	segments := strings.Split(raw, ".")
	names := make([]string, 0, len(segments))
	current := table
	for _, seg := range segments {
		if current == nil {
			return "", false
		}
		name := ToPascalCase(seg)
		if name == "" {
			return "", false
		}
		rel, exists := current.Relations[name]
		if !exists {
			return "", false
		}
		names = append(names, name)
		current = rel.JoinTable
	}
	return strings.Join(names, "."), true
}

func ApplyPreloadsMany[TData any](
	ctx context.Context,
	db bun.IDB,
	data *[]TData,
	defaultPreloads []string,
	preload ...string,
) (dropped []string, err error) {
	resolved := ResolvePreload(preload, defaultPreloads)
	if len(resolved) == 0 || len(*data) == 0 {
		return nil, nil
	}
	valid, dropped := ValidPreloads[TData](db, resolved)
	if len(valid) == 0 {
		return dropped, nil
	}
	q := db.NewSelect().Model(data).WherePK()
	for _, rel := range valid {
		q = q.Relation(rel)
	}
	if err := q.Scan(ctx); err != nil {
		return dropped, fmt.Errorf("loading preloads %v: %w", valid, err)
	}
	return dropped, nil
}

func RowLocksDisabled(ctx context.Context) bool {
	off, _ := ctx.Value(noLockKey{}).(bool)
	return off
}
func TenantFrom(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(tenantKey{}).(string)
	return id, ok && id != ""
}
func Scoped[T any](ctx context.Context, db bun.IDB, fn func(q bun.IDB) (T, error)) (T, error) {
	pool, pooled := db.(*bun.DB)
	if _, ok := TenantFrom(ctx); !ok || !pooled {
		return RetryStale(db, func() (T, error) { return fn(db) })
	}
	return RetryStale(db, func() (T, error) {
		var out T
		err := pool.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
			if err := SetTenant(ctx, tx); err != nil {
				return err
			}
			var err error
			out, err = fn(tx)
			return err
		})
		return out, err
	})
}

func RetryStale[T any](db bun.IDB, fn func() (T, error)) (T, error) {
	v, err := fn()
	if _, pooled := db.(*bun.DB); pooled && IsStaleSession(err) {
		return fn()
	}
	return v, err
}

func SetTenant(ctx context.Context, tx bun.IDB) error {
	id, ok := TenantFrom(ctx)
	if !ok {
		return nil
	}
	_, err := tx.ExecContext(ctx, `SELECT set_config('app.tenant_id', ?, true)`, id)
	return err
}

func IsStaleSession(err error) bool {
	if err == nil {
		return false
	}
	if pg, ok := errors.AsType[*pgconn.PgError](err); ok {
		return pg.Code == "57P01" || pg.Code == "57P02" || pg.Code == "57P03"
	}
	return pgconn.SafeToRetry(err) || errors.Is(err, driver.ErrBadConn) || strings.Contains(err.Error(), "conn closed")
}

func CheckDB(db bun.IDB, notInit error) error {
	switch v := db.(type) {
	case nil:
		return notInit
	case *bun.DB:
		if v == nil {
			return notInit
		}
	case bun.Tx:
		if v.Tx == nil {
			return ErrNilTx
		}
	case *bun.Tx:
		if v == nil || v.Tx == nil {
			return ErrNilTx
		}
	}
	return nil
}

func CleanText(v reflect.Value, p TextPolicy) (reflect.Value, error) {
	switch v.Kind() {
	case reflect.String:
		s, err := cleanString(v.String(), p)
		if err != nil {
			return v, err
		}
		out := reflect.New(v.Type()).Elem()
		out.SetString(s)
		return out, nil
	case reflect.Pointer, reflect.Interface:
		if v.IsNil() {
			return v, nil
		}
		e, err := CleanText(v.Elem(), p)
		if err != nil {
			return v, err
		}
		if v.Kind() == reflect.Pointer {
			ptr := reflect.New(v.Type().Elem())
			ptr.Elem().Set(e)
			return ptr, nil
		}
		out := reflect.New(v.Type()).Elem()
		out.Set(e)
		return out, nil
	case reflect.Slice:
		if v.IsNil() || v.Type().Elem().Kind() == reflect.Uint8 { // bytes are bytea, not text
			return v, nil
		}
		out := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		for i := range v.Len() {
			e, err := CleanText(v.Index(i), p)
			if err != nil {
				return v, err
			}
			out.Index(i).Set(e)
		}
		return out, nil
	case reflect.Map:
		if v.IsNil() {
			return v, nil
		}
		out := reflect.MakeMapWithSize(v.Type(), v.Len())
		iter := v.MapRange()
		for iter.Next() {
			k, err := CleanText(iter.Key(), p)
			if err != nil {
				return v, err
			}
			if out.MapIndex(k).IsValid() {
				return v, fmt.Errorf("%w: two keys are the same text after normalization (%v)", ErrUnsafeText, k)
			}
			val, err := CleanText(iter.Value(), p)
			if err != nil {
				return v, err
			}
			out.SetMapIndex(k, val)
		}
		return out, nil
	}
	return v, nil
}

func cleanString(s string, p TextPolicy) (string, error) {
	if !utf8.ValidString(s) {
		return "", fmt.Errorf("%w: invalid UTF-8", ErrInvalidText)
	}
	if strings.IndexByte(s, 0) >= 0 {
		return "", fmt.Errorf("%w: contains a NUL byte", ErrInvalidText)
	}
	if p.RejectUnsafe {
		for _, r := range s {
			switch {
			case r == utf8.RuneError:
				return "", fmt.Errorf("%w: contains U+FFFD, the trace of a lossy conversion", ErrUnsafeText)
			case r < 0x20 && r != '\t' && r != '\n' && r != '\r', r >= 0x7f && r <= 0x9f:
				return "", fmt.Errorf("%w: contains control character U+%04X", ErrUnsafeText, r)
			case r >= 0x202a && r <= 0x202e, r >= 0x2066 && r <= 0x2069:
				return "", fmt.Errorf("%w: contains BiDi control U+%04X", ErrUnsafeText, r)
			}
		}
	}
	if p.Normalize {
		return norm.NFC.String(s), nil
	}
	return s, nil

}
