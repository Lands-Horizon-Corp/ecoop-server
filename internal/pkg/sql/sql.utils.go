package sql

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
	"golang.org/x/text/unicode/norm"
)

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
