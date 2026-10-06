package sql

import (
	"fmt"
	"reflect"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

// TextPolicy decides how CleanText treats valid text. Invalid UTF-8 (including truncated and overlong
// sequences) and NUL bytes are always refused: Postgres cannot store them and bun inlines values into
// the SQL text, where a NUL cuts the statement short.
type TextPolicy struct {
	// Normalize rewrites strings to NFC, so "Müller" typed as u + U+0308 and as U+00FC is one value.
	Normalize bool
	// RejectUnsafe refuses U+FFFD (the trace of a lossy conversion), C0/C1 control characters other
	// than tab, newline and carriage return, and the BiDi embedding, override and isolate characters
	// (U+202A..U+202E, U+2066..U+2069). Other format characters, such as the zero-width joiner inside
	// emoji sequences, stay allowed.
	RejectUnsafe bool
}

// CleanText returns a cleaned copy of v: strings, pointers, interfaces, slices (not []byte) and maps
// (keys and values) are walked; the input is never modified in place.
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
