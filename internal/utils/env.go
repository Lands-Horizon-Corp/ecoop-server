package utils

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// ErrEnvMissing and ErrEnvInvalid are what Env.Err wraps, for errors.Is.
var (
	ErrEnvMissing = errors.New("env: missing required variable")
	ErrEnvInvalid = errors.New("env: invalid value")
)

// Env reads typed settings from the environment and remembers every problem, so
// a service's FromEnv can read all its fields and then report all the mistakes
// at once instead of stopping at the first. An empty value counts as unset.
//
// An error names the variable and what was expected, never the value: a
// malformed password or token must not end up in a log.
type Env struct {
	lookup func(string) (string, bool)
	errs   []error
}

// NewEnv reads through lookup, which is os.LookupEnv in production and a map in
// a test. A nil lookup means os.LookupEnv.
func NewEnv(lookup func(string) (string, bool)) *Env {
	if lookup == nil {
		lookup = os.LookupEnv
	}
	return &Env{lookup: lookup}
}

func (e *Env) get(key string) (string, bool) {
	v, ok := e.lookup(key)
	v = strings.TrimSpace(v)
	return v, ok && v != ""
}

// String is the variable, or def when it is unset.
func (e *Env) String(key, def string) string {
	if v, ok := e.get(key); ok {
		return v
	}
	return def
}

// Required is the variable; an unset one is recorded as an error.
func (e *Env) Required(key string) string {
	v, ok := e.get(key)
	if !ok {
		e.errs = append(e.errs, fmt.Errorf("%w: %s", ErrEnvMissing, key))
	}
	return v
}

// Int is the variable as a whole number, or def when unset.
func (e *Env) Int(key string, def int) int {
	v, ok := e.get(key)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		e.invalid(key, "a whole number")
		return def
	}
	return n
}

// Int64 is Int for 64-bit values such as byte sizes.
func (e *Env) Int64(key string, def int64) int64 {
	v, ok := e.get(key)
	if !ok {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		e.invalid(key, "a whole number")
		return def
	}
	return n
}

// Bool is the variable as true or false (also 1 and 0), or def when unset.
// Anything else is an error: a typo in a security flag must not pass as false.
func (e *Env) Bool(key string, def bool) bool {
	v, ok := e.get(key)
	if !ok {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		e.invalid(key, "true or false")
		return def
	}
	return b
}

// Duration is the variable as a Go duration such as 15s, or def when unset.
func (e *Env) Duration(key string, def time.Duration) time.Duration {
	v, ok := e.get(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		e.invalid(key, "a duration such as 15s")
		return def
	}
	return d
}

// List is the variable split on commas, without blanks; nil when unset.
func (e *Env) List(key string) []string {
	v, _ := e.get(key)
	return SplitList(v)
}

// Invalid records that key holds a value that parsed but is not acceptable,
// for a range or a combination FromEnv checks itself: want completes "key must
// be ...".
func (e *Env) Invalid(key, want string) {
	e.errs = append(e.errs, fmt.Errorf("%w: %s must be %s", ErrEnvInvalid, key, want))
}

func (e *Env) invalid(key, want string) { e.Invalid(key, want) }

// Err is every problem found so far, or nil.
func (e *Env) Err() error { return errors.Join(e.errs...) }
