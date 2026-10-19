package env_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

func look(kv map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := kv[k]; return v, ok }
}

func TestDefaultsApplyWhenUnsetOrBlank(t *testing.T) {
	t.Parallel()
	e := utils.NewEnv(look(map[string]string{"BLANK": "   ", "EMPTY": ""}))
	if got := e.String("NOPE", "d"); got != "d" {
		t.Errorf("unset String = %q", got)
	}
	if got := e.String("BLANK", "d"); got != "d" {
		t.Errorf("blank String = %q, want the default: a blank value counts as unset", got)
	}
	if got := e.String("EMPTY", "d"); got != "d" {
		t.Errorf("empty String = %q", got)
	}
	if e.Int("NOPE", 7) != 7 || e.Int64("NOPE", 8) != 8 || !e.Bool("NOPE", true) || e.Duration("NOPE", time.Second) != time.Second {
		t.Error("a typed default was not applied")
	}
	if e.List("NOPE") != nil {
		t.Error("an unset List is not nil")
	}
	if err := e.Err(); err != nil {
		t.Errorf("defaults produced an error: %v", err)
	}
}

func TestValuesAreParsedAndTrimmed(t *testing.T) {
	t.Parallel()
	e := utils.NewEnv(look(map[string]string{
		"S": "  hi  ", "I": " 42 ", "I64": "9223372036854775807", "B1": "true", "B2": "0", "B3": "T",
		"D": "1m30s", "L": " a , b ,, c ",
	}))
	if e.String("S", "") != "hi" || e.Int("I", 0) != 42 || e.Int64("I64", 0) != 9223372036854775807 {
		t.Error("a scalar was not parsed")
	}
	if !e.Bool("B1", false) || e.Bool("B2", true) || !e.Bool("B3", false) {
		t.Error("a bool was not parsed")
	}
	if e.Duration("D", 0) != 90*time.Second {
		t.Error("a duration was not parsed")
	}
	if got := e.List("L"); len(got) != 3 || got[0] != "a" || got[2] != "c" {
		t.Errorf("List = %q", got)
	}
	if err := e.Err(); err != nil {
		t.Errorf("valid values produced an error: %v", err)
	}
}

func TestEveryProblemIsReportedAtOnce(t *testing.T) {
	t.Parallel()
	e := utils.NewEnv(look(map[string]string{"I": "x", "B": "maybe", "D": "-5s", "D2": "soon", "I64": "1.5"}))
	e.Required("MISSING_A")
	e.Required("MISSING_B")
	e.Int("I", 1)
	e.Bool("B", false)
	e.Duration("D", 0)
	e.Duration("D2", 0)
	e.Int64("I64", 0)
	err := e.Err()
	if err == nil {
		t.Fatal("no error for bad values")
	}
	if !errors.Is(err, utils.ErrEnvMissing) || !errors.Is(err, utils.ErrEnvInvalid) {
		t.Errorf("the error does not wrap both kinds: %v", err)
	}
	for _, name := range []string{"MISSING_A", "MISSING_B", "I ", "B ", "D ", "D2 ", "I64 "} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("the error does not mention %q: %v", strings.TrimSpace(name), err)
		}
	}
}

// A secret that does not parse must not end up in an error, and so in a log.
func TestAnErrorNamesTheVariableAndNeverTheValue(t *testing.T) {
	t.Parallel()
	const secret = "hunter2-not-a-number"
	e := utils.NewEnv(look(map[string]string{"DB_PORT": secret, "FLAG": secret, "WAIT": secret}))
	e.Int("DB_PORT", 0)
	e.Bool("FLAG", false)
	e.Duration("WAIT", 0)
	if err := e.Err(); err == nil || strings.Contains(err.Error(), "hunter2") {
		t.Errorf("the value leaked into the error: %v", err)
	}
}

func TestInvalidRecordsACheckTheCallerMakes(t *testing.T) {
	t.Parallel()
	e := utils.NewEnv(look(nil))
	e.Invalid("PORT", "between 1 and 65535")
	if err := e.Err(); !errors.Is(err, utils.ErrEnvInvalid) || !strings.Contains(err.Error(), "PORT must be between 1 and 65535") {
		t.Errorf("Invalid = %v", err)
	}
}

func TestRequiredReturnsTheValueWhenSet(t *testing.T) {
	t.Parallel()
	e := utils.NewEnv(look(map[string]string{"K": " v "}))
	if got := e.Required("K"); got != "v" {
		t.Errorf("Required = %q", got)
	}
	if e.Err() != nil {
		t.Error("a set required variable was reported missing")
	}
}

func TestANilLookupReadsTheProcessEnvironment(t *testing.T) {
	t.Setenv("UTILS_ENV_TEST_VALUE", "from-process")
	if got := utils.NewEnv(nil).String("UTILS_ENV_TEST_VALUE", ""); got != "from-process" {
		t.Errorf("nil lookup read %q", got)
	}
}
