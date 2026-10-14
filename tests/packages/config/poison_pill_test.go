package config_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/config"
)

// Poison pill: hostile or corrupt files and values must produce an error (or
// a sane value), never a panic or a hang, and a reload that meets one must
// keep the config that was already serving.

// within fails the test when f panics or does not return in time.
func within(t *testing.T, d time.Duration, f func()) {
	t.Helper()
	done := make(chan any, 1)
	go func() {
		defer func() { done <- recover() }()
		f()
	}()
	select {
	case p := <-done:
		if p != nil {
			t.Fatalf("panic: %v", p)
		}
	case <-time.After(d):
		t.Fatalf("did not return within %v", d)
	}
}

var poison = map[string]string{
	"binary garbage":           "\x00\x01\x02\xff\xfe\x00garbage\x00",
	"invalid utf-8":            "A=\xc3\x28\n",
	"nul bytes in a value":     "DATABASE_URL=a\x00b\nJWT_SECRET=s\n",
	"top-level list":           "- a\n- b\n",
	"top-level scalar":         "just a string\n",
	"unclosed flow":            "a: [1, 2\n",
	"tab indentation":          "a:\n\tb: 1\n",
	"unknown tag":              "a: !!python/object:os.system x\n",
	"duplicate keys":           "a: 1\na: 2\n",
	"only comments":            "# nothing\n# here\n",
	"null document":            "~\n",
	"empty file":               "",
	"env as a scalar":          "env: 5\n",
	"env list of junk":         "env: [1, [2], {name: x}, {value: y}, null]\n",
	"secret with list data":    "kind: Secret\ndata: {X: [a, b]}\n",
	"secret with non-map data": "kind: Secret\ndata: 7\nstringData: nope\n",
	"deep nesting":             strings.Repeat("a: {", 500) + "b: 1" + strings.Repeat("}", 500) + "\n",
	"yaml alias bomb":          "a: &a [x,x,x,x,x,x,x,x,x]\nb: &b [*a,*a,*a,*a,*a,*a,*a,*a,*a]\nc: &c [*b,*b,*b,*b,*b,*b,*b,*b,*b]\nd: &d [*c,*c,*c,*c,*c,*c,*c,*c,*c]\ne: [*d,*d,*d,*d,*d,*d,*d,*d,*d]\n",
	"huge value":               "DATABASE_URL=" + strings.Repeat("x", 1<<20) + "\nJWT_SECRET=s\n",
	"huge slice":               "DATABASE_URL=d\nJWT_SECRET=s\nEXTRA_PORTS=" + strings.TrimSuffix(strings.Repeat("1,", 20000), ",") + "\n",
	"shell-looking value":      "DATABASE_URL=$(rm -rf /)\nJWT_SECRET=`id`\n",
}

func TestPoison_FilesNeverPanicOrHang(t *testing.T) {
	for name, body := range poison {
		for _, file := range []string{config.EnvFile, config.HelmFile, config.SecretsFile} {
			t.Run(name+"/"+file, func(t *testing.T) {
				setup(t, map[string]string{file: body}, required)
				within(t, 20*time.Second, func() {
					cfg, err := load(t)
					if err == nil && (cfg.DSN == "" && cfg.Secret == "") {
						t.Errorf("no error but required values are gone: %+v", cfg)
					}
				})
			})
		}
	}
}

func TestPoison_ShellSyntaxIsNeverExecuted(t *testing.T) {
	setup(t, map[string]string{config.EnvFile: "DATABASE_URL=$(echo pwned)\nJWT_SECRET=`echo pwned`\n"}, nil)
	cfg, err := load(t)
	if err == nil && strings.Contains(cfg.DSN+cfg.Secret, "pwned") && !strings.Contains(cfg.DSN, "$(") && !strings.Contains(cfg.Secret, "`") {
		t.Fatalf("a command was expanded: %+v", cfg)
	}
}

func TestPoison_ReloadKeepsTheServingConfig(t *testing.T) {
	for name, body := range poison {
		if name == "only comments" || name == "empty file" || name == "null document" {
			continue // these are valid empty files; the .env keeps the required keys
		}
		t.Run(name, func(t *testing.T) {
			svc, reloads := started(t, map[string]string{config.EnvFile: baseEnv + "APP_NAME=good\n"})
			write(t, config.HelmFile, body)
			select {
			case <-reloads:
			case <-time.After(10 * time.Second):
				t.Fatal("no reload")
			}
			cfg, err := svc.Get(context.Background())
			if err != nil {
				t.Fatalf("Get after a poisoned reload: %v", err)
			}
			if cfg.DSN == "" || cfg.Secret == "" {
				t.Fatalf("serving config was lost: %+v", cfg)
			}
		})
	}
}

func TestPoison_UnsupportedAndOddTargetTypes(t *testing.T) {
	t.Run("map field", func(t *testing.T) {
		type c struct {
			M map[string]string `env:"M" envDefault:"a=b"`
		}
		setup(t, nil, nil)
		svc, _ := config.NewConfigService[c]()
		if err := svc.Start(context.Background()); !errors.Is(err, config.ErrInvalidValue) {
			t.Fatalf("err = %v, want ErrInvalidValue", err)
		}
	})
	t.Run("no exported fields", func(t *testing.T) {
		type c struct{ a int }
		setup(t, nil, nil)
		svc, _ := config.NewConfigService[c]()
		var startErr error
		within(t, time.Second, func() { startErr = svc.Start(context.Background()) })
		if _, err := svc.Get(context.Background()); (err == nil) != (startErr == nil) {
			t.Fatalf("Start = %v but Get = %v", startErr, err)
		}
	})
	t.Run("unexported and untagged fields are ignored", func(t *testing.T) {
		type c struct {
			Tagged   string `env:"APP_NAME"`
			Untagged string
			hidden   string
		}
		setup(t, nil, map[string]string{"APP_NAME": "x"})
		svc, _ := config.NewConfigService[c]()
		if err := svc.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got, _ := svc.Get(context.Background()); got.Tagged != "x" || got.Untagged != "" || got.hidden != "" {
			t.Fatalf("got %+v", got)
		}
	})
	t.Run("integers overflow their width", func(t *testing.T) {
		type c struct {
			N int8 `env:"N"`
		}
		setup(t, nil, map[string]string{"N": "128"})
		svc, _ := config.NewConfigService[c]()
		if err := svc.Start(context.Background()); !errors.Is(err, config.ErrInvalidValue) {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestPoison_UnreadableAndMisplacedFiles(t *testing.T) {
	t.Run("a directory named .env", func(t *testing.T) {
		setup(t, nil, required)
		if err := os.Mkdir(config.EnvFile, 0o755); err != nil {
			t.Fatal(err)
		}
		within(t, 5*time.Second, func() {
			if _, err := load(t); err == nil {
				t.Error("a directory was accepted as .env")
			}
		})
	})
	t.Run("a permission-denied file", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root reads everything")
		}
		setup(t, map[string]string{config.HelmFile: "a: 1\n"}, required)
		if err := os.Chmod(config.HelmFile, 0); err != nil {
			t.Fatal(err)
		}
		if _, err := load(t); err == nil || !strings.Contains(err.Error(), config.HelmFile) {
			t.Fatalf("err = %v, want one naming %s", err, config.HelmFile)
		}
	})
}
