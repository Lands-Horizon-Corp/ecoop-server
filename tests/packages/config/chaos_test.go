package config_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/config"
)

// Chaos: random sequences of good and bad edits checked against a model. The
// service must always serve the last good config and never get stuck.

func seed(t *testing.T) *rand.Rand {
	s := uint64(time.Now().UnixNano())
	t.Logf("seed %d", s)
	return rand.New(rand.NewPCG(s, s^0x9e3779b97f4a7c15))
}

func TestChaos_RandomEditsAgainstAModel(t *testing.T) {
	rng := seed(t)
	svc, reloads := started(t, map[string]string{config.EnvFile: baseEnv + "APP_NAME=n0\nPORT=1\n"})
	want := struct {
		name string
		port int
	}{"n0", 1}

	for i := 1; i <= 120; i++ {
		var (
			body    string
			wantErr bool
			name    = fmt.Sprintf("n%d", i)
			port    = 1 + rng.IntN(60000)
		)
		switch rng.IntN(5) {
		case 0, 1: // a good edit
			body = fmt.Sprintf("%sAPP_NAME=%s\nPORT=%d\n", baseEnv, name, port)
		case 2: // a value that does not parse
			body, wantErr = fmt.Sprintf("%sAPP_NAME=%s\nPORT=%dx\n", baseEnv, name, port), true
		case 3: // a required key goes missing
			body, wantErr = fmt.Sprintf("JWT_SECRET=s\nAPP_NAME=%s\n", name), true
		case 4: // the file disappears
			if err := os.Remove(config.EnvFile); err != nil {
				t.Fatal(err)
			}
			wantErr = true
		}
		if body != "" {
			write(t, config.EnvFile, body)
		}
		err := nextReload(t, reloads)
		if wantErr != (err != nil) {
			t.Fatalf("step %d: reload error = %v, want error=%v", i, err, wantErr)
		}
		if !wantErr {
			want.name, want.port = name, port
		}
		cfg, gerr := svc.Get(context.Background())
		if gerr != nil || cfg.Name != want.name || cfg.Port != want.port {
			t.Fatalf("step %d: got %+v, %v; want name=%s port=%d", i, cfg, gerr, want.name, want.port)
		}
	}
}

func TestChaos_ManyServicesIndependentFiles(t *testing.T) {
	// Several services in one process share the working directory, so each
	// must see the same edits and end on the same config without crosstalk.
	setup(t, map[string]string{config.EnvFile: baseEnv + "APP_NAME=a\n"}, nil)
	var svcs []interface {
		Stop(context.Context) error
	}
	type pair struct {
		get func() (appCfg, error)
		ch  chan error
	}
	var ps []pair
	for range 6 {
		s, _ := config.NewConfigService[appCfg]()
		s.ReloadInterval = 5 * time.Millisecond
		ch := make(chan error, 64)
		s.OnReload = func(err error) { ch <- err }
		if err := s.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := s.Run(context.Background()); err != nil {
			t.Fatal(err)
		}
		svcs = append(svcs, s)
		ps = append(ps, pair{func() (appCfg, error) { return s.Get(context.Background()) }, ch})
	}
	t.Cleanup(func() {
		for _, s := range svcs {
			_ = s.Stop(context.Background())
		}
	})
	write(t, config.EnvFile, baseEnv+"APP_NAME=b\n")
	for i, p := range ps {
		if err := nextReload(t, p.ch); err != nil {
			t.Fatal(err)
		}
		if cfg, _ := p.get(); cfg.Name != "b" {
			t.Fatalf("service %d name = %q", i, cfg.Name)
		}
	}
}
