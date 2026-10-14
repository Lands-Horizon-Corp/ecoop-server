package config_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/config"
)

// Integration chaos: the real filesystem behaving the way editors, deploy
// tools and Kubernetes mount updates do.

func TestIntegrationChaos_AtomicRenameWrites(t *testing.T) {
	svc, reloads := started(t, map[string]string{config.EnvFile: baseEnv + "APP_NAME=v0\n"})
	for i := 1; i <= 20; i++ {
		tmp := fmt.Sprintf(".env.tmp%d", i)
		write(t, tmp, fmt.Sprintf("%sAPP_NAME=v%d\n", baseEnv, i))
		if err := os.Rename(tmp, config.EnvFile); err != nil {
			t.Fatal(err)
		}
		if err := nextReload(t, reloads); err != nil {
			t.Fatal(err)
		}
		if cfg, _ := svc.Get(context.Background()); cfg.Name != fmt.Sprintf("v%d", i) {
			t.Fatalf("round %d: name = %q", i, cfg.Name)
		}
	}
}

// Kubernetes updates a mounted ConfigMap by atomically repointing the ..data
// symlink at a new timestamped directory; .env is a symlink through it.
func TestIntegrationChaos_KubernetesSymlinkSwap(t *testing.T) {
	setup(t, nil, nil)
	publish := func(n int) {
		dir := fmt.Sprintf("..ts%d", n)
		if err := os.Mkdir(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		write(t, filepath.Join(dir, config.EnvFile), fmt.Sprintf("%sAPP_NAME=k%d\n", baseEnv, n))
		link := fmt.Sprintf("..data_tmp%d", n)
		if err := os.Symlink(dir, link); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(link, "..data"); err != nil {
			t.Fatal(err)
		}
	}
	publish(0)
	if err := os.Symlink(filepath.Join("..data", config.EnvFile), config.EnvFile); err != nil {
		t.Fatal(err)
	}
	svc, _ := config.NewConfigService[appCfg]()
	svc.ReloadInterval = 10 * time.Millisecond
	reloads := make(chan error, 16)
	svc.OnReload = func(err error) { reloads <- err }
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := svc.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = svc.Stop(context.Background()) })
	for n := 1; n <= 5; n++ {
		publish(n)
		if err := nextReload(t, reloads); err != nil {
			t.Fatal(err)
		}
		if cfg, _ := svc.Get(context.Background()); cfg.Name != fmt.Sprintf("k%d", n) {
			t.Fatalf("after swap %d: name = %q", n, cfg.Name)
		}
	}
}

func TestIntegrationChaos_FileBecomesDirectoryThenRecovers(t *testing.T) {
	svc, reloads := started(t, map[string]string{config.EnvFile: baseEnv + "APP_NAME=ok\n"})
	if err := os.Remove(config.EnvFile); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(config.EnvFile, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := nextReload(t, reloads); err == nil {
		t.Fatal("a directory in place of .env reloaded without error")
	}
	if cfg, err := svc.Get(context.Background()); err != nil || cfg.Name != "ok" {
		t.Fatalf("serving config lost: %+v, %v", cfg, err)
	}
	if err := os.Remove(config.EnvFile); err != nil {
		t.Fatal(err)
	}
	write(t, config.EnvFile, baseEnv+"APP_NAME=back\n")
	// The removal and the rewrite may be seen as one change or two.
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-reloads:
		case <-deadline:
			t.Fatal("never recovered")
		}
		if cfg, _ := svc.Get(context.Background()); cfg.Name == "back" {
			return
		}
	}
}

// A writer that truncates and rewrites in place lets a reload see half a
// file. Whatever it sees, Get must keep working, and once the writer is done
// the config must converge on the final content.
func TestIntegrationChaos_TornWritesConverge(t *testing.T) {
	svc, _ := started(t, map[string]string{config.EnvFile: baseEnv + "APP_NAME=v0\n"})
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := svc.Get(context.Background()); err != nil {
				t.Errorf("Get failed during torn writes: %v", err)
				return
			}
		}
	})
	for i := 1; i <= 50; i++ {
		f, err := os.OpenFile(config.EnvFile, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		body := fmt.Sprintf("%sAPP_NAME=v%d\n", baseEnv, i)
		_, _ = f.WriteString(body[:len(body)/2])
		time.Sleep(time.Millisecond)
		_, _ = f.WriteString(body[len(body)/2:])
		_ = f.Close()
	}
	close(stop)
	wg.Wait()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cfg, _ := svc.Get(context.Background()); cfg.Name == "v50" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	cfg, _ := svc.Get(context.Background())
	t.Fatalf("did not converge: name = %q, want v50", cfg.Name)
}
