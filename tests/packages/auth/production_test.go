//go:build integration

package auth_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/auth"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/security"
)

// Production: what happens around the service in a real deployment.

// A Valkey outage is "the session store failed" (the app answers 503 and
// the user retries), never "you are signed out"; sessions survive it.
func TestProduction_CacheOutageIsNotASignOut(t *testing.T) {
	t.Parallel()
	p := newProxy(t)
	ch := cache.NewCacheService([]string{p.addr()}, valkeyPassword, nil)
	if err := ch.Run(ctxT(t)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ch.Stop(context.Background()) })
	a, _, _ := newAuth(t, ch)
	base := api(t, a)
	b := newBrowser(t, base, site)
	b.do(t, http.MethodPost, "/login?id=u-5")
	p.down()
	if _, code := b.me(t); code != http.StatusServiceUnavailable {
		t.Fatalf("during the outage: %d, want 503 (a 401 would sign the user out)", code)
	}
	p.up()
	var code int
	for range 20 { // the cache reconnects on its own
		if _, code = b.me(t); code == http.StatusOK {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if code != http.StatusOK {
		t.Fatalf("after the outage: %d; the session should have survived", code)
	}
}

// Rotating the security secret (a leaked key, a scheduled rotation) ends
// every session cleanly: invalid, not an error page or a crash.
func TestProduction_SecretRotationEndsSessions(t *testing.T) {
	t.Parallel()
	a, ch, name := newAuth(t)
	token := login(t, a, ana, site)
	rotated := auth.NewAuthService[user](name, header, false, ch,
		security.NewSecurityService("a-new-secret", 8*1024, 1, 2, 16, 32))
	if _, err := rotated.VerifyCSRF(ctxT(t), token); !errors.Is(err, auth.ErrInvalidToken) {
		t.Fatalf("session from the old secret: %v, want ErrInvalidToken", err)
	}
	if _, err := rotated.VerifyCSRF(ctxT(t), login(t, rotated, ana, site)); err != nil {
		t.Fatalf("new sessions after rotation: %v", err)
	}
}

// userV2 is the claim after a deploy that added a field.
type userV2 struct {
	ID       string `json:"id"`
	Email    string `json:"email"`
	Role     string `json:"role"`
	BranchID string `json:"branch_id"`
}

func (u userV2) GetID() string { return u.ID }

// Sessions made before a deploy that changes the claim keep working after
// it: users are not all signed out by a release.
func TestProduction_ClaimEvolutionAcrossADeploy(t *testing.T) {
	t.Parallel()
	before, ch, name := newAuth(t)
	token := login(t, before, ana, site)
	after := auth.NewAuthService[userV2](name, header, false, ch, newSecurity())
	got, err := after.VerifyCSRF(ctxT(t), token)
	if err != nil || got.ID != ana.ID || got.Email != ana.Email || got.BranchID != "" {
		t.Fatalf("old session after the deploy = %+v, %v", got, err)
	}
}

// A user who signs in often does not grow an index forever: expired
// sessions are pruned the next time it is read.
func TestProduction_ExpiredSessionsArePruned(t *testing.T) {
	t.Parallel()
	a, ch, name := newAuth(t)
	for range 30 {
		if err := a.SetCSRF(ctxT(t), request(site), ana, time.Second); err != nil {
			t.Fatal(err)
		}
	}
	keep := login(t, a, ana, site)
	time.Sleep(2100 * time.Millisecond)
	if _, err := a.HasActiveSessionOnOtherDevice(ctxT(t), with(keep, site)); err != nil {
		t.Fatal(err)
	}
	if n, err := ch.ZCard(ctxT(t), name+":auth:user:"+ana.ID); err != nil || n != 1 {
		t.Fatalf("index holds %d sessions after expiry, want 1 (%v)", n, err)
	}
}

// Once a revoke returns, no request may still get in: there is no window
// where a stolen session keeps working.
func TestProduction_RevokeTakesEffectImmediately(t *testing.T) {
	t.Parallel()
	a, _, _ := newAuth(t)
	token := login(t, a, ana, site)
	var revokedAt atomic.Int64
	var lateSuccess atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				start := time.Now().UnixNano()
				_, err := a.VerifyCSRF(ctxT(t), token)
				if r := revokedAt.Load(); err == nil && r != 0 && start > r {
					lateSuccess.Add(1)
				}
			}
		})
	}
	time.Sleep(100 * time.Millisecond)
	if err := a.RevokeUserSessions(ctxT(t), ana.ID); err != nil {
		t.Fatal(err)
	}
	revokedAt.Store(time.Now().UnixNano())
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
	if n := lateSuccess.Load(); n > 0 {
		t.Fatalf("%d requests got in after the revoke returned", n)
	}
}

// Many users on several devices at once: every session works, sign-out of
// other devices leaves exactly one per user, and the indexes agree.
func TestProduction_ManyUsersManyDevices(t *testing.T) {
	t.Parallel()
	a, ch, name := newAuth(t)
	const users, devices = 30, 3
	var wg sync.WaitGroup
	for u := range users {
		wg.Go(func() {
			who := user{ID: fmt.Sprintf("u-%d", u), Email: fmt.Sprintf("u%d@ecoop.test", u)}
			tokens := make([]string, devices)
			for d := range tokens {
				tokens[d] = login(t, a, who, site)
			}
			for _, tok := range tokens {
				if got, err := a.VerifyCSRF(ctxT(t), tok); err != nil || got != who {
					t.Errorf("user %d: %+v, %v", u, got, err)
					return
				}
			}
			if err := a.RevokeOtherDeviceSessions(ctxT(t), with(tokens[0], site)); err != nil {
				t.Errorf("user %d: %v", u, err)
				return
			}
			for d, tok := range tokens {
				_, err := a.VerifyCSRF(ctxT(t), tok)
				if (d == 0) != (err == nil) {
					t.Errorf("user %d device %d after signing out others: %v", u, d, err)
				}
			}
			if n, _ := ch.ZCard(ctxT(t), name+":auth:user:"+who.ID); n != 1 {
				t.Errorf("user %d index holds %d sessions, want 1", u, n)
			}
		})
	}
	wg.Wait()
}

// proxy relays TCP to Valkey and can take it away.
type proxy struct {
	ln    net.Listener
	mu    sync.Mutex
	conns []net.Conn
	off   atomic.Bool
}

func newProxy(t *testing.T) *proxy {
	t.Helper()
	if skipReason != "" {
		t.Skip(skipReason)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &proxy{ln: ln}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			if p.off.Load() {
				_ = c.Close()
				continue
			}
			s, err := net.Dial("tcp", valkeyAddr)
			if err != nil {
				_ = c.Close()
				continue
			}
			p.mu.Lock()
			p.conns = append(p.conns, c, s)
			p.mu.Unlock()
			go func() { relay(s, c) }()
			go func() { relay(c, s) }()
		}
	}()
	t.Cleanup(func() { _ = ln.Close(); p.down() })
	return p
}

func relay(dst, src net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				break
			}
		}
		if err != nil {
			break
		}
	}
	_ = dst.Close()
	_ = src.Close()
}

func (p *proxy) addr() string { return p.ln.Addr().String() }

func (p *proxy) down() {
	p.off.Store(true)
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.conns = nil
}

func (p *proxy) up() { p.off.Store(false) }
