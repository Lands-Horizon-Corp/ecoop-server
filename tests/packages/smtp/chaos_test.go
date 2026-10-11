package smtp_test

import (
	"bufio"
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/smtp"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// Chaos: hostile or broken servers, random cancellation, dropped
// connections, and randomly assembled XSS.

func chaosRand(t *testing.T) *rand.Rand {
	t.Helper()
	seed := uint64(time.Now().UnixNano())
	t.Logf("chaos seed: %d", seed)
	return rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
}

func TestChaos_StalledServerIsBoundedByCtx(t *testing.T) {
	t.Parallel()
	svc := newService(t, stalledServer(t))
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := svc.Ping(ctx); err == nil {
		t.Fatal("Ping succeeded against a server that never answers")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("Ping ignored its 300ms deadline: took %v", d)
	}
	// Send only queues, so a stalled server never blocks the caller.
	start = time.Now()
	if err := svc.Send(context.Background(), smtp.SMTPRequest{To: "a@b.co", Body: "x"}); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Send blocked on the SMTP server for %v", d)
	}
}

func TestChaos_GarbageServers(t *testing.T) {
	t.Parallel()
	r := chaosRand(t)
	var mu sync.Mutex
	garbage := func() []byte {
		mu.Lock()
		defer mu.Unlock()
		b := make([]byte, 1+r.IntN(512))
		for i := range b {
			b[i] = byte(r.UintN(256))
		}
		return b
	}
	servers := map[string]func(net.Conn){
		"random bytes":     func(c net.Conn) { _, _ = c.Write(garbage()) },
		"closes at once":   func(c net.Conn) {},
		"error greeting":   func(c net.Conn) { _, _ = c.Write([]byte("554 go away\r\n")) },
		"http server":      func(c net.Conn) { _, _ = c.Write([]byte("HTTP/1.1 400 Bad Request\r\n\r\n")) },
		"endless greeting": func(c net.Conn) { _, _ = c.Write([]byte("220-" + strings.Repeat("x", 1<<20))) },
		"rejects mail": func(c net.Conn) {
			w := bufio.NewWriter(c)
			rd := bufio.NewReader(c)
			_, _ = w.WriteString("220 ok\r\n")
			_ = w.Flush()
			for {
				line, err := rd.ReadString('\n')
				if err != nil {
					return
				}
				if strings.HasPrefix(strings.ToUpper(line), "MAIL") {
					_, _ = w.WriteString("550 rejected\r\n")
				} else {
					_, _ = w.WriteString("250 ok\r\n")
				}
				_ = w.Flush()
			}
		},
	}
	for name, handle := range servers {
		t.Run(name, func(t *testing.T) {
			before := failedDeliveries(t)
			svc := newSMTP(t, serviceOpts{smtpAddr: fakeServer(t, handle)})
			if err := svc.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = svc.Stop(context.Background()) })
			var err error
			mustNotPanic(t, "Send", func() { err = svc.Send(ctxT(t), smtp.SMTPRequest{To: "a@b.co", Body: "x"}) })
			if err != nil {
				t.Fatalf("Send = %v; queueing must not depend on the server", err)
			}
			// The worker hits the broken server, fails cleanly and schedules a retry.
			eventually(t, 10*time.Second, func() bool { return failedDeliveries(t) > before }, "delivery against "+name+" should fail")
		})
	}
}

func TestChaos_ConnectionDropsMidDelivery(t *testing.T) {
	t.Parallel()
	p := newProxy(t)
	svc := newService(t, p.Addr())
	to := inbox(t)
	const n = 20
	for i := range n {
		var err error
		mustNotPanic(t, "Send", func() { err = svc.Send(ctxT(t), smtp.SMTPRequest{To: to, Subject: fmt.Sprint(i), Body: "x"}) })
		if err != nil {
			t.Fatal(err)
		}
	}
	for range 5 {
		time.Sleep(40 * time.Millisecond)
		p.DropAll()
	}
	// Dropped deliveries are retried, so every queued email arrives. Delivery
	// is at-least-once: a drop after the server accepted the message can
	// cause a duplicate, which is why this counts distinct subjects.
	distinct := map[string]bool{}
	for _, m := range received(t, to, n) {
		distinct[m.Subject] = true
	}
	eventually(t, 15*time.Second, func() bool {
		for _, s := range search(t, to) {
			distinct[s.Subject] = true
		}
		return len(distinct) == n
	}, "every queued email should arrive despite dropped connections")
}

func TestChaos_RandomDeadlines(t *testing.T) {
	t.Parallel()
	svc := newService(t)
	to := inbox(t)
	r := chaosRand(t)
	acked := 0
	for range 40 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(r.IntN(30))*time.Millisecond)
		if svc.Send(ctx, smtp.SMTPRequest{To: to, Body: "x"}) == nil {
			acked++
		}
		cancel()
	}
	received(t, to, acked)
}

// Randomly assembled markup around XSS vectors, run through the same
// sanitizer Send uses. CSS-only vectors are covered by their own test.
func TestChaos_RandomXSSIsAlwaysNeutralised(t *testing.T) {
	t.Parallel()
	r := chaosRand(t)
	fragments := []string{
		"<p>", "</p>", "<div>", "</div>", "<table><tr><td>", "</td></tr></table>", "text ", `"`, "'", ">", "<",
		"<script>", "</script>", "alert(1)", "<img src=x ", "onerror=alert(1)", " onload=alert(1)", ">",
		`<a href="javascript:alert(1)">`, "</a>", "<svg>", "<iframe>", "<object>", "<embed>", "&lt;script&gt;",
		"<scr", "ipt>", "javascript:", "&#106;avascript:", "<!--", "-->", "<style>", "</style>", "<math>", "<mtext>",
		`<meta http-equiv="refresh">`, "<base href=x>", "<form>", "<body onload=x>", "\x00", "\r\n", "🧨",
	}
	for i := range 3000 {
		var sb strings.Builder
		for range 1 + r.IntN(25) {
			sb.WriteString(fragments[r.IntN(len(fragments))])
		}
		var out string
		mustNotPanic(t, "Sanitize", func() { out = utils.Sanitize(sb.String()) })
		low := strings.ToLower(out)
		for _, bad := range []string{"<script", "<iframe", "<object", "<embed", "<svg", "<meta", "<base", "<form"} {
			if strings.Contains(low, bad) {
				t.Fatalf("case %d: %q survived in\n in: %q\nout: %q", i, bad, sb.String(), out)
			}
		}
		if handlerAttr.MatchString(out) {
			t.Fatalf("case %d: event handler survived\n in: %q\nout: %q", i, sb.String(), out)
		}
		if strings.Contains(low, "href=") && strings.Contains(low, "javascript:") {
			t.Fatalf("case %d: javascript: link survived\nout: %q", i, out)
		}
	}
}

func TestChaos_RandomXSSEndToEnd(t *testing.T) {
	t.Parallel()
	ctx := ctxT(t)
	svc := newService(t)
	r := chaosRand(t)
	keys := make([]string, 0, len(xssPayloads))
	for k := range xssPayloads {
		keys = append(keys, k)
	}
	for i := range 15 {
		a, b := xssPayloads[keys[r.IntN(len(keys))]], xssPayloads[keys[r.IntN(len(keys))]]
		to := inbox(t)
		body := fmt.Sprintf("<p>{{.Name}}</p>%s<div>%s</div>", a, b)
		if err := svc.Send(ctx, smtp.SMTPRequest{To: to, Name: b, Body: body}); err != nil {
			t.Fatalf("case %d: %v", i, err)
		}
		assertNoXSS(t, receivedOne(t, to).HTML)
	}
}
