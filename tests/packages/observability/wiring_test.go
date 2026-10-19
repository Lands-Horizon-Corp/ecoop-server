package observability_test

import (
	"context"
	"go/parser"
	"go/token"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/ut"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/api"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/config"
	obs "github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/qr"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/security"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sms"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/smtp"
	"github.com/Lands-Horizon-Corp/ecoop-server/tests/internal/obstest"
)

// No build tag: everything here runs in process. It is the wiring main.go will
// do for the packages that need no infrastructure (api, security, qr, smtp, sms,
// config): ONE observer handed to each, and a request that flows through several
// of them. The packages that need a database, a cache or a broker are each
// tested against the observer in their own observer_test.go, not together here.

// repoRoot is the module root, found from this file so it does not depend on
// the working directory.
func repoRoot(t testing.TB) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate the test file")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

type wired struct {
	o    *obs.Service
	sec  security.ISecurityService
	qr   qr.IQRServices
	mail smtp.ISMTPService
	text sms.ISMSServices
	http *server.Hertz
}

// wire builds the services the way the application will: each takes the same
// observer, and the API gets the middleware.
func wire(t testing.TB) *wired {
	t.Helper()
	ctx := context.Background()
	o := newService(t)
	w := &wired{o: o}
	w.sec = security.NewSecurityService("wiring-secret", 8*1024, 1, 2, 16, 32, security.WithObserver(o))
	w.qr = qr.NewQRServices("wiring-qr-secret", nil, qr.WithObserver(o))
	w.mail = smtp.NewSMTPService("", 0, "", "", "noreply@ecoop.test", "eCoop", false, nil, nil, smtp.WithObserver(o))
	w.text = sms.NewSMSService("", "", "+15005550006", 160, false, nil, nil, sms.WithObserver(o))
	for name, start := range map[string]func(context.Context) error{
		"qr": w.qr.Start, "smtp": w.mail.Start, "sms": w.text.Start,
	} {
		if err := start(ctx); err != nil {
			t.Fatalf("%s Start: %v", name, err)
		}
	}
	t.Cleanup(func() {
		_ = w.qr.Stop(ctx)
		_ = w.mail.Stop(ctx)
		_ = w.text.Stop(ctx)
	})

	h := server.Default(server.WithHostPorts("127.0.0.1:0"))
	h.Use(api.Observe(o))
	h.GET("/members/:id/card", func(ctx context.Context, c *app.RequestContext) {
		if _, err := w.qr.Encode(ctx, &qr.QRData{Data: "member:" + c.Param("id"), Type: "member"}); err != nil {
			c.String(500, "qr")
			return
		}
		c.String(200, "ok")
	})
	h.GET("/login", func(ctx context.Context, c *app.RequestContext) {
		hash, err := w.sec.GeneratePassword(ctx, "right")
		if err != nil {
			c.String(500, "hash")
			return
		}
		ok, err := w.sec.VerifyPassword(ctx, hash, c.Query("pw"))
		if err != nil {
			c.String(500, "verify")
			return
		}
		if !ok {
			c.String(401, "no")
			return
		}
		c.String(200, "ok")
	})
	h.GET("/notify", func(ctx context.Context, c *app.RequestContext) {
		if err := w.mail.Send(ctx, smtp.SMTPRequest{To: "a@b.co", Subject: "s", Body: "hi"}); err != nil {
			c.String(500, "mail")
			return
		}
		if err := w.text.Send(ctx, sms.SMSRequest{To: "+639171234567", Body: "hi"}); err != nil {
			c.String(500, "sms")
			return
		}
		c.String(200, "ok")
	})
	h.GET("/boom", func(_ context.Context, c *app.RequestContext) { c.String(500, "fail") })
	w.http = h
	return w
}

func (w *wired) get(path string) int {
	return ut.PerformRequest(w.http.Engine, "GET", path, nil).Result().StatusCode()
}

func count(o *obs.Service, component, op, outcome string) float64 {
	return obstest.Count(o, component, op, outcome)
}

// One observer sees a request and everything it touched, each under its own
// component, with outcomes that mean the same thing in every package.
func TestWiring_OneObserverSeesEveryPackage(t *testing.T) {
	t.Parallel()
	w := wire(t)
	for range 3 {
		if got := w.get("/members/42/card"); got != 200 {
			t.Fatalf("/members/42/card = %d", got)
		}
	}
	if got := w.get("/login?pw=right"); got != 200 {
		t.Fatalf("right password = %d", got)
	}
	for range 4 {
		if got := w.get("/login?pw=wrong"); got != 401 {
			t.Fatalf("wrong password = %d", got)
		}
	}
	if got := w.get("/notify"); got != 200 {
		t.Fatalf("/notify = %d", got)
	}
	if got := w.get("/boom"); got != 500 {
		t.Fatalf("/boom = %d", got)
	}

	want := []struct {
		component, op, outcome string
		n                      float64
	}{
		{"api", "/members/:id/card", "success", 3},
		{"api", "/login", "success", 5}, // a 401 is the caller's answer, not a server fault
		{"api", "/notify", "success", 1},
		{"api", "/boom", "error", 1},
		{"qr", "encode", "success", 3},
		{"security", "hash_password", "success", 5},
		{"security", "verify_password", "success", 1},
		{"security", "verify_password", "skipped", 4}, // wrong passwords
		{"smtp", "send", "success", 1},
		{"sms", "send", "success", 1},
	}
	for _, c := range want {
		if got := count(w.o, c.component, c.op, c.outcome); got != c.n {
			t.Errorf("%s %s %s = %v, want %v", c.component, c.op, c.outcome, got, c.n)
		}
	}
	if got := count(w.o, "security", "verify_password", "error"); got != 0 {
		t.Errorf("wrong passwords raised the security error count to %v", got)
	}
}

// A thousand different members are one API series and one QR series, and none
// of the member ids reaches the registry.
func TestWiring_RequestsAreBoundedWhateverTheTraffic(t *testing.T) {
	t.Parallel()
	w := wire(t)
	for i := range 200 {
		w.get("/members/" + strconv.Itoa(100000+i) + "/card")
	}
	if got := count(w.o, "api", "/members/:id/card", "success"); got != 200 {
		t.Fatalf("api = %v, want 200", got)
	}
	if got := count(w.o, "qr", "encode", "success"); got != 200 {
		t.Fatalf("qr = %v, want 200", got)
	}
	for _, sm := range w.o.Snapshot() {
		for k, v := range sm.Attrs {
			if strings.Contains(k+v, "10000") || strings.Contains(v, "member:") {
				t.Fatalf("a member id reached the registry: %s %s=%q", sm.Name, k, v)
			}
		}
	}
	if n := len(w.o.Snapshot()); n > 40 {
		t.Fatalf("%d series for one route, want a small constant", n)
	}
}

// Many requests at once, through every package, are all counted and none is
// left in flight.
func TestWiring_ConcurrentTrafficIsExact(t *testing.T) {
	t.Parallel()
	w := wire(t)
	const workers, each = 12, 25
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for range each {
				w.get("/members/7/card")
				w.get("/login?pw=wrong")
			}
		})
	}
	wg.Wait()
	if got := count(w.o, "api", "/members/:id/card", "success"); got != workers*each {
		t.Fatalf("api card = %v, want %d", got, workers*each)
	}
	if got := count(w.o, "security", "verify_password", "skipped"); got != workers*each {
		t.Fatalf("wrong passwords = %v, want %d", got, workers*each)
	}
	assertNothingInFlight(t, w.o)
}

func assertNothingInFlight(t testing.TB, o *obs.Service) {
	t.Helper()
	obstest.NothingInFlight(t, o)
}

// The config package is observed through the same observer, and reading it
// writes nothing.
func TestWiring_ConfigSharesTheObserver(t *testing.T) {
	o := newService(t)
	t.Chdir(t.TempDir())
	t.Setenv("WIRING_APP_NAME", "ecoop")
	type appConfig struct {
		Name string `env:"WIRING_APP_NAME" envDefault:"ecoop"`
	}
	svc, err := config.NewConfigService[appConfig](config.WithObserver(o))
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	for range 50 {
		if _, err := svc.Get(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if got := count(o, "config", "load", "success"); got != 1 {
		t.Fatalf("config load = %v, want 1", got)
	}
	if got := len(o.Snapshot()); got > 4 {
		t.Fatalf("%d series after a load and 50 reads, want only the load", got)
	}
}

// Every package works with no observer at all, and the middleware with a nil
// one: a deployment that forgets to wire one must not crash.
func TestWiring_NoObserverAnywhere(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	sec := security.NewSecurityService("s", 8*1024, 1, 2, 16, 32)
	q := qr.NewQRServices("q", nil)
	mail := smtp.NewSMTPService("", 0, "", "", "noreply@ecoop.test", "eCoop", false, nil, nil)
	text := sms.NewSMSService("", "", "+15005550006", 160, false, nil, nil)
	for name, start := range map[string]func(context.Context) error{"qr": q.Start, "smtp": mail.Start, "sms": text.Start} {
		if err := start(ctx); err != nil {
			t.Fatalf("%s Start: %v", name, err)
		}
	}
	mustNotPanic(t, "unwired services", func() {
		hash, err := sec.GeneratePassword(ctx, "x")
		if err != nil {
			t.Error(err)
		}
		if ok, err := sec.VerifyPassword(ctx, hash, "x"); err != nil || !ok {
			t.Errorf("VerifyPassword = %v, %v", ok, err)
		}
		if _, err := q.Encode(ctx, &qr.QRData{Data: "d", Type: "t"}); err != nil {
			t.Error(err)
		}
		if err := mail.Send(ctx, smtp.SMTPRequest{To: "a@b.co", Body: "x"}); err != nil {
			t.Error(err)
		}
		if err := text.Send(ctx, sms.SMSRequest{To: "+639171234567", Body: "x"}); err != nil {
			t.Error(err)
		}
	})
	h := server.Default(server.WithHostPorts("127.0.0.1:0"))
	h.Use(api.Observe(nil))
	h.GET("/ok", func(_ context.Context, c *app.RequestContext) { c.String(200, "ok") })
	if got := ut.PerformRequest(h.Engine, "GET", "/ok", nil).Result().StatusCode(); got != 200 {
		t.Fatalf("/ok = %d with a nil observer", got)
	}
}

// The registry, written out as Prometheus text, is valid: the names and label
// names are legal, no metric changes type, every series says which component
// it is from, and the in-flight gauge never carries an outcome. This is what a
// Prometheus or OTLP exporter will have to write.
func TestWiring_ExpositionIsPrometheusValid(t *testing.T) {
	t.Parallel()
	w := wire(t)
	for _, p := range []string{"/members/1/card", "/login?pw=right", "/login?pw=wrong", "/notify", "/boom", "/nowhere"} {
		w.get(p)
	}
	text := expose(w.o)
	if strings.TrimSpace(text) == "" {
		t.Fatal("nothing to expose")
	}
	line := regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*(\{([a-zA-Z_][a-zA-Z0-9_]*="([^"\\\n]|\\.)*",?)*\})? -?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)
	for l := range strings.SplitSeq(strings.TrimSpace(text), "\n") {
		if !line.MatchString(l) {
			t.Errorf("not valid Prometheus text: %q", l)
		}
	}
	kinds := map[string]obs.Kind{}
	for _, sm := range w.o.Snapshot() {
		if prev, ok := kinds[sm.Name]; ok && prev != sm.Kind {
			t.Errorf("%s is both a %s and a %s", sm.Name, prev, sm.Kind)
		}
		kinds[sm.Name] = sm.Kind
		if sm.Attrs[obs.KeyComponent] == "" {
			t.Errorf("%s has a series with no component: %v", sm.Name, sm.Attrs)
		}
		if sm.Name == obs.MetricInflight {
			if _, ok := sm.Attrs[obs.KeyOutcome]; ok {
				t.Errorf("an in-flight series carries an outcome: %v", sm.Attrs)
			}
		}
		for k := range sm.Attrs {
			if !regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`).MatchString(k) || strings.HasPrefix(k, "__") {
				t.Errorf("illegal label name %q on %s", k, sm.Name)
			}
		}
	}
	if kinds[obs.MetricOperations] != obs.KindCounter || kinds[obs.MetricOperationDuration] != obs.KindHistogram || kinds[obs.MetricInflight] != obs.KindGauge {
		t.Errorf("the shared instruments have the wrong types: %v", kinds)
	}
}

// expose writes the registry the way a Prometheus endpoint would.
func expose(o *obs.Service) string {
	esc := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	labels := func(a map[string]string) string {
		keys := make([]string, 0, len(a))
		for k := range a {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			parts = append(parts, k+`="`+esc.Replace(a[k])+`"`)
		}
		if len(parts) == 0 {
			return ""
		}
		return "{" + strings.Join(parts, ",") + "}"
	}
	num := func(v float64) string { return strconv.FormatFloat(v, 'g', -1, 64) }
	var b strings.Builder
	for _, sm := range o.Snapshot() {
		switch sm.Kind {
		case obs.KindHistogram:
			b.WriteString(sm.Name + "_count" + labels(sm.Attrs) + " " + strconv.FormatUint(sm.Count, 10) + "\n")
			b.WriteString(sm.Name + "_sum" + labels(sm.Attrs) + " " + num(sm.Sum) + "\n")
		default:
			b.WriteString(sm.Name + labels(sm.Attrs) + " " + num(sm.Value) + "\n")
		}
	}
	return b.String()
}

// The design rule: a package never imports a telemetry library. Only the
// observability package may, because the exporter will live there.
func TestWiring_NoPackageImportsATelemetryLibrary(t *testing.T) {
	t.Parallel()
	root := filepath.Join(repoRoot(t), "internal", "pkg")
	banned := []string{"go.opentelemetry.io/", "github.com/prometheus/", "go.uber.org/zap"}
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && d.Name() == "observability" {
			return filepath.SkipDir
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		f, perr := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if perr != nil {
			return perr
		}
		for _, imp := range f.Imports {
			p, _ := strconv.Unquote(imp.Path.Value)
			for _, b := range banned {
				if strings.HasPrefix(p, b) {
					rel, _ := filepath.Rel(root, path)
					t.Errorf("%s imports %s: packages observe through internal/pkg/observability only", rel, p)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// Nothing is forgotten: every package under internal/pkg uses the observability
// package, and every Component has a package that actually uses it. A new
// package, or a new Component nobody uses, fails here.
func TestWiring_EveryPackageAndComponentIsCovered(t *testing.T) {
	t.Parallel()
	root := filepath.Join(repoRoot(t), "internal", "pkg")
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	var all strings.Builder
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		files, _ := filepath.Glob(filepath.Join(root, e.Name(), "*.go"))
		used := false
		for _, f := range files {
			if strings.HasSuffix(f, "_test.go") {
				continue
			}
			b, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			if e.Name() != "observability" {
				all.Write(b)
			}
			if strings.Contains(string(b), "observability.") {
				used = true
			}
		}
		if e.Name() != "observability" && !used {
			t.Errorf("internal/pkg/%s does not use the observability package", e.Name())
		}
	}
	types, err := os.ReadFile(filepath.Join(root, "observability", "observability.types.go"))
	if err != nil {
		t.Fatal(err)
	}
	comps := regexp.MustCompile(`(?m)^\s*Component(\w+)\s+Component\s*=\s*"(\w+)"`).FindAllStringSubmatch(string(types), -1)
	if len(comps) < 19 {
		t.Fatalf("found only %d components in the contract", len(comps))
	}
	for _, c := range comps {
		if !regexp.MustCompile(`observability\.Component` + c[1] + `\b`).MatchString(all.String()) {
			t.Errorf("Component%s (%q) is declared but no package uses it", c[1], c[2])
		}
	}
}

// The component list the tests use is the contract's, so the two cannot drift.
func TestWiring_TestComponentListMatchesTheContract(t *testing.T) {
	t.Parallel()
	types, err := os.ReadFile(filepath.Join(repoRoot(t), "internal", "pkg", "observability", "observability.types.go"))
	if err != nil {
		t.Fatal(err)
	}
	declared := map[string]bool{}
	for _, m := range regexp.MustCompile(`(?m)^\s*Component\w+\s+Component\s*=\s*"(\w+)"`).FindAllStringSubmatch(string(types), -1) {
		declared[m[1]] = true
	}
	listed := map[string]bool{}
	for _, c := range allComponents {
		listed[string(c)] = true
		if !declared[string(c)] {
			t.Errorf("%q is in the test list but not in the contract", c)
		}
	}
	for c := range declared {
		if !listed[c] {
			t.Errorf("%q is in the contract but not in the test list", c)
		}
	}
}

var _ = slog.New
