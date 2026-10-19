//go:build integration

package otelobs_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	obs "github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability/otelobs"
)

// These tests run the real thing: the app's own Setup, exporting over OTLP/HTTP
// to the collector in docker-compose.yml (profile "telemetry"), which feeds
// Prometheus and Tempo. They then ask Prometheus and Tempo what arrived. They
// skip when the stack is not running, so nothing else depends on it:
//
//	docker compose --profile telemetry up -d otel-collector prometheus tempo grafana

const (
	collectorURL  = "http://127.0.0.1:14318"
	prometheusURL = "http://127.0.0.1:9090"
	grafanaURL    = "http://127.0.0.1:3000"
)

func requireStack(t testing.TB) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker is not installed")
	}
	for _, addr := range []string{"127.0.0.1:14318", "127.0.0.1:9090", "127.0.0.1:3000"} {
		c, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			t.Skipf("telemetry stack is not running (%s): docker compose --profile telemetry up -d otel-collector prometheus tempo grafana", addr)
		}
		_ = c.Close()
	}
}

func getJSON(t testing.TB, u string, into any) {
	t.Helper()
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Get(u)
	if err != nil {
		t.Fatalf("GET %s: %v", u, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s = %d: %s", u, resp.StatusCode, b)
	}
	if err := json.Unmarshal(b, into); err != nil {
		t.Fatalf("GET %s: %v: %s", u, err, b)
	}
}

type promResult struct {
	Data struct {
		Result []struct {
			Metric map[string]string `json:"metric"`
			Value  []any             `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

func promQuery(t testing.TB, q string) promResult {
	t.Helper()
	var r promResult
	getJSON(t, prometheusURL+"/api/v1/query?query="+url.QueryEscape(q), &r)
	return r
}

func repoRootE2E(t testing.TB) string {
	t.Helper()
	dir, _ := os.Getwd()
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		if p := filepath.Dir(dir); p != dir {
			dir = p
		} else {
			t.Fatal("go.mod not found")
		}
	}
}

// The app's own Setup sends operations through the collector into Prometheus,
// under the names and labels the dashboard queries.
func TestE2E_MetricsReachPrometheus(t *testing.T) {
	requireStack(t)
	env := func(k string) (string, bool) {
		return map[string]string{
				"APP_NAME": "ecoop-e2e", "APP_ENV": "test", "APP_VERSION": "e2e",
				"OTEL_EXPORTER_OTLP_ENDPOINT": collectorURL, "OTEL_EXPORTER_OTLP_INSECURE": "true",
				"OTEL_METRIC_EXPORT_INTERVAL": "1000", "OTEL_TRACES_SAMPLER": "always_on",
				"LOG_LEVEL": "error",
			}[k], map[string]bool{
				"APP_NAME": true, "APP_ENV": true, "APP_VERSION": true, "OTEL_EXPORTER_OTLP_ENDPOINT": true,
				"OTEL_EXPORTER_OTLP_INSECURE": true, "OTEL_METRIC_EXPORT_INTERVAL": true, "OTEL_TRACES_SAMPLER": true, "LOG_LEVEL": true,
			}[k]
	}
	svc, _, _, err := otelobs.Setup(env)
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	marker := fmt.Sprintf("e2e_%d", time.Now().UnixNano())
	for range 7 {
		_, op := svc.Begin(context.Background(), obs.ComponentSQL, marker)
		time.Sleep(3 * time.Millisecond)
		op.End(nil)
	}
	_, bad := svc.Begin(context.Background(), obs.ComponentSQL, marker)
	bad.End(fmt.Errorf("boom"))
	if err := svc.Stop(context.Background()); err != nil { // flushes
		t.Fatalf("Stop: %v", err)
	}

	// Prometheus names the job <service.namespace>/<service.name> when a namespace is
	// set, so the service is e-coop/ecoop-e2e here and e-coop/<APP_NAME> in the app.
	q := fmt.Sprintf(`sum by (outcome) (operations_total{job="e-coop/ecoop-e2e",component="sql",operation=%q})`, marker)
	deadline := time.Now().Add(60 * time.Second)
	var got map[string]string
	for time.Now().Before(deadline) {
		got = map[string]string{}
		for _, r := range promQuery(t, q).Data.Result {
			got[r.Metric["outcome"]] = fmt.Sprint(r.Value[1])
		}
		if got["success"] == "7" && got["error"] == "1" {
			break
		}
		time.Sleep(time.Second)
	}
	if got["success"] != "7" || got["error"] != "1" {
		t.Fatalf("Prometheus has %v for operations_total, want success=7 error=1", got)
	}
	// The duration histogram has buckets, so a quantile can be computed.
	// A rate over one short burst can be empty, so the check is that the bucket
	// series exist: that is what makes histogram_quantile possible at all.
	bq := fmt.Sprintf(`count(operation_duration_seconds_bucket{job="e-coop/ecoop-e2e",operation=%q})`, marker)
	if r := promQuery(t, bq).Data.Result; len(r) == 0 || fmt.Sprint(r[0].Value[1]) == "0" {
		t.Error("no operation_duration_seconds_bucket series reached Prometheus")
	}
}

// A trace exported by the app is stored in Tempo and can be read back through
// Grafana: two spans, parent and child, one trace id, the error as a status and
// an exception TYPE, and none of the error's text.
func TestE2E_TracesReachTempoThroughGrafana(t *testing.T) {
	requireStack(t)
	vars := map[string]string{
		"APP_NAME": "ecoop-e2e-traces", "APP_ENV": "test",
		"OTEL_EXPORTER_OTLP_ENDPOINT": collectorURL, "OTEL_EXPORTER_OTLP_INSECURE": "true",
		"OTEL_BSP_SCHEDULE_DELAY": "200", "OTEL_TRACES_SAMPLER": "always_on", "LOG_LEVEL": "error",
	}
	svc, _, _, err := otelobs.Setup(func(k string) (string, bool) { v, ok := vars[k]; return v, ok })
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	marker := fmt.Sprintf("trace_%d", time.Now().UnixNano())
	ctx, parent := svc.Begin(context.Background(), obs.ComponentAPI, marker)
	traceID := ""
	car := obs.MapCarrier{}
	obs.Inject(ctx, svc, car)
	if tp := strings.Split(car["traceparent"], "-"); len(tp) == 4 {
		traceID = tp[1]
	}
	if traceID == "" {
		t.Fatalf("no trace id was produced: %v", car)
	}
	_, child := svc.Begin(ctx, obs.ComponentSQL, "migrate")
	child.End(fmt.Errorf("secret row value 4111111111111111 for ana@example.com"))
	parent.End(nil)
	if err := svc.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	type attr struct {
		Key   string `json:"key"`
		Value struct {
			StringValue string `json:"stringValue"`
		} `json:"value"`
	}
	type sp struct {
		SpanID       string `json:"spanId"`
		ParentSpanID string `json:"parentSpanId"`
		Name         string `json:"name"`
		Status       struct {
			Code    any    `json:"code"`
			Message string `json:"message"`
		} `json:"status"`
		Events []struct {
			Name       string `json:"name"`
			Attributes []attr `json:"attributes"`
		} `json:"events"`
	}
	var body struct {
		Batches []struct {
			ScopeSpans []struct {
				Spans []sp `json:"spans"`
			} `json:"scopeSpans"`
		} `json:"batches"`
	}
	urlTrace := grafanaURL + "/api/datasources/proxy/uid/tempo/api/traces/" + traceID
	var spans []sp
	deadline := time.Now().Add(60 * time.Second)
	var raw string
	for time.Now().Before(deadline) {
		resp, err := (&http.Client{Timeout: 10 * time.Second}).Get(urlTrace)
		if err == nil {
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			raw = string(b)
			if resp.StatusCode == http.StatusOK && json.Unmarshal(b, &body) == nil {
				spans = nil
				for _, b := range body.Batches {
					for _, ss := range b.ScopeSpans {
						spans = append(spans, ss.Spans...)
					}
				}
				if len(spans) >= 2 {
					break
				}
			}
		}
		time.Sleep(time.Second)
	}
	if len(spans) != 2 {
		t.Fatalf("Tempo has %d spans for trace %s, want 2. Last answer: %.300s", len(spans), traceID, raw)
	}
	var par, chi *sp
	for i := range spans {
		if strings.HasPrefix(spans[i].Name, "api ") {
			par = &spans[i]
		} else {
			chi = &spans[i]
		}
	}
	if par == nil || chi == nil {
		t.Fatalf("unexpected span names: %v, %v", spans[0].Name, spans[1].Name)
	}
	// Tempo returns ids base64-encoded in this API, so compare them to each other.
	if chi.ParentSpanID == "" || chi.ParentSpanID != par.SpanID {
		t.Errorf("the child's parent is %q, want the parent span %q", chi.ParentSpanID, par.SpanID)
	}
	if fmt.Sprint(chi.Status.Code) != "STATUS_CODE_ERROR" && fmt.Sprint(chi.Status.Code) != "2" {
		t.Errorf("the failed child's status is %v, want error", chi.Status.Code)
	}
	for _, leak := range []string{"4111111111111111", "ana@example.com", "secret row value"} {
		if strings.Contains(raw, leak) {
			t.Errorf("%q reached Tempo", leak)
		}
	}
	if !strings.Contains(raw, "exception.type") {
		t.Error("the failed span has no exception.type")
	}
}

// Every query in the dashboard is valid PromQL that Prometheus accepts, against
// the real server. Whether it returns data depends on traffic; whether it parses
// does not.
func TestE2E_DashboardQueriesAreValidPromQL(t *testing.T) {
	requireStack(t)
	files, _ := filepath.Glob(filepath.Join(repoRootE2E(t), "telemetry", "*.json"))
	if len(files) == 0 {
		t.Fatal("no dashboards")
	}
	for _, f := range files {
		raw, _ := os.ReadFile(f)
		var d struct {
			Panels []struct {
				Title   string `json:"title"`
				Targets []struct {
					Expr string `json:"expr"`
				} `json:"targets"`
			} `json:"panels"`
		}
		if err := json.Unmarshal(raw, &d); err != nil {
			t.Fatal(err)
		}
		for _, p := range d.Panels {
			for _, tg := range p.Targets {
				e := strings.NewReplacer(`$service`, "e-coop/ecoop-e2e", `$component`, ".*", `$__rate_interval`, "1m").Replace(tg.Expr)
				var r struct {
					Status    string `json:"status"`
					Error     string `json:"error"`
					ErrorType string `json:"errorType"`
				}
				resp, err := (&http.Client{Timeout: 10 * time.Second}).Get(prometheusURL + "/api/v1/query?query=" + url.QueryEscape(e))
				if err != nil {
					t.Fatal(err)
				}
				b, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				_ = json.Unmarshal(b, &r)
				if r.Status != "success" {
					t.Errorf("%s: %q: Prometheus rejects the query: %s %s\n  %s", filepath.Base(f), p.Title, r.ErrorType, r.Error, e)
				}
			}
		}
	}
}

// Grafana loads the provisioned dashboard and both datasources.
func TestE2E_GrafanaProvisionsTheDashboardAndDatasources(t *testing.T) {
	requireStack(t)
	var ds []struct {
		UID  string `json:"uid"`
		Type string `json:"type"`
	}
	var search []struct {
		UID string `json:"uid"`
	}
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		ds, search = nil, nil
		if resp, err := http.Get(grafanaURL + "/api/datasources"); err == nil {
			_ = json.NewDecoder(resp.Body).Decode(&ds)
			resp.Body.Close()
		}
		if resp, err := http.Get(grafanaURL + "/api/search?query=ecoop"); err == nil {
			_ = json.NewDecoder(resp.Body).Decode(&search)
			resp.Body.Close()
		}
		if len(ds) >= 2 && len(search) >= 1 {
			break
		}
		time.Sleep(time.Second)
	}
	have := map[string]string{}
	for _, d := range ds {
		have[d.UID] = d.Type
	}
	if have["prometheus"] != "prometheus" || have["tempo"] != "tempo" {
		t.Errorf("datasources = %v, want prometheus and tempo", have)
	}
	found := false
	for _, s := range search {
		if s.UID == "ecoop-observability" {
			found = true
		}
	}
	if !found {
		t.Errorf("the dashboard ecoop-observability was not provisioned: %v", search)
	}
}
