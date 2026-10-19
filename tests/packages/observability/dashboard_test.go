package observability_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	obs "github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
)

// No build tag. These tests read telemetry/*.json and check every query in it
// against what the code can actually produce, so a renamed metric, a retired
// operation or a typo in a label fails here and not on a blank panel in
// production.

type dashboard struct {
	UID        string `json:"uid"`
	Title      string `json:"title"`
	Templating struct {
		List []struct {
			Name       string `json:"name"`
			Definition string `json:"definition"`
		} `json:"list"`
	} `json:"templating"`
	Panels []panel `json:"panels"`
}

type panel struct {
	ID      int    `json:"id"`
	Type    string `json:"type"`
	Title   string `json:"title"`
	Targets []struct {
		Expr         string `json:"expr"`
		LegendFormat string `json:"legendFormat"`
	} `json:"targets"`
	Datasource *struct {
		Type string `json:"type"`
		UID  string `json:"uid"`
	} `json:"datasource"`
	Panels []panel `json:"panels"`
}

func loadDashboards(t testing.TB) map[string]dashboard {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(repoRoot(t), "telemetry", "*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no dashboards in telemetry/: %v", err)
	}
	out := map[string]dashboard{}
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		var d dashboard
		if err := json.Unmarshal(raw, &d); err != nil {
			t.Fatalf("%s is not valid JSON: %v", filepath.Base(f), err)
		}
		out[filepath.Base(f)] = d
	}
	return out
}

func allPanels(ps []panel) []panel {
	var out []panel
	for _, p := range ps {
		out = append(out, p)
		out = append(out, allPanels(p.Panels)...)
	}
	return out
}

// every metric the code can emit, as a Prometheus series would be named: a
// counter as it is, a gauge as it is, a histogram as _count and _sum.
func knownSeries() map[string]bool {
	known := map[string]bool{}
	for _, n := range []string{
		obs.MetricOperations, obs.MetricInflight,
		obs.MetricCQRSBatches, obs.MetricCQRSRecords, obs.MetricCQRSApplied, obs.MetricCQRSDuplicates,
		obs.MetricCQRSTombstones, obs.MetricCQRSSkipped, obs.MetricCQRSDeadLettered, obs.MetricCQRSRetries,
		obs.MetricCQRSStale, obs.MetricCQRSCommitFails, obs.MetricCQRSHooks, obs.MetricCQRSHooksDropped,
		obs.MetricCQRSRepaired, obs.MetricCQRSAwaiting,
	} {
		known[n] = true
	}
	for _, h := range []string{obs.MetricOperationDuration, obs.MetricCQRSBatchDuration} {
		known[h+"_count"], known[h+"_sum"], known[h+"_bucket"] = true, true, true
	}
	return known
}

// knownLabels are the labels a series can carry: the bounded keys the registry
// keeps, plus overflow. "job" is added by Prometheus itself.
func knownLabels() map[string]bool {
	return map[string]bool{
		obs.KeyComponent: true, obs.KeyOperation: true, obs.KeyOutcome: true, obs.KeyTopic: true,
		obs.KeyConsumerGroup: true, obs.KeyTable: true, obs.KeyCacheFamily: true, obs.KeyOverflow: true,
		// ecoop.* are span attributes, not labels; none here.
		"job": true, "instance": true, "le": true,
	}
}

var (
	reMetric    = regexp.MustCompile(`\b([a-z][a-z0-9_]*)\s*\{`)
	reSelectors = regexp.MustCompile(`([a-z][a-z0-9_]*)\s*\{([^}]*)\}`)
	reLabel     = regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*)\s*(=~|!~|!=|=)\s*"((?:[^"\\]|\\.)*)"`)
	reBy        = regexp.MustCompile(`\b(?:by|without)\s*\(([^)]*)\)`)
	reLegend    = regexp.MustCompile(`\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}`)
)

// Every dashboard is well formed: a unique uid, a title, a Prometheus
// datasource, unique panel ids and a query on every panel that draws data.
func TestDashboard_IsWellFormed(t *testing.T) {
	t.Parallel()
	uids := map[string]string{}
	for file, d := range loadDashboards(t) {
		if d.UID == "" || d.Title == "" {
			t.Errorf("%s: uid and title are required", file)
		}
		if other, dup := uids[d.UID]; dup {
			t.Errorf("%s and %s share the uid %q: Grafana would load only one", file, other, d.UID)
		}
		uids[d.UID] = file
		ids := map[int]bool{}
		for _, p := range allPanels(d.Panels) {
			if p.ID == 0 || ids[p.ID] {
				t.Errorf("%s: panel %q has a missing or repeated id %d", file, p.Title, p.ID)
			}
			ids[p.ID] = true
			if p.Type == "row" {
				continue
			}
			if p.Title == "" {
				t.Errorf("%s: panel %d has no title", file, p.ID)
			}
			if len(p.Targets) == 0 {
				t.Errorf("%s: panel %q has no query", file, p.Title)
			}
			if p.Datasource == nil || p.Datasource.Type != "prometheus" || p.Datasource.UID == "" {
				t.Errorf("%s: panel %q has no Prometheus datasource", file, p.Title)
			}
			for _, tg := range p.Targets {
				if strings.TrimSpace(tg.Expr) == "" {
					t.Errorf("%s: panel %q has an empty query", file, p.Title)
				}
				if strings.Count(tg.Expr, "(") != strings.Count(tg.Expr, ")") ||
					strings.Count(tg.Expr, "{") != strings.Count(tg.Expr, "}") ||
					strings.Count(tg.Expr, "[") != strings.Count(tg.Expr, "]") {
					t.Errorf("%s: panel %q has unbalanced brackets in %q", file, p.Title, tg.Expr)
				}
			}
		}
	}
}

// Every metric a query reads is one the code emits, and the type matches how
// it is used: rate() and increase() only on counters, _sum and _count only on
// histograms.
func TestDashboard_QueriesReadRealMetrics(t *testing.T) {
	t.Parallel()
	known := knownSeries()
	counters := map[string]bool{obs.MetricOperations: true}
	for _, n := range []string{
		obs.MetricCQRSBatches, obs.MetricCQRSRecords, obs.MetricCQRSApplied, obs.MetricCQRSDuplicates,
		obs.MetricCQRSTombstones, obs.MetricCQRSSkipped, obs.MetricCQRSDeadLettered, obs.MetricCQRSRetries,
		obs.MetricCQRSStale, obs.MetricCQRSCommitFails, obs.MetricCQRSHooks, obs.MetricCQRSHooksDropped, obs.MetricCQRSRepaired,
	} {
		counters[n] = true
	}
	for _, h := range []string{obs.MetricOperationDuration, obs.MetricCQRSBatchDuration} {
		counters[h+"_count"], counters[h+"_sum"], counters[h+"_bucket"] = true, true, true // monotonic, so rate() is right
	}
	rateOf := regexp.MustCompile(`\b(?:rate|increase|irate)\(\s*([a-z][a-z0-9_]*)\s*\{`)
	for file, d := range loadDashboards(t) {
		for _, p := range allPanels(d.Panels) {
			for _, tg := range p.Targets {
				for _, m := range reMetric.FindAllStringSubmatch(tg.Expr, -1) {
					if !known[m[1]] {
						t.Errorf("%s: panel %q reads %q, which the code does not emit", file, p.Title, m[1])
					}
				}
				for _, m := range rateOf.FindAllStringSubmatch(tg.Expr, -1) {
					if !counters[m[1]] {
						t.Errorf("%s: panel %q takes a rate of %q, which is not a counter", file, p.Title, m[1])
					}
				}
			}
		}
	}
}

// Every label a query filters or groups by exists on the series it reads, and
// every value it filters on is one the code can produce: a real component, a
// real outcome, an operation name some package actually begins.
func TestDashboard_LabelsAndValuesExist(t *testing.T) {
	t.Parallel()
	labels := knownLabels()
	components := map[string]bool{}
	types := mustRead(t, filepath.Join(repoRoot(t), "internal", "pkg", "observability", "observability.types.go"))
	for _, m := range regexp.MustCompile(`(?m)^\s*Component\w+\s+Component\s*=\s*"(\w+)"`).FindAllStringSubmatch(types, -1) {
		components[m[1]] = true
	}
	outcomes := map[string]bool{"success": true, "error": true, "retry": true, "dlq": true, "skipped": true}
	ops := operationNames(t)
	for file, d := range loadDashboards(t) {
		for _, p := range allPanels(d.Panels) {
			for _, tg := range p.Targets {
				for _, sel := range reSelectors.FindAllStringSubmatch(tg.Expr, -1) {
					for _, l := range reLabel.FindAllStringSubmatch(sel[2], -1) {
						name, op, val := l[1], l[2], l[3]
						if !labels[name] {
							t.Errorf("%s: panel %q filters on the label %q, which no series carries", file, p.Title, name)
							continue
						}
						if strings.HasPrefix(val, "$") {
							continue // a dashboard variable
						}
						for _, v := range splitAlternatives(val, op) {
							switch name {
							case obs.KeyComponent:
								if !components[v] {
									t.Errorf("%s: panel %q filters on component %q, which is not a Component", file, p.Title, v)
								}
							case obs.KeyOutcome:
								if !outcomes[v] {
									t.Errorf("%s: panel %q filters on outcome %q, which is not an Outcome", file, p.Title, v)
								}
							case obs.KeyOperation:
								if !ops[v] && !strings.HasPrefix(v, "/") && v != "unmatched" {
									t.Errorf("%s: panel %q filters on operation %q, which no package begins", file, p.Title, v)
								}
							}
						}
					}
				}
				for _, g := range reBy.FindAllStringSubmatch(tg.Expr, -1) {
					for name := range strings.SplitSeq(g[1], ",") {
						if name = strings.TrimSpace(name); name != "" && !labels[name] {
							t.Errorf("%s: panel %q groups by %q, which no series carries", file, p.Title, name)
						}
					}
				}
				for _, l := range reLegend.FindAllStringSubmatch(tg.LegendFormat, -1) {
					if !labels[l[1]] {
						t.Errorf("%s: panel %q legend uses {{%s}}, which no series carries", file, p.Title, l[1])
					}
				}
			}
		}
	}
}

func splitAlternatives(val, op string) []string {
	if op != "=~" && op != "!~" {
		return []string{val}
	}
	var out []string
	for v := range strings.SplitSeq(val, "|") {
		if v != "" && v != ".*" {
			out = append(out, v)
		}
	}
	return out
}

func mustRead(t testing.TB, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// operationNames is every operation name a package passes to begin or Begin, read from the source.
func operationNames(t testing.TB) map[string]bool {
	t.Helper()
	root := filepath.Join(repoRoot(t), "internal", "pkg")
	re := regexp.MustCompile(`(?:begin|Begin)\(\s*[a-zA-Z_.]+\s*,\s*(?:observability\.Component\w+\s*,\s*)?"([a-z_]+)"`)
	out := map[string]bool{}
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, m := range re.FindAllStringSubmatch(string(b), -1) {
			out[m[1]] = true
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) < 40 {
		t.Fatalf("found only %d operation names in the source; the pattern no longer matches how packages begin operations", len(out))
	}
	return out
}

// A quantile is only right if it is taken over buckets, summed by le, and over a
// rate: anything else gives a number that looks like a latency and is not.
func TestDashboard_QuantilesAreWellFormed(t *testing.T) {
	t.Parallel()
	quantile := regexp.MustCompile(`histogram_quantile\(\s*([0-9.]+)\s*,\s*(.*)\)\s*$`)
	found := 0
	for file, d := range loadDashboards(t) {
		for _, p := range allPanels(d.Panels) {
			for _, tg := range p.Targets {
				if !strings.Contains(tg.Expr, "histogram_quantile") {
					continue
				}
				found++
				m := quantile.FindStringSubmatch(tg.Expr)
				if m == nil {
					t.Errorf("%s: %q: cannot parse the quantile in %q", file, p.Title, tg.Expr)
					continue
				}
				q, _ := strconv.ParseFloat(m[1], 64)
				if q <= 0 || q >= 1 {
					t.Errorf("%s: %q: quantile %v is not between 0 and 1", file, p.Title, q)
				}
				body := m[2]
				if !strings.Contains(body, "_bucket{") {
					t.Errorf("%s: %q: a quantile over something that is not a _bucket series: %s", file, p.Title, tg.Expr)
				}
				if !regexp.MustCompile(`by\s*\(\s*le\b`).MatchString(body) && !regexp.MustCompile(`by\s*\([^)]*,\s*le\b`).MatchString(body) {
					t.Errorf("%s: %q: the buckets are not summed by le, so the quantile is wrong: %s", file, p.Title, tg.Expr)
				}
				if !strings.Contains(body, "rate(") {
					t.Errorf("%s: %q: a quantile of cumulative buckets, not of a rate: %s", file, p.Title, tg.Expr)
				}
			}
		}
	}
	if found == 0 {
		t.Fatal("the dashboard has no quantile panel, so latency tails are invisible")
	}
}

// Variables the dashboard declares are the ones its queries use, and each is
// defined from a series that exists.
func TestDashboard_VariablesAreDeclaredAndReal(t *testing.T) {
	t.Parallel()
	known := knownSeries()
	for file, d := range loadDashboards(t) {
		declared := map[string]bool{}
		for _, v := range d.Templating.List {
			declared[v.Name] = true
			for _, m := range reMetric.FindAllStringSubmatch(v.Definition, -1) {
				if !known[m[1]] {
					t.Errorf("%s: variable %q is defined from %q, which the code does not emit", file, v.Name, m[1])
				}
			}
		}
		used := map[string]bool{}
		use := regexp.MustCompile(`\$([a-zA-Z_][a-zA-Z0-9_]*)`)
		for _, p := range allPanels(d.Panels) {
			for _, tg := range p.Targets {
				for _, m := range use.FindAllStringSubmatch(tg.Expr, -1) {
					used[m[1]] = true
				}
			}
		}
		builtin := map[string]bool{"__rate_interval": true, "__interval": true, "__range": true, "__all": true}
		var missing []string
		for name := range used {
			if !declared[name] && !builtin[name] {
				missing = append(missing, name)
			}
		}
		sort.Strings(missing)
		if len(missing) > 0 {
			t.Errorf("%s: queries use undeclared variables %v", file, missing)
		}
	}
}

// The dashboard must not hide a counted fault: the error-rate panels read
// outcome="error" only, and divide by the total including skipped, so an
// expected answer never raises or lowers the rate.
func TestDashboard_ErrorRateMeansFaultsOnly(t *testing.T) {
	t.Parallel()
	found := false
	for file, d := range loadDashboards(t) {
		for _, p := range allPanels(d.Panels) {
			if !strings.Contains(strings.ToLower(p.Title), "error rate") {
				continue
			}
			found = true
			for _, tg := range p.Targets {
				if !strings.Contains(tg.Expr, `outcome="error"`) {
					t.Errorf("%s: %q does not count outcome=\"error\": %s", file, p.Title, tg.Expr)
				}
				if strings.Contains(tg.Expr, "skipped") || strings.Contains(tg.Expr, "retry") {
					t.Errorf("%s: %q mixes expected answers into the error rate: %s", file, p.Title, tg.Expr)
				}
			}
		}
	}
	if !found {
		t.Fatal("no error-rate panel found")
	}
}

// Every component has somewhere to be seen. The dashboard has panels that name
// the important ones, and panels that group operations_total by component so
// the rest (and any component added later) appear on their own. The first
// panels of that kind must exist, and must not be filtered down to a few
// components, or a package could fail unnoticed.
func TestDashboard_EveryComponentIsVisible(t *testing.T) {
	t.Parallel()
	for file, d := range loadDashboards(t) {
		grouped := 0
		for _, p := range allPanels(d.Panels) {
			for _, tg := range p.Targets {
				// Only the panels that group by component alone are the
				// "everything" panels. One that also groups by operation is a
				// pipeline panel, scoped on purpose.
				if !strings.Contains(tg.Expr, "by (component)") || !strings.Contains(tg.Expr, "operations_total") {
					continue
				}
				grouped++
				if regexp.MustCompile(`component=~?"[a-z]`).MatchString(tg.Expr) {
					t.Errorf("%s: %q groups by component but is filtered to some components: %s", file, p.Title, tg.Expr)
				}
			}
		}
		if grouped < 2 {
			t.Errorf("%s: %d panels group operations_total by component, want at least 2 (rate and errors)", file, grouped)
		}
	}
}
