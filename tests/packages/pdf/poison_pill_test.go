//go:build integration

package pdf_test

import (
	"strings"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pdf"
)

// Poison pill: hostile templates and data render safely or are refused,
// never crash the service or the engine.

// Images from private addresses (or anywhere not allowed) are skipped; the
// PDF still renders.
func TestPoison_RemoteImagesAreNotFetched(t *testing.T) {
	t.Parallel()
	s := started(t)
	render(t, s, pdf.RenderOptions{Template: `<p>Statement</p>
<img src="http://169.254.169.254/latest/meta-data/">
<img src="http://127.0.0.1:6767/health">
<img src="file:///etc/passwd">`})
}

// Markup and template syntax in data are data, not code.
func TestPoison_HostileData(t *testing.T) {
	t.Parallel()
	s := started(t)
	render(t, s, pdf.RenderOptions{
		Template: `<p>{{ memo }}</p><p>{{ name }}</p>`,
		Data: map[string]string{
			"memo": `<script>alert(1)</script><img src=x onerror=alert(1)>`,
			"name": `{{ 7*7 }} {% for i in range(100000000) %}x{% endfor %}`, // not evaluated
		},
	})
}

// Large documents still render: a 2,000-line statement.
func TestPoison_LargeDocument(t *testing.T) {
	t.Parallel()
	s := started(t)
	lines := make([]map[string]any, 2000)
	for i := range lines {
		lines[i] = map[string]any{"item": "Deposit " + strings.Repeat("·", i%40), "amount": i}
	}
	render(t, s, pdf.RenderOptions{
		Template: `<table>{% for l in lines %}<tr><td>{{ l.item }}</td><td>{{ l.amount }}</td></tr>{% endfor %}</table>`,
		Data:     map[string]any{"lines": lines},
	})
}

// A template that misbehaves is refused by the engine, and the service keeps
// working.
func TestPoison_EngineErrorsDoNotBreakTheService(t *testing.T) {
	t.Parallel()
	s := started(t)
	for _, tpl := range []string{"{{ undefined.attr.chain }}", "{% raw %}unclosed", strings.Repeat("{{", 5000)} {
		var err error
		mustNotPanic(t, "render", func() { _, err = s.Generate(ctxT(t), pdf.RenderOptions{Template: tpl}) })
		_ = err // refused or rendered: either is fine, as long as nothing breaks
	}
	render(t, s, invoice())
}
