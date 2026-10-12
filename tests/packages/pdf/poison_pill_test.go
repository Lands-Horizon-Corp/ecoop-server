//go:build integration

package pdf_test

import (
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pdf"
)

// Poison pill: hostile data stays data, and private image URLs are not
// fetched; the PDF still renders.
func TestPoison_HostileInput(t *testing.T) {
	t.Parallel()
	s := started(t)
	render(t, s, pdf.RenderOptions{
		Template: `<p>{{ memo }}</p><img src="http://169.254.169.254/latest/meta-data/"><img src="http://127.0.0.1:6767/health">`,
		Data:     map[string]string{"memo": `<script>alert(1)</script> {{ 7*7 }} {% for i in range(100000000) %}x{% endfor %}`},
	})
}
