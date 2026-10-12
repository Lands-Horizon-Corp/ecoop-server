//go:build integration

package pdf_test

import (
	"sync"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pdf"
)

// Integration chaos: several app instances rendering and storing at once.
func TestIntegrationChaos_InstancesRenderAtOnce(t *testing.T) {
	t.Parallel()
	instances := []pdf.PDFServices{started(t), started(t), started(t)}
	var wg sync.WaitGroup
	for w := range 9 {
		wg.Go(func() {
			s := instances[w%len(instances)]
			render(t, s, invoice())
			if p, err := s.GenerateToS3(ctxT(t), invoice()); err != nil {
				t.Error(err)
			} else {
				stored(t, p.StorageKey)
			}
		})
	}
	wg.Wait()
}
