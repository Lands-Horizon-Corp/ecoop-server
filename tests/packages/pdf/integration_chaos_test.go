//go:build integration

package pdf_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pdf"
)

// Integration chaos: several app instances rendering at once, one shutting
// down mid-traffic.

func TestIntegrationChaos_ManyInstancesRenderAtOnce(t *testing.T) {
	t.Parallel()
	instances := []pdf.PDFServices{started(t), started(t), started(t)}
	var wg sync.WaitGroup
	for w := range 12 {
		wg.Go(func() {
			s := instances[w%len(instances)]
			for range 5 {
				if w%2 == 0 {
					render(t, s, invoice())
				} else if p, err := s.GenerateToS3(ctxT(t), invoice()); err != nil {
					t.Error(err)
				} else {
					stored(t, p.StorageKey)
				}
			}
		})
	}
	wg.Wait()
}

func TestIntegrationChaos_OneInstanceStops(t *testing.T) {
	t.Parallel()
	a, b := started(t), started(t)
	var done, refused atomic.Int64
	var wg sync.WaitGroup
	for range 6 {
		wg.Go(func() {
			for range 10 {
				body, err := a.Generate(context.Background(), invoice())
				switch {
				case err == nil:
					_ = body.Close()
					done.Add(1)
				case errors.Is(err, pdf.ErrNotStarted):
					refused.Add(1)
				default:
					t.Errorf("Generate: %v", err)
				}
			}
		})
	}
	_ = a.Stop(ctxT(t))
	wg.Wait()
	render(t, b, invoice())
	t.Logf("%d rendered before the stop, %d refused after", done.Load(), refused.Load())
}
