package api_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/api"
)

// Stop straight after Run returns must really stop the server. Run used to
// return before the event loop existed, so Shutdown found nothing to shut down,
// and the serve loop later died on a listener that had been closed twice.
func TestStartStop_StopRightAfterRunLeavesNothingServing(t *testing.T) {
	for i := range 300 {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		svc := api.NewAPIService(0, nil, nil, api.WithListener(l)).(*api.APIService)
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("round %d: Start: %v", i, err)
		}
		if err := svc.Run(context.Background()); err != nil {
			t.Fatalf("round %d: Run: %v", i, err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = svc.Stop(ctx)
		cancel()
		if err != nil {
			t.Fatalf("round %d: Stop: %v", i, err)
		}
		c, err := net.DialTimeout("tcp", l.Addr().String(), 200*time.Millisecond)
		if err == nil {
			c.Close()
			t.Fatalf("round %d: the listener still accepts after Stop", i)
		}
	}
}
