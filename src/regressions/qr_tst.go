package regressions

import (
	"context"
	"e-coop-server/pkg/services/qr"
	qrService "e-coop-server/pkg/services/qr"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

const qrIterations = 1_000_000

func newSampleData() qr.QRData {
	sample := map[string]any{
		"ticket_id":  "TKT-98421049-X7",
		"event_id":   "EVT-2026-SUMMIT-01",
		"event_name": "Global Tech Developers Conference 2026",
		"status":     "ACTIVE",
		"holder": map[string]any{
			"user_id": "USR-882039", "full_name": "Jane Doe",
			"email": "jane.doe@example.com", "phone": "+1-555-019-2834",
			"is_vip": true, "tier_level": "PLATINUM",
		},
		"venue": map[string]any{
			"name": "Metropolitan Convention Center", "building": "Hall B",
			"gate": "Gate 4A", "seat": "Section 102, Row F, Seat 14",
			"city": "San Francisco", "country": "USA",
			"latitude": 37.7833, "longitude": -122.4167,
		},
		"validity": map[string]any{
			"valid_from": "2026-10-15T08:00:00Z",
			"valid_to":   "2026-10-18T20:00:00Z",
			"timezone":   "America/Los_Angeles",
		},
		"access_scopes": []string{"MAIN_STAGE", "VIP_LOUNGE", "WORKSHOP_ROOM_A", "AFTER_PARTY"},
		"perks": []map[string]any{
			{"code": "MEAL_VOUCHER_1", "description": "Free Lunch Day 1", "redeemed": true},
			{"code": "MEAL_VOUCHER_2", "description": "Free Lunch Day 2", "redeemed": false},
			{"code": "SWAG_BAG", "description": "Developer Kit & T-Shirt", "redeemed": true},
		},
		"security": map[string]any{
			"issuer":            "https://auth.ticketqr.io",
			"device_id":         "DEV-MACBOOK-PRO-M3-9021",
			"ip_history":        []string{"192.168.1.50", "10.0.4.12", "172.16.254.1"},
			"check_in_attempts": 0,
		},
		"metadata": map[string]any{
			"nested_key": "nested_value", "source_channel": "MOBILE_APP_IOS",
			"app_version": "4.12.0",
			"extra_notes": "Attendee requires wheelchair access and dietary preference: Vegan.",
		},
	}
	return qr.QRData{Data: fmt.Sprintf("%v", sample), Type: "ticket"}
}

func report(t *testing.T, label string, n int, elapsed time.Duration, before, after *runtime.MemStats) {
	un := uint64(n)
	t.Logf("[%s] %d round trips in %v (%.0f ops/s)", label, n, elapsed,
		float64(n)/elapsed.Seconds())
	t.Logf("[%s] %d B/op, %d allocs/op, total alloc %.1f MB, GC runs %d, sys %.1f MB",
		label,
		(after.TotalAlloc-before.TotalAlloc)/un,
		(after.Mallocs-before.Mallocs)/un,
		float64(after.TotalAlloc-before.TotalAlloc)/1e6,
		after.NumGC-before.NumGC,
		float64(after.Sys)/1e6)
}

func TestQRRoundTrip(t *testing.T) {
	ctx := context.Background()
	qr := qrService.NewQRService("your-secret-key-asg")

	data := newSampleData()

	enc, err := qr.Encode(ctx, &data)
	if err != nil {
		t.Fatal(err)
	}
	got, err := qr.Decode(ctx, enc)
	if err != nil {
		t.Fatal(err)
	}
	if got != data {
		t.Fatalf("round trip mismatch: got %+v want %+v", got, data)
	}

	// tampered payload must fail authentication
	b := []byte(enc)
	b[len(b)/2] ^= 1
	if _, err := qr.Decode(ctx, string(b)); err == nil {
		t.Fatal("expected error decoding tampered data")
	}
}

func TestQR1MSequential(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 1M benchmark in -short mode")
	}
	ctx := context.Background()
	qr := qrService.NewQRService("your-secret-key-asg")
	data := newSampleData()

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	start := time.Now()

	for range qrIterations {
		enc, err := qr.Encode(ctx, &data)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := qr.Decode(ctx, enc); err != nil {
			t.Fatal(err)
		}
	}

	elapsed := time.Since(start)
	runtime.ReadMemStats(&after)
	report(t, "sequential", qrIterations, elapsed, &before, &after)
}

func TestQR1MParallel(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 1M benchmark in -short mode")
	}
	ctx := context.Background()
	qr := qrService.NewQRService("your-secret-key-asg")
	data := newSampleData()

	workers := runtime.NumCPU()
	per := qrIterations / workers

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	start := time.Now()

	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for range workers {
		wg.Go(func() {
			for range per {
				enc, err := qr.Encode(ctx, &data)
				if err != nil {
					errs <- err
					return
				}
				if _, err := qr.Decode(ctx, enc); err != nil {
					errs <- err
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	elapsed := time.Since(start)
	runtime.ReadMemStats(&after)
	report(t, fmt.Sprintf("parallel x%d", workers), per*workers, elapsed, &before, &after)
}
