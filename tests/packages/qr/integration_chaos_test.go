package qr_test

import (
	"context"
	"encoding/base64"
	"fmt"
	"math/rand/v2"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/qr"
)

// Integration chaos: several nodes issuing and scanning QR codes across
// restarts, secret rotation and lossy transport, with faults injected at random.

type fault int

const (
	faultNone fault = iota
	faultFlip
	faultTruncate
	faultRogueNode
)

func TestIntegrationChaos_ClusterWithRandomFaults(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	nodes := []qr.QRServices{newStarted(t), newStarted(t), newStarted(t)}
	rogue := newStartedWithSecret(t, "rogue")
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)

	var wg sync.WaitGroup
	for i := range 90 {
		wg.Go(func() {
			r := rand.New(rand.NewPCG(seed, uint64(i)))
			issuer, scanner := nodes[r.IntN(len(nodes))], nodes[r.IntN(len(nodes))]
			f := fault(r.IntN(4))
			if f == faultRogueNode {
				scanner = rogue
			}

			in := qr.QRData{Data: fmt.Sprintf("txn-%d", i), Type: "payment"}
			enc, err := issuer.Encode(ctx, &in)
			if err != nil {
				t.Errorf("run %d: Encode: %v", i, err)
				return
			}
			switch f {
			case faultFlip:
				raw, _ := base64.StdEncoding.DecodeString(enc)
				raw[r.IntN(len(raw))] ^= byte(1 + r.IntN(255))
				enc = base64.StdEncoding.EncodeToString(raw)
			case faultTruncate:
				enc = enc[:r.IntN(len(enc))]
			}

			out, err := scanner.Decode(ctx, enc)
			switch {
			case f == faultNone && (err != nil || out != in):
				t.Errorf("run %d: clean scan failed: %+v, %v", i, out, err)
			case f != faultNone && err == nil:
				t.Errorf("run %d: fault %d undetected, got %+v", i, f, out)
			}
		})
	}
	wg.Wait()
}

func TestIntegrationChaos_SecretRotation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	oldNode := newStartedWithSecret(t, "qr-v1")
	newNode := newStartedWithSecret(t, "qr-v2")
	printed := mustEncode(t, oldNode, &qr.QRData{Data: "member:1"})

	if _, err := newNode.Decode(ctx, printed); err == nil {
		t.Fatal("QR issued under the old secret accepted after rotation")
	}
	if out, err := oldNode.Decode(ctx, printed); err != nil || out.Data != "member:1" {
		t.Fatalf("old node lost its own QR: %+v, %v", out, err)
	}
}

func TestIntegrationChaos_PrintedBeforeRestartScannedAfter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc := qr.NewQRServices(testSecret, nil)
	if err := svc.Start(ctx); err != nil {
		t.Fatal(err)
	}
	in := qr.QRData{Data: "survives", Type: "restart"}
	printed, _ := svc.Encode(ctx, &in)
	_ = svc.Stop(ctx)

	if err := svc.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer svc.Stop(ctx)
	if out, err := svc.Decode(ctx, printed); err != nil || out != in {
		t.Fatalf("QR from before restart unreadable: %+v, %v", out, err)
	}
}

func TestIntegrationChaos_URLTransport(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc := newStarted(t)
	in := qr.QRData{Data: "?&=+/ #%", Type: "url"}

	// Codes repeatedly encoded until one contains '+' and '/', the characters
	// URL transport mangles.
	var enc string
	for range 200 {
		enc = mustEncode(t, svc, &in)
		if strings.Contains(enc, "+") && strings.Contains(enc, "/") {
			break
		}
	}

	link := "https://ecoop.example/scan?c=" + url.QueryEscape(enc)
	u, err := url.Parse(link)
	if err != nil {
		t.Fatal(err)
	}
	out, err := svc.Decode(ctx, u.Query().Get("c"))
	if err != nil || out != in {
		t.Fatalf("properly escaped URL transport broke the QR: %+v, %v", out, err)
	}
}
