//go:build integration

package pdf_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pdf"
)

// Happy path: real documents in every supported shape.

func TestHappy_PageSizeAndOrientation(t *testing.T) {
	t.Parallel()
	s := started(t)
	if w, h := pageSize(t, render(t, s, invoice())); !near(w, 612) || !near(h, 792) {
		t.Fatalf("default page = %vx%v pt, want 612x792 (8.5x11in)", w, h)
	}
	land := invoice()
	land.Orientation = "LandScape " // case and spaces do not matter
	if w, h := pageSize(t, render(t, s, land)); w <= h {
		t.Fatalf("landscape page = %vx%v pt", w, h)
	}
	a4 := invoice()
	a4.Width, a4.Height = "210mm", "297mm"
	if w, h := pageSize(t, render(t, s, a4)); !near(w, 595.28) || !near(h, 841.89) {
		t.Fatalf("A4 page = %vx%v pt", w, h)
	}
}

// near compares page sizes in points; the engine rounds to float32.
func near(got, want float64) bool { return got > want-0.5 && got < want+0.5 }

func TestHappy_PasswordEncrypts(t *testing.T) {
	t.Parallel()
	s := started(t)
	plain := render(t, s, invoice())
	locked := invoice()
	locked.Password = "member-pin-1234"
	enc := render(t, s, locked)
	if bytes.Contains(plain, []byte("/Encrypt")) || !bytes.Contains(enc, []byte("/Encrypt")) {
		t.Fatal("the password did not encrypt the PDF")
	}
}

func TestHappy_GenerateToS3StoresThePDF(t *testing.T) {
	t.Parallel()
	s := started(t)
	p, err := s.GenerateToS3(ctxT(t), invoice())
	if err != nil {
		t.Fatal(err)
	}
	if p.StorageKey == "" || p.BucketName != bucket || !strings.Contains(p.URL, "X-Amz-Signature") {
		t.Fatalf("payload = %+v", p)
	}
	if size := stored(t, p.StorageKey); size == 0 || (p.FileSize != 0 && size != p.FileSize) {
		t.Fatalf("stored %d bytes, payload says %d", size, p.FileSize)
	}
}

func TestHappy_TemplatesWithoutData(t *testing.T) {
	t.Parallel()
	s := started(t)
	render(t, s, pdf.RenderOptions{Template: "<p>Branch notice: office closed on Monday.</p>"})
}

// Every language the bank's members write in, emoji included.
func TestHappy_ManyScripts(t *testing.T) {
	t.Parallel()
	s := started(t)
	render(t, s, pdf.RenderOptions{
		Template: `<p>{{ name }}</p><p>中文 日本語 한국어 العربية עברית ไทย 💸</p>`,
		Data:     map[string]string{"name": "Juan dela Cruz — ₱1,250.00"},
	})
}
