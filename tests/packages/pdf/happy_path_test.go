//go:build integration

package pdf_test

import (
	"bytes"
	"testing"
)

// Happy path: options shape the PDF, and GenerateToS3 stores it.

func TestHappy_Generate(t *testing.T) {
	t.Parallel()
	s := started(t)
	opts := invoice()
	opts.Orientation, opts.Password = "landscape", "member-pin-1234"
	doc := render(t, s, opts)
	if !landscape(doc) || !bytes.Contains(doc, []byte("/Encrypt")) {
		t.Fatal("orientation or password was not applied")
	}
}

func TestHappy_GenerateToS3(t *testing.T) {
	t.Parallel()
	s := started(t)
	p, err := s.GenerateToS3(ctxT(t), invoice())
	if err != nil {
		t.Fatal(err)
	}
	if p.BucketName != bucket || stored(t, p.StorageKey) == 0 {
		t.Fatalf("payload = %+v", p)
	}
}
