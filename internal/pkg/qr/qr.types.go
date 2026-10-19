package qr

import (
	"context"
	"errors"
)

var (
	ErrNotStarted  = errors.New("qr: service not started")
	ErrNilData     = errors.New("qr: data is nil")
	ErrInvalidUTF8 = errors.New("qr: data contains invalid UTF-8")
	ErrTooShort    = errors.New("qr: data is too short")
	ErrTooLarge    = errors.New("qr: data exceeds size limit")
	ErrNoTracker   = errors.New("qr: no in-flight tracker; create the service with NewQRServices")
)

// maxDecodedSize caps the JSON payload in both directions so a small QR
// cannot decompress into a large allocation.
const maxDecodedSize = 1 << 20 // 1 MiB

type (
	// Option configures a QRService: WithObserver.
	Option func(*QRService)

	QRData struct {
		Data string `json:"data"`
		Type string `json:"type"`
	}

	IQRServices interface {
		Start(ctx context.Context) error
		Stop(ctx context.Context) error
		Encode(ctx context.Context, data *QRData) (string, error)
		Decode(ctx context.Context, encodedStr string) (QRData, error)
	}
)
