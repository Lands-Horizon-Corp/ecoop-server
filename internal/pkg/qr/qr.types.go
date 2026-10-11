package qr

import "context"

type (
	QRResult struct {
		Data string `json:"data"`
		Type string `json:"type"`
	}

	QRServices interface {
		EncodeQR(ctx context.Context, data any, qrType string) (*QRResult, error)
		DecodeQR(ctx context.Context, data *QRResult) (*any, error)
	}
)
