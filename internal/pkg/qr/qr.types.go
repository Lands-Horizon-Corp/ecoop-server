package qr

import "context"

type (
	QRData struct {
		Data string `json:"data"`
		Type string `json:"type"`
	}

	QRServices interface {
		Start(ctx context.Context) error
		Stop(ctx context.Context) error
		Encode(ctx context.Context, data *QRData) (string, error)
		Decode(ctx context.Context, encodedStr string) (QRData, error)
	}
)
