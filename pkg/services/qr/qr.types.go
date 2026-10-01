package qr

import "context"

type QRData struct {
	Data string `json:"data"`
	Type string `json:"type"`
}

type QRServices interface {
	Encode(ctx context.Context, data *QRData) (string, error)
	Decode(ctx context.Context, encodedStr string) (QRData, error)
}
