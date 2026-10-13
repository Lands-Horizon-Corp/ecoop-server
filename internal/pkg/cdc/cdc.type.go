package cdc

import (
	"context"
	"encoding/json"
)

type (
	ChangeEvent struct {
		Op     string          `json:"op"`
		Before json.RawMessage `json:"before"`
		After  json.RawMessage `json:"after"`
		Source struct {
			DB     string `json:"db"`
			Schema string `json:"schema"`
			Table  string `json:"table"`
			LSN    int64  `json:"lsn"`
			TxID   int64  `json:"txId"`
		} `json:"source"`
		TsMs int64 `json:"ts_ms"`
	}

	CDCServices interface {
		Run(ctx context.Context) error
		Stop(ctx context.Context) error

		Subscribe(ctx context.Context, topic string, handler func(ctx context.Context, key, value ChangeEvent) error) error
	}
)
