package sms

import "context"

type (
	SMSRequest struct {
		To   string
		Body string
		Vars map[string]string
	}
	SMSServices interface {
		Start(ctx context.Context) error
		Stop(ctx context.Context) error
		Ping(ctx context.Context) error

		Format(ctx context.Context, req SMSRequest) (*SMSRequest, error)
		Send(ctx context.Context, req SMSRequest) error
	}
)
