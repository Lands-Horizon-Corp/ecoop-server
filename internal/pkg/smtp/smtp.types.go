package smtp

import "context"

type (
	SMTPRequest struct {
		Name     string
		To       string
		Subject  string
		Body     string
		Vars     map[string]string
		FromName string
	}

	SMTPServices interface {
		Start(ctx context.Context) error
		Stop(ctx context.Context) error
		Ping(ctx context.Context) error

		Format(ctx context.Context, req SMTPRequest) (*SMTPRequest, error)
		Send(ctx context.Context, req SMTPRequest) error
	}
)
