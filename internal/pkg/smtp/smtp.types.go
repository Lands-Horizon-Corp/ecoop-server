package smtp

import (
	"context"
	"errors"
)

var (
	ErrReadTemplate     = errors.New("failed to read template file")
	ErrParseTemplate    = errors.New("parse template failed")
	ErrExecuteTemplate  = errors.New("execute template failed")
	ErrNotStarted       = errors.New("smtp service not started")
	ErrNoTracker        = errors.New("smtp service has no in-flight tracker; create it with NewSMTPService")
	ErrInvalidHost      = errors.New("smtp host is not set")
	ErrInvalidPort      = errors.New("smtp port must be between 1 and 65535")
	ErrInvalidRecipient = errors.New("recipient email format is invalid")
	ErrInvalidSender    = errors.New("admin email format is invalid")
	ErrFormatBody       = errors.New("failed to inject variables into body")
	ErrConnect          = errors.New("smtp connect failed")
	ErrSendFailed       = errors.New("smtp send failed")
)

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
