package smtp

import (
	"context"
	"errors"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/queue"
)

const (
	QueueName       = queue.Critical   // OTPs and notices are time-sensitive
	TaskTypeSend    = "smtp:send"      // queue job type
	SendRateLimit   = 10               // emails per RateWindow, shared by every instance
	RateWindow      = time.Second      // rate limit window
	MaxRetries      = 8                // delivery attempts after the first
	DeliveryTimeout = 30 * time.Second // one delivery attempt, connect to QUIT
)

var (
	ErrReadTemplate     = errors.New("smtp: cannot read the template file")
	ErrParseTemplate    = errors.New("smtp: cannot parse the template")
	ErrExecuteTemplate  = errors.New("smtp: cannot execute the template")
	ErrNotStarted       = errors.New("smtp: service not started")
	ErrNoTracker        = errors.New("smtp: service has no in-flight tracker; create it with NewSMTPService")
	ErrInvalidHost      = errors.New("smtp: host is not set")
	ErrInvalidPort      = errors.New("smtp: port must be between 1 and 65535")
	ErrInvalidRecipient = errors.New("smtp: recipient email format is invalid")
	ErrInvalidSender    = errors.New("smtp: sender email format is invalid")
	ErrFormatBody       = errors.New("smtp: cannot inject variables into the body")
	ErrConnect          = errors.New("smtp: connect failed")
	ErrSendFailed       = errors.New("smtp: send failed")
	ErrQueueUnavailable = errors.New("smtp: queue is not reachable")
	ErrNoQueue          = errors.New("smtp: service needs a queue service")
	ErrEnqueue          = errors.New("smtp: enqueue failed")
)

type (
	// Option configures an SMTPService: WithObserver.
	Option func(*SMTPService)

	SMTPRequest struct {
		Name     string
		To       string
		Subject  string
		Body     string
		Vars     map[string]string
		FromName string
	}

	ISMTPService interface {
		Start(ctx context.Context) error
		Stop(ctx context.Context) error
		Ping(ctx context.Context) error

		Format(ctx context.Context, req SMTPRequest) (*SMTPRequest, error)
		Send(ctx context.Context, req SMTPRequest) error
	}
)
