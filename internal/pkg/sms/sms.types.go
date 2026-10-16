package sms

import (
	"context"
	"errors"
	"time"

	"github.com/twilio/twilio-go"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/queue"
)

const (
	QueueName       = queue.Critical   // OTPs are time-sensitive
	TaskTypeSend    = "sms:send"       // queue job type
	SendRateLimit   = 10               // messages per RateWindow, across every instance (queue.RateLimit)
	RateWindow      = time.Second      // rate limit window
	MaxRetries      = 5                // delivery attempts after the first
	DeliveryTimeout = 15 * time.Second // one Twilio API call
)

var (
	ErrNotStarted       = errors.New("sms: service not started")
	ErrNoTracker        = errors.New("sms: service has no in-flight tracker; create it with NewSMSService")
	ErrNoQueue          = errors.New("sms: service needs a queue service")
	ErrNoCredentials    = errors.New("sms: twilio account SID and auth token are required")
	ErrInvalidMaxLength = errors.New("sms: max characters must be greater than zero")
	ErrInvalidRecipient = errors.New("sms: recipient phone number must be in E.164 format")
	ErrInvalidSender    = errors.New("sms: sender phone number must be in E.164 format")
	ErrReadTemplate     = errors.New("sms: cannot read the template file")
	ErrParseTemplate    = errors.New("sms: cannot parse the template")
	ErrExecuteTemplate  = errors.New("sms: cannot execute the template")
	ErrFormatBody       = errors.New("sms: cannot inject variables into the body")
	ErrTooLong          = errors.New("sms: body exceeds the maximum length")
	ErrEmptyBody        = errors.New("sms: body is empty")
	ErrQueueUnavailable = errors.New("sms: queue is not reachable")
	ErrEnqueue          = errors.New("sms: enqueue failed")
	ErrConnect          = errors.New("sms: twilio is not reachable")
	ErrSendFailed       = errors.New("sms: send failed")
)

type (
	SMSRequest struct {
		To   string
		Body string
		Vars map[string]string
	}

	ISMSServices interface {
		Start(ctx context.Context) error
		Stop(ctx context.Context) error
		Ping(ctx context.Context) error
		Client() *twilio.RestClient

		Format(ctx context.Context, req SMSRequest) (*SMSRequest, error)
		Send(ctx context.Context, req SMSRequest) error
	}
)
