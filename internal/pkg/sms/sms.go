package sms

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"net/http"
	"os"
	"text/template"
	"unicode/utf8"

	"github.com/twilio/twilio-go"
	twilioclient "github.com/twilio/twilio-go/client"
	openapi "github.com/twilio/twilio-go/rest/api/v2010"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/queue"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// SMSService sends text messages through Twilio via the job queue. When
// secured is false it runs in mock mode: requests are validated and
// formatted but nothing is queued or sent.
type SMSService struct {
	accountSID    string
	authToken     string
	sender        string
	maxCharacters int32
	secured       bool
	queue         queue.QueueServices

	gate   *utils.Gate
	twilio *twilio.RestClient
	send   *queue.Job[sendPayload]
}

type sendPayload struct {
	To   string `json:"to"`
	Body string `json:"body"`
}

// NewSMSService creates the service. inflight may be shared with other
// services; nil gives the service its own tracker.
func NewSMSService(
	accountSID, authToken, sender string, maxCharacters int32, secured bool,
	queue queue.QueueServices,
	inflight *utils.Inflight) SMSServices {
	return &SMSService{
		accountSID:    accountSID,
		authToken:     authToken,
		sender:        sender,
		maxCharacters: maxCharacters,
		secured:       secured,
		queue:         queue,
		gate:          utils.NewGate(inflight),
	}
}

func (s *SMSService) Start(ctx context.Context) error {
	if s.gate == nil {
		return ErrNoTracker
	}
	var errs []error
	if !utils.IsValidPhoneNumber(s.sender) {
		errs = append(errs, ErrInvalidSender)
	}
	if s.maxCharacters <= 0 {
		errs = append(errs, ErrInvalidMaxLength)
	}
	if s.secured {
		if s.accountSID == "" || s.authToken == "" {
			errs = append(errs, ErrNoCredentials)
		}
		if s.queue == nil {
			errs = append(errs, ErrNoQueue)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	if s.secured {
		if err := s.queue.Ping(ctx); err != nil {
			return errors.Join(ErrQueueUnavailable, err)
		}
		if s.twilio == nil {
			s.twilio = twilio.NewRestClientWithParams(twilio.ClientParams{Username: s.accountSID, Password: s.authToken})
			s.twilio.SetTimeout(DeliveryTimeout)
		}
		if s.send == nil {
			s.send = queue.NewJob(s.queue, TaskTypeSend, s.deliver,
				queue.Guard(s.gate),
				queue.RateLimit(SendRateLimit, RateWindow),
				queue.Priority(QueueName),
				queue.MaxRetry(MaxRetries),
				queue.Timeout(DeliveryTimeout),
			)
		}
	}
	s.gate.Open()
	return nil
}

func (s *SMSService) Stop(ctx context.Context) error {
	return s.gate.Close(ctx)
}
func (s *SMSService) Ping(ctx context.Context) error {
	if !s.secured {
		return nil
	}
	if !s.gate.IsOpen() {
		return ErrNotStarted
	}
	errc := make(chan error, 1)
	go func() {
		_, err := s.twilio.Api.FetchAccount(s.accountSID)
		errc <- err
	}()
	select {
	case err := <-errc:
		if err != nil {
			return errors.Join(ErrConnect, err)
		}
	case <-ctx.Done():
		return errors.Join(ErrConnect, ctx.Err())
	}
	return s.queue.Ping(ctx)
}

func (s *SMSService) Client() *twilio.RestClient {
	return s.twilio
}

func (s *SMSService) Format(ctx context.Context, req SMSRequest) (*SMSRequest, error) {
	req.Vars = maps.Clone(req.Vars)
	tmplBody := req.Body
	if utils.IsValidFilePath(req.Body) == nil {
		content, err := os.ReadFile(req.Body)
		if err != nil {
			return nil, errors.Join(ErrReadTemplate, err)
		}
		tmplBody = string(content)
	}
	tmpl, err := template.New("sms").Option("missingkey=zero").Parse(tmplBody)
	if err != nil {
		return nil, errors.Join(ErrParseTemplate, err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, req.Vars); err != nil {
		return nil, errors.Join(ErrExecuteTemplate, err)
	}
	req.Body = buf.String()
	return &req, nil
}

func (s *SMSService) Send(ctx context.Context, req SMSRequest) error {
	done, ok := s.gate.Enter()
	if !ok {
		return ErrNotStarted
	}
	defer done()
	if !utils.IsValidPhoneNumber(req.To) {
		return ErrInvalidRecipient
	}
	formatted, err := s.Format(ctx, req)
	if err != nil {
		return errors.Join(ErrFormatBody, err)
	}
	switch n := utf8.RuneCountInString(formatted.Body); {
	case n == 0:
		return ErrEmptyBody
	case n > int(s.maxCharacters):
		return ErrTooLong
	}
	if !s.secured {
		return nil
	}
	if err := s.send.Deliver(ctx, sendPayload{To: req.To, Body: formatted.Body}); err != nil {
		return errors.Join(ErrEnqueue, err)
	}
	return nil
}

func (s *SMSService) deliver(ctx context.Context, p sendPayload) error {
	params := &openapi.CreateMessageParams{}
	params.SetTo(p.To)
	params.SetFrom(s.sender)
	params.SetBody(p.Body)
	if _, err := s.twilio.Api.CreateMessage(params); err != nil {
		if rest, ok := errors.AsType[*twilioclient.TwilioRestError](err); ok &&
			rest.Status >= 400 && rest.Status < 500 && rest.Status != http.StatusTooManyRequests {
			return errors.Join(ErrSendFailed, err, queue.ErrSkipRetry)
		}
		return errors.Join(ErrSendFailed, err)
	}
	return nil
}
