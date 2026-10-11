package sms

import (
	"context"

	"github.com/twilio/twilio-go"
)

type SMSService struct {
	twilio        *twilio.RestClient
	accountSID    string
	authToken     string
	sender        string
	maxCharacters int32
	secured       bool
}

func NewSMSService(
	accountSID, authToken, sender string, maxCharacters int32, secured bool) SMSServices {
	return &SMSService{
		accountSID:    accountSID,
		authToken:     authToken,
		sender:        sender,
		maxCharacters: maxCharacters,
		secured:       secured,
	}
}

// Start implements [SMSServices].
func (s *SMSService) Start(ctx context.Context) error {
	twilioClient := twilio.NewRestClientWithParams(twilio.ClientParams{
		Username: s.accountSID,
		Password: s.authToken,
	})
	s.twilio = twilioClient
	return nil
}

// Client implements [SMSServices].
func (s *SMSService) Client() *twilio.RestClient {
	return s.twilio
}

// Format implements [SMSServices].
func (s *SMSService) Format(ctx context.Context, req SMSRequest) (*SMSRequest, error) {
	panic("unimplemented")
}

// Ping implements [SMSServices].
func (s *SMSService) Ping(ctx context.Context) error {
	panic("unimplemented")
}

// Send implements [SMSServices].
func (s *SMSService) Send(ctx context.Context, req SMSRequest) error {
	panic("unimplemented")
}

// Stop implements [SMSServices].
func (s *SMSService) Stop(ctx context.Context) error {
	panic("unimplemented")
}
