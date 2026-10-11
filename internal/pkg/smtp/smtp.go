package smtp

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"text/template"
	"time"

	"github.com/wneessen/go-mail"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

type SMTPService struct {
	host        string
	port        int
	username    string
	password    string
	from        string
	secured     bool
	defaultName string
	started     atomic.Bool
	inflight    *utils.Inflight
}

func NewSMTPService(
	host string, port int, username, password, from, defaultName string, secured bool, inflight *utils.Inflight) SMTPServices {
	if inflight == nil {
		inflight = utils.NewInflight()
	}
	return &SMTPService{
		host:        host,
		port:        port,
		username:    username,
		password:    password,
		from:        from,
		defaultName: defaultName,
		secured:     secured,
		inflight:    inflight,
	}
}

func (s *SMTPService) Start(ctx context.Context) error {
	if s.inflight == nil {
		return ErrNoTracker
	}
	var errs []error
	if !utils.IsValidEmail(s.from) {
		errs = append(errs, ErrInvalidSender)
	}
	if s.secured {
		if s.host == "" {
			errs = append(errs, ErrInvalidHost)
		}
		if s.port < 1 || s.port > 65535 {
			errs = append(errs, ErrInvalidPort)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	s.started.Store(true)
	return nil
}

func (s *SMTPService) Stop(ctx context.Context) error {
	if !s.started.Swap(false) {
		return nil
	}
	return s.inflight.Wait(ctx)
}

func (s *SMTPService) Ping(ctx context.Context) error {
	if !s.secured {
		return nil
	}
	client, err := s.client(ctx)
	if err != nil {
		return errors.Join(ErrConnect, err)
	}
	if err := client.DialWithContext(ctx); err != nil {
		return errors.Join(ErrConnect, err)
	}
	return client.Close()
}

func (s *SMTPService) acquire() (done func(), err error) {
	if !s.started.Load() {
		return nil, ErrNotStarted
	}
	done = s.inflight.Track()
	if !s.started.Load() {
		done()
		return nil, ErrNotStarted
	}
	return done, nil
}

func (s *SMTPService) Format(ctx context.Context, req SMTPRequest) (*SMTPRequest, error) {
	var formattedReq *SMTPRequest
	if req.Vars == nil {
		req.Vars = make(map[string]string)
	}
	req.Vars["Name"] = req.Name
	var tmplBody string
	if err := utils.IsValidFilePath(req.Body); err == nil {
		content, err := os.ReadFile(req.Body)
		if err != nil {
			return nil, errors.Join(ErrReadTemplate, err)
		}
		tmplBody = string(content)
	} else {
		tmplBody = req.Body
	}
	tmpl, err := template.New("email").Parse(tmplBody)
	if err != nil {
		return nil, errors.Join(ErrParseTemplate, err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, req.Vars); err != nil {
		return nil, errors.Join(ErrExecuteTemplate, err)
	}
	req.Body = buf.String()
	formattedReq = &req
	return formattedReq, nil
}

func (s *SMTPService) Send(ctx context.Context, req SMTPRequest) error {
	done, err := s.acquire()
	if err != nil {
		return err
	}
	defer done()
	if !utils.IsValidEmail(req.To) {
		return ErrInvalidRecipient
	}
	if !utils.IsValidEmail(s.from) {
		return ErrInvalidSender
	}
	formatted, err := s.Format(ctx, req)
	if err != nil {
		return errors.Join(ErrFormatBody, err)
	}
	body := utils.Sanitize(formatted.Body)
	if !s.secured {
		return nil
	}

	name := req.FromName
	if name == "" {
		name = s.defaultName
	}
	msg := mail.NewMsg()
	name = strings.Join(strings.Fields(name), " ")
	if err := msg.FromFormat(name, s.from); err != nil {
		return errors.Join(ErrInvalidSender, err)
	}
	if err := msg.To(req.To); err != nil {
		return errors.Join(ErrInvalidRecipient, err)
	}
	msg.Subject(req.Subject)
	msg.SetDate()
	msg.SetMessageID()
	msg.SetBodyString(mail.TypeTextHTML, body)

	client, err := s.client(ctx)
	if err != nil {
		return errors.Join(ErrConnect, err)
	}
	if err := client.DialAndSendWithContext(ctx, msg); err != nil {
		return errors.Join(ErrSendFailed, err)
	}
	return nil
}

func (s *SMTPService) client(ctx context.Context) (*mail.Client, error) {
	opts := []mail.Option{
		mail.WithPort(s.port),
		mail.WithTLSPolicy(mail.TLSOpportunistic),
		mail.WithTLSConfig(&tls.Config{ServerName: s.host, MinVersion: tls.VersionTLS12}),
	}
	if deadline, ok := ctx.Deadline(); ok {
		opts = append(opts, mail.WithTimeout(max(time.Until(deadline), time.Millisecond)))
	}
	if s.port == 465 {
		opts = append(opts, mail.WithSSL())
	}
	if s.username != "" && s.password != "" {
		opts = append(opts,
			mail.WithSMTPAuth(mail.SMTPAuthPlain),
			mail.WithUsername(s.username),
			mail.WithPassword(s.password),
		)
	}
	return mail.NewClient(s.host, opts...)
}
