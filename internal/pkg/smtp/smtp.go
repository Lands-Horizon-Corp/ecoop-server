package smtp

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"maps"
	"os"
	"strings"
	"sync/atomic"
	"text/template"
	"time"

	"github.com/wneessen/go-mail"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/queue"
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
	queue       queue.QueueServices
	inflight    *utils.Inflight

	started atomic.Bool
	client  *mail.Client
	send    *queue.Job[sendPayload]
}

type sendPayload struct {
	FromName string `json:"from_name"`
	To       string `json:"to"`
	Subject  string `json:"subject"`
	Body     string `json:"body"`
}

func NewSMTPService(
	host string, port int, username, password, from,
	defaultName string, secured bool,
	queue queue.QueueServices,
	inflight *utils.Inflight) SMTPServices {
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
		queue:       queue,
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
		if s.client == nil {
			opts := []mail.Option{
				mail.WithPort(s.port),
				mail.WithTimeout(DeliveryTimeout),
				mail.WithTLSPolicy(mail.TLSOpportunistic),
				mail.WithTLSConfig(&tls.Config{ServerName: s.host, MinVersion: tls.VersionTLS12}),
			}
			if s.port == 465 {
				opts = append(opts, mail.WithSSL())
			}
			if s.username != "" && s.password != "" {
				opts = append(opts, mail.WithSMTPAuth(mail.SMTPAuthPlain), mail.WithUsername(s.username), mail.WithPassword(s.password))
			}
			client, err := mail.NewClient(s.host, opts...)
			if err != nil {
				return errors.Join(ErrConnect, err)
			}
			s.client = client
		}
		if s.send == nil {
			s.send = queue.NewJob(s.queue, TaskTypeSend, s.deliver,
				queue.RateLimit(SendRateLimit, RateWindow),
				queue.Priority(QueueName),
				queue.MaxRetry(MaxRetries),
				queue.Timeout(DeliveryTimeout),
			)
		}
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
	if !s.started.Load() {
		return ErrNotStarted
	}
	errc := make(chan error, 1)
	go func() {
		conn, err := s.client.DialToSMTPClientWithContext(ctx)
		if err == nil {
			err = s.client.CloseWithSMTPClient(conn)
		}
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
	vars := make(map[string]string, len(req.Vars)+1)
	maps.Copy(vars, req.Vars)
	vars["Name"] = req.Name
	req.Vars = vars
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
	tmpl, err := template.New("email").Option("missingkey=zero").Parse(tmplBody)
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

func (s *SMTPService) Send(ctx context.Context, req SMTPRequest) error {
	done, err := s.acquire()
	if err != nil {
		return err
	}
	defer done()
	if !utils.IsValidEmail(req.To) {
		return ErrInvalidRecipient
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
	err = s.send.Deliver(ctx, sendPayload{
		FromName: strings.Join(strings.Fields(name), " "),
		To:       req.To,
		Subject:  req.Subject,
		Body:     body,
	})
	if err != nil {
		return errors.Join(ErrEnqueue, err)
	}
	return nil
}

func (s *SMTPService) deliver(ctx context.Context, p sendPayload) error {
	done, err := s.acquire()
	if err != nil {
		return queue.RetryAfter(time.Second, err)
	}
	defer done()
	msg := mail.NewMsg()
	if err := msg.FromFormat(p.FromName, s.from); err != nil {
		return errors.Join(ErrInvalidSender, err, queue.ErrSkipRetry)
	}
	if err := msg.To(p.To); err != nil {
		return errors.Join(ErrInvalidRecipient, err, queue.ErrSkipRetry)
	}
	msg.Subject(p.Subject)
	msg.SetDate()
	msg.SetMessageID()
	msg.SetBodyString(mail.TypeTextHTML, p.Body)
	if err := s.client.DialAndSendWithContext(ctx, msg); err != nil {
		return errors.Join(ErrSendFailed, err)
	}
	return nil
}
