package smtp

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"os"
	"strings"
	"text/template"

	"github.com/wneessen/go-mail"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
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
	queue       queue.IQueueService

	gate   *utils.Gate
	client *mail.Client
	send   *queue.Job[sendPayload]
	obs    observability.IObserver
}

// WithObserver gives the service an observer for its metrics, traces and logs.
// Without one it observes nothing. Only the operation name and its outcome are
// ever recorded: never an address, a subject or a body.
func WithObserver(o observability.IObserver) Option {
	return func(s *SMTPService) { s.obs = observability.Pick(o) }
}

// begin starts the Operation of one call. A service built as a bare struct
// literal has no observer, so it falls back to Nop without writing.
func (s *SMTPService) begin(ctx context.Context, name string) (context.Context, observability.Operation) {
	if s.obs == nil {
		return observability.Nop().Begin(ctx, observability.ComponentSMTP, name)
	}
	return s.obs.Begin(ctx, observability.ComponentSMTP, name)
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
	queue queue.IQueueService,
	inflight *utils.Inflight, opts ...Option) ISMTPService {
	s := &SMTPService{
		host:        host,
		port:        port,
		username:    username,
		password:    password,
		from:        from,
		defaultName: defaultName,
		secured:     secured,
		queue:       queue,
		gate:        utils.NewGate(inflight),
		obs:         observability.Nop(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	return s
}
func (s *SMTPService) Start(ctx context.Context) error {
	if s.gate == nil {
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

func (s *SMTPService) Stop(ctx context.Context) error {
	return s.gate.Close(ctx)
}

func (s *SMTPService) Ping(ctx context.Context) error {
	if !s.secured {
		return nil
	}
	if !s.gate.IsOpen() {
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

func (s *SMTPService) Format(ctx context.Context, req SMTPRequest) (*SMTPRequest, error) {
	req.Vars = utils.CloneWith(req.Vars, "Name", req.Name)
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

func (s *SMTPService) Send(ctx context.Context, req SMTPRequest) (err error) {
	done, ok := s.gate.Enter()
	if !ok {
		return ErrNotStarted
	}
	defer done()
	ctx, op := s.begin(ctx, "send")
	defer func() { op.End(err) }()
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

func (s *SMTPService) deliver(ctx context.Context, p sendPayload) (err error) {
	ctx, op := s.begin(ctx, "deliver")
	defer func() { op.End(err) }()
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
