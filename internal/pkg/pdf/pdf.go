package pdf

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

type PDFEngineService struct {
	url    string
	token  string
	client *http.Client
	gate   *utils.Gate
	obs    observability.IObserver
}

// WithObserver gives the service an observer for its metrics, traces and logs.
// Without one it observes nothing.
func WithObserver(o observability.IObserver) Option {
	return func(p *PDFEngineService) { p.obs = observability.Pick(o) }
}

// begin starts the Operation of one call. A service built as a bare struct
// literal has no observer, so it falls back to Nop without writing.
func (p *PDFEngineService) begin(ctx context.Context, name string) (context.Context, observability.Operation) {
	if p.obs == nil {
		return observability.Nop().Begin(ctx, observability.ComponentPDF, name)
	}
	return p.obs.Begin(ctx, observability.ComponentPDF, name)
}

func NewPDFService(url, token string, inflight *utils.Inflight, opts ...Option) IPDFServices {
	p := &PDFEngineService{
		url:    strings.TrimRight(url, "/"),
		token:  token,
		client: &http.Client{Timeout: RequestTimeout},
		gate:   utils.NewGate(inflight),
		obs:    observability.Nop(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(p)
		}
	}
	return p
}

func (p *PDFEngineService) Start(ctx context.Context) error {
	if p.gate == nil {
		return ErrNoTracker
	}
	var errs []error
	if p.url == "" {
		errs = append(errs, ErrNoURL)
	}
	if p.token == "" {
		errs = append(errs, ErrNoToken)
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	if err := p.health(ctx); err != nil {
		return err
	}
	p.gate.Open()
	return nil
}

func (p *PDFEngineService) Stop(ctx context.Context) error {
	return p.gate.Close(ctx)
}

func (p *PDFEngineService) Ping(ctx context.Context) (err error) {
	done, ok := p.gate.Enter()
	if !ok {
		return ErrNotStarted
	}
	defer done()
	ctx, op := p.begin(ctx, "ping")
	defer func() { op.End(err) }()
	return p.health(ctx)
}

func (p *PDFEngineService) health(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.url+"/health", nil)
	if err != nil {
		return errors.Join(ErrConnect, err)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return errors.Join(ErrConnect, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return engineError(resp)
	}
	return nil
}

func (p *PDFEngineService) Generate(ctx context.Context, render RenderOptions) (_ io.ReadCloser, err error) {
	done, ok := p.gate.Enter()
	if !ok {
		return nil, ErrNotStarted
	}
	defer done()
	ctx, op := p.begin(ctx, "generate")
	defer func() { op.End(err) }()
	resp, err := p.post(ctx, "/api/to-bytes", render) //nolint:bodyclose // the body is the result: the caller closes it
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (p *PDFEngineService) GenerateToS3(ctx context.Context, render RenderOptions) (_ *MediaPayload, err error) {
	done, ok := p.gate.Enter()
	if !ok {
		return nil, ErrNotStarted
	}
	defer done()
	ctx, op := p.begin(ctx, "generate_to_s3")
	defer func() { op.End(err) }()
	resp, err := p.post(ctx, "/api/to-s3", render)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var payload MediaPayload
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, errors.Join(ErrDecode, err)
	}
	return &payload, nil
}

// post sends render as a form to path and returns the response when it is
// 200; anything else becomes an error carrying the engine's message.
func (p *PDFEngineService) post(ctx context.Context, path string, render RenderOptions) (*http.Response, error) {
	body, contentType, err := form(render)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url+path, body)
	if err != nil {
		return nil, errors.Join(ErrConnect, err)
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Authorization", "Bearer "+p.token)
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, errors.Join(ErrConnect, err)
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		return nil, engineError(resp)
	}
	return resp, nil
}

// form encodes render as the engine's multipart form, checking it first.
func form(render RenderOptions) (*bytes.Buffer, string, error) {
	if strings.TrimSpace(render.Template) == "" {
		return nil, "", ErrNoTemplate
	}
	orientation := strings.ToLower(strings.TrimSpace(render.Orientation))
	if orientation != "" && orientation != Portrait && orientation != Landscape {
		return nil, "", ErrOrientation
	}
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	fields := [][2]string{
		{"template", render.Template}, {"width", render.Width}, {"height", render.Height},
		{"orientation", orientation}, {"filename", render.Filename}, {"password", render.Password},
	}
	if render.Data != nil {
		data, err := json.Marshal(render.Data)
		if err != nil {
			return nil, "", errors.Join(ErrEncodeData, err)
		}
		fields = append(fields, [2]string{"data", string(data)})
	}
	for _, f := range fields {
		if f[1] == "" {
			continue
		}
		if err := w.WriteField(f[0], f[1]); err != nil {
			return nil, "", fmt.Errorf("pdf: writing form field %q: %w", f[0], err)
		}
	}
	if err := w.Close(); err != nil {
		return nil, "", fmt.Errorf("pdf: closing form: %w", err)
	}
	return &buf, w.FormDataContentType(), nil
}

// engineError keeps the status and the start of the engine's message.
func engineError(resp *http.Response) error {
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, MaxErrorBody))
	return fmt.Errorf("%w (%d): %s", ErrEngine, resp.StatusCode, strings.TrimSpace(string(msg)))
}
