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

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

type PDFEngineService struct {
	url    string
	token  string
	client *http.Client
	gate   *utils.Gate
}

func NewPDFService(url, token string, inflight *utils.Inflight) IPDFServices {
	return &PDFEngineService{
		url:    strings.TrimRight(url, "/"),
		token:  token,
		client: &http.Client{Timeout: RequestTimeout},
		gate:   utils.NewGate(inflight),
	}
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

func (p *PDFEngineService) Ping(ctx context.Context) error {
	done, ok := p.gate.Enter()
	if !ok {
		return ErrNotStarted
	}
	defer done()
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

func (p *PDFEngineService) Generate(ctx context.Context, render RenderOptions) (io.ReadCloser, error) {
	done, ok := p.gate.Enter()
	if !ok {
		return nil, ErrNotStarted
	}
	defer done()
	resp, err := p.post(ctx, "/api/to-bytes", render)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (p *PDFEngineService) GenerateToS3(ctx context.Context, render RenderOptions) (*MediaPayload, error) {
	done, ok := p.gate.Enter()
	if !ok {
		return nil, ErrNotStarted
	}
	defer done()
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
