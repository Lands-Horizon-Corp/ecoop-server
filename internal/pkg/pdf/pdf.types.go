package pdf

import (
	"context"
	"errors"
	"io"
	"time"
)

const (
	RequestTimeout = 60 * time.Second // above the engine's own 45s render timeout
	MaxErrorBody   = 4 << 10          // how much of an engine error is kept

	Portrait  = "portrait"
	Landscape = "landscape"
)

var (
	ErrNotStarted  = errors.New("pdf: service not started")
	ErrNoTracker   = errors.New("pdf: service has no in-flight tracker; create it with NewPDFEngine")
	ErrNoURL       = errors.New("pdf: missing engine URL")
	ErrNoToken     = errors.New("pdf: missing engine token")
	ErrNoTemplate  = errors.New("pdf: template is empty")
	ErrOrientation = errors.New("pdf: orientation must be portrait or landscape")
	ErrEncodeData  = errors.New("pdf: data cannot be encoded as JSON")
	ErrConnect     = errors.New("pdf: cannot reach the PDF engine")
	ErrEngine      = errors.New("pdf: the PDF engine refused the request")
	ErrDecode      = errors.New("pdf: unexpected response from the PDF engine")
)

type (
	// RenderOptions is one document: an HTML/Minijinja Template filled with
	// Data. Width and Height take in, mm, cm, pt, pc or px (the engine
	// defaults to 8.5in x 11in); Orientation is Portrait or Landscape.
	RenderOptions struct {
		Template    string `json:"template"`
		Data        any    `json:"data"`
		Filename    string `json:"filename,omitempty"`
		Width       string `json:"width,omitempty"`
		Height      string `json:"height,omitempty"`
		Password    string `json:"password,omitempty"`
		Orientation string `json:"orientation,omitempty"`
	}

	MediaPayload struct {
		FileName   string `json:"file_name"`
		FileSize   int64  `json:"file_size"`
		FileType   string `json:"file_type"`
		StorageKey string `json:"storage_key"`
		URL        string `json:"url"`
		BucketName string `json:"bucket_name"`
		Status     string `json:"status"`
		Progress   int32  `json:"progress"`
	}

	PDFServices interface {
		Start(ctx context.Context) error
		Stop(ctx context.Context) error
		Ping(ctx context.Context) error

		Generate(ctx context.Context, render RenderOptions) (io.ReadCloser, error)
		GenerateToS3(ctx context.Context, render RenderOptions) (*MediaPayload, error)
	}
)
