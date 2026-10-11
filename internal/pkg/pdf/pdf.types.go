package pdf

import (
	"context"
	"io"
)

type (
	RenderOptions struct {
		Template    string `json:"template"`
		Data        any    `json:"data"`
		Filename    string `json:"filename,omitempty"`
		Width       string `json:"width,omitempty"`
		Height      string `json:"height,omitempty"`
		Password    string `json:"password,omitempty"`
		BaseURL     string `json:"base_url,omitempty"`
		Media       string `json:"media,omitempty"`
		Orientation string `json:"orientation"`
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
		Generate(ctx context.Context, render RenderOptions) (io.ReadCloser, error)
		GenerateToS3(ctx context.Context, render RenderOptions) (*MediaPayload, error)
	}
)
