package storage

import (
	"context"
	"io"
	"mime/multipart"
	"time"
)

type (
	Storage struct {
		FileName   string
		FileSize   int64
		FileType   string
		StorageKey string
		URL        string
		BucketName string
		Status     string
		Progress   int64
	}

	ProgressCallback func(progress int64, total int64, storage *Storage)

	StorageServices interface {
		Run(ctx context.Context) error
		Stop(ctx context.Context) error
		Ping(ctx context.Context) error
		Upload(ctx context.Context, file any, onProgress ProgressCallback)
		UploadFromPath(ctx context.Context, path string, cb ProgressCallback) (*Storage, error)
		UploadFromBase64(ctx context.Context, base64 string, cb ProgressCallback) (*Storage, error)
		UploadFromURL(ctx context.Context, url string, cb ProgressCallback) (*Storage, error)
		UploadFromBinary(ctx context.Context, data []byte, cb ProgressCallback) (*Storage, error)
		UploadFromHeader(ctx context.Context, header *multipart.FileHeader, cb ProgressCallback) (*Storage, error)
		DeleteFile(ctx context.Context, storage *Storage) error
		GeneratePresignedURL(ctx context.Context, storage *Storage, expiry time.Duration) (string, error)
		GenerateUniqueName(original string, contentType string) (string, error)
		RemoveAllFiles(ctx context.Context) error
		UploadFromStream(ctx context.Context, r io.Reader, size int64, fileName string, contentType string, cb ProgressCallback) (*Storage, error)
	}
)
