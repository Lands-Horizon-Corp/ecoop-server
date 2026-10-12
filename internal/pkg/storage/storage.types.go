package storage

import (
	"context"
	"errors"
	"io"
	"mime/multipart"
	"time"
)

const (
	DriverS3 = "s3" // AWS S3: virtual-host (DNS) bucket addressing
	// Any other driver (minio, rustfs, ...) uses path-style addressing.

	DefaultRegion      = "us-east-1"
	DefaultMaxFileSize = 100 << 20          // 100 MiB
	DefaultURLTimeout  = 30 * time.Second   // whole download in UploadFromURL
	MaxPresignExpiry   = 7 * 24 * time.Hour // the S3 signature limit
	UnknownPartSize    = 16 << 20           // multipart part size when the size is unknown

	StatusUploading = "uploading"
	StatusCompleted = "completed"
)

var (
	ErrNotStarted      = errors.New("storage: service not started")
	ErrNoTracker       = errors.New("storage: service has no in-flight tracker; create it with NewStorageService")
	ErrNoEndpoint      = errors.New("storage: missing endpoint")
	ErrNoAccessKey     = errors.New("storage: missing access key")
	ErrNoSecretKey     = errors.New("storage: missing secret key")
	ErrNoBucket        = errors.New("storage: missing bucket name")
	ErrConnect         = errors.New("storage: cannot reach the storage server")
	ErrBucket          = errors.New("storage: cannot use the bucket")
	ErrUnsupportedType = errors.New("storage: Upload accepts a path, URL, data URI, []byte, io.Reader or *multipart.FileHeader")
	ErrNilReader       = errors.New("storage: reader is nil")
	ErrNilHeader       = errors.New("storage: file header is nil")
	ErrNilStorage      = errors.New("storage: storage is nil")
	ErrNoKey           = errors.New("storage: storage key is empty")
	ErrWrongBucket     = errors.New("storage: the file belongs to another bucket")
	ErrEmptyFile       = errors.New("storage: file is empty")
	ErrTooLarge        = errors.New("storage: file is larger than the limit")
	ErrInvalidBase64   = errors.New("storage: invalid base64 data")
	ErrInvalidURL      = errors.New("storage: URL must be an absolute http or https URL")
	ErrBlockedAddress  = errors.New("storage: URL points at a private or local address")
	ErrDownload        = errors.New("storage: downloading the URL failed")
	ErrInvalidExpiry   = errors.New("storage: expiry must be between 1 second and 7 days")
	ErrUpload          = errors.New("storage: upload failed")
	ErrCallback        = errors.New("storage: progress callback panicked; upload stopped")
	ErrDelete          = errors.New("storage: delete failed")
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

	Option func(*StorageService)

	StorageServices interface {
		Run(ctx context.Context) error
		Stop(ctx context.Context) error
		Ping(ctx context.Context) error

		Upload(ctx context.Context, file any, onProgress ProgressCallback) (*Storage, error)
		UploadFromPath(ctx context.Context, path string, cb ProgressCallback) (*Storage, error)
		UploadFromBase64(ctx context.Context, base64 string, cb ProgressCallback) (*Storage, error)
		UploadFromURL(ctx context.Context, url string, cb ProgressCallback) (*Storage, error)
		UploadFromBinary(ctx context.Context, data []byte, cb ProgressCallback) (*Storage, error)
		UploadFromHeader(ctx context.Context, header *multipart.FileHeader, cb ProgressCallback) (*Storage, error)
		UploadFromStream(ctx context.Context, r io.Reader, size int64, fileName string, contentType string, cb ProgressCallback) (*Storage, error)

		DeleteFile(ctx context.Context, storage *Storage) error
		GeneratePresignedURL(ctx context.Context, storage *Storage, expiry time.Duration) (string, error)
		GenerateUniqueName(original string, contentType string) (string, error)
		RemoveAllFiles(ctx context.Context) error
	}
)
