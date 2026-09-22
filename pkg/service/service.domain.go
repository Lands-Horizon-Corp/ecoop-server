package service

import (
	"context"
	"io"
	"mime/multipart"
	"time"

	"github.com/meilisearch/meilisearch-go"
	"gorm.io/gorm"
)

type AuthService interface {
	SecurityService
	Init(ctx context.Context) error
	Shutdown(ctx context.Context) error
}
type BroadcastService interface {
	Init(ctx context.Context) error
	Shutdown(ctx context.Context) error
}
type CacheService interface {
	Init(ctx context.Context) error
	Shutdown(ctx context.Context) error
}
type ConfigService interface {
	Init(ctx context.Context) error
	Shutdown(ctx context.Context) error
}
type LoggingService interface {
	Init(ctx context.Context) error
	Shutdown(ctx context.Context) error
}
type OTPService interface {
	Init(ctx context.Context) error
	Shutdown(ctx context.Context) error
}
type QRService interface {
	Init(ctx context.Context) error
	Shutdown(ctx context.Context) error
}
type ReportService interface {
	Init(ctx context.Context) error
	Shutdown(ctx context.Context) error
}
type CronService interface {
	Init(ctx context.Context) error
	Shutdown(ctx context.Context) error
}
type SecurityService interface {
	Init(ctx context.Context) error
	Shutdown(ctx context.Context) error
}

type StreamService interface {
	Init(ctx context.Context) error
	Shutdown(ctx context.Context) error
}

type StorageService interface {
	Init(ctx context.Context) error
	Shutdown(ctx context.Context) error

	Ping(ctx context.Context) error
	Upload(ctx context.Context, file any, cb ProgressCallback) (*Storage, error)
	UploadFromPath(ctx context.Context, path string, cb ProgressCallback) (*Storage, error)
	UploadFromURL(ctx context.Context, url string, cb ProgressCallback) (*Storage, error)
	UploadFromBinary(ctx context.Context, data []byte, cb ProgressCallback) (*Storage, error)
	UploadFromHeader(ctx context.Context, header *multipart.FileHeader, cb ProgressCallback) (*Storage, error)
	ReUpload(ctx context.Context, storage *Storage, cb ProgressCallback) (*Storage, error)

	DeleteFile(ctx context.Context, storage *Storage) error
	GeneratePresignedURL(ctx context.Context, storage *Storage, expiry time.Duration) (string, error)
	GenerateUniqueName(original string, contentType string) (string, error)
	RemoveAllFiles(ctx context.Context) error
	UploadFromStream(ctx context.Context, r io.Reader, size int64, fileName string, contentType string, cb ProgressCallback) (*Storage, error)
}

// SMSService represents the interface for the SMS service.
type SMSService interface {
	Init(ctx context.Context) error
	Shutdown(ctx context.Context) error
	Format(ctx context.Context, req SMSRequest) (*SMSRequest, error)
	Send(ctx context.Context, req SMSRequest) error
}
type SMTPService interface {
	Init(ctx context.Context) error
	Shutdown(ctx context.Context) error
	Format(ctx context.Context, req SMTPRequest) (*SMTPRequest, error)
	Send(ctx context.Context, req SMTPRequest) error
}

// SQL and NoSQL services for database interactions.
type SQLService interface {
	Init(ctx context.Context) error
	Shutdown(ctx context.Context) error
	Run(ctx context.Context) error
	Client() *gorm.DB
	StartTransaction(ctx context.Context) (*gorm.DB, func(error) error)
	Ping() error

	Migrate(ctx context.Context) error
	Rollback(ctx context.Context) error
	RollbackTo(ctx context.Context, version string) error
	Redo(ctx context.Context) error
	Status(ctx context.Context) (string, error)
	Version(ctx context.Context) (string, error)
	Fresh(ctx context.Context) error
	Create(ctx context.Context) error
	RollbackSteps(ctx context.Context, steps int) error
	UpSteps(ctx context.Context, steps int) error
}

type NOSQLService interface {
	Init(ctx context.Context) error
	Shutdown(ctx context.Context) error
	Run(ctx context.Context) error
	Client() meilisearch.ServiceManager
	SwapIndexes(ctx context.Context, indexA, indexB string) (*meilisearch.TaskInfo, error)
	Ping() error

	Migrate(ctx context.Context) error
	Rollback(ctx context.Context) error
	RollbackTo(ctx context.Context, version string) error
	Redo(ctx context.Context) error
	Status(ctx context.Context) (string, error)
	Version(ctx context.Context) (string, error)
	Fresh(ctx context.Context) error
	Create(ctx context.Context) error
	RollbackSteps(ctx context.Context, steps int) error
	UpSteps(ctx context.Context, steps int) error
}

type CQRSService interface {
	Init(ctx context.Context) error
	Shutdown(ctx context.Context) error
	Run(ctx context.Context) error
	Client() (*gorm.DB, meilisearch.ServiceManager)
	StartTransaction(ctx context.Context) (*gorm.DB, func(error) error)
	Ping() (error, error)

	Migrate(ctx context.Context) error
	Rollback(ctx context.Context) error
	RollbackTo(ctx context.Context, version string) error
	Redo(ctx context.Context) error
	Status(ctx context.Context) (string, error)
	Version(ctx context.Context) (string, error)
	Fresh(ctx context.Context) error
	Create(ctx context.Context) error
	RollbackSteps(ctx context.Context, steps int) error
	UpSteps(ctx context.Context, steps int) error
}

// APIService represents the interface for the API service, which includes security, caching, CQRS, storage, and logging capabilities.
type APIService interface {
	Init(ctx context.Context) error
	Shutdown(ctx context.Context) error
}
