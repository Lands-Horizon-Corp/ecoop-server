package service

import (
	"context"

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

type StorageService interface {
	Init(ctx context.Context) error
	Shutdown(ctx context.Context) error
}
type StreamService interface {
	Init(ctx context.Context) error
	Shutdown(ctx context.Context) error
}

// SMSService represents the interface for the SMS service.
type SMSService interface {
	Init(ctx context.Context) error
	Shutdown(ctx context.Context) error
}
type SMTPService interface {
	Init(ctx context.Context) error
	Shutdown(ctx context.Context) error
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
