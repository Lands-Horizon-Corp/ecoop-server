package service

import (
	"context"
	"io"
	"mime/multipart"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/meilisearch/meilisearch-go"
	"github.com/redis/go-redis/v9"
	"github.com/uptrace/bun"
)

type AuthService[T ClaimWithID] interface {
	SecurityService

	CurrentUser(ctx context.Context, c *app.RequestContext) (T, error)
	CurrentUserDevices(ctx context.Context, c *app.RequestContext) ([]T, error)
	Login(ctx context.Context, c *app.RequestContext, claim T, expiry time.Duration) error
	Logout(ctx context.Context, c *app.RequestContext) error
	LogoutOtherDevices(ctx context.Context, c *app.RequestContext) error

	LoggedInUsers(ctx context.Context, c *app.RequestContext) ([]T, error)
	LogoutAllUsers(ctx context.Context, c *app.RequestContext) error
}

type BroadcastService interface {
	Lifecycle

	Publish(ctx context.Context, channel, event string, payload any) error
	Dispatch(ctx context.Context, channels []string, event string, payload any) error
	Broadcast(ctx context.Context, channel string, events []string, payload any) error
	Runner(ctx context.Context)
}

type CacheService interface {
	Lifecycle
	Ping(ctx context.Context) error

	Flush(ctx context.Context) error
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value any, ttl time.Duration) error
	Exists(ctx context.Context, key string) (bool, error)
	Delete(ctx context.Context, key string) error
	Keys(ctx context.Context, pattern string) ([]string, error)
	ZAdd(ctx context.Context, key string, score float64, member any) error
	ZRange(ctx context.Context, key string, start, stop int64) ([]string, error)
	ZRangeWithScores(ctx context.Context, key string, start, stop int64) ([]redis.Z, error)
	ZCard(ctx context.Context, key string) (int64, error)
	ZRem(ctx context.Context, key string, members ...any) (int64, error)
	ZRemRangeByScore(ctx context.Context, key string, min, max string) (int64, error)
	SetNX(ctx context.Context, key string, value any, ttl time.Duration) (bool, error)
	Incr(ctx context.Context, key string, ttl time.Duration) (int64, error)
	Expire(ctx context.Context, key string, ttl time.Duration) (bool, error)
}
type ConfigService[T any] interface {
	Lifecycle
	LoadConfig(ctx context.Context) error
	Config() *T
}

type LoggingService interface {
	Lifecycle

	Debug(ctx context.Context, msg string, keysAndValues ...any)
	Info(ctx context.Context, msg string, keysAndValues ...any)
	Warn(ctx context.Context, msg string, keysAndValues ...any)
	Error(ctx context.Context, msg string, err error, keysAndValues ...any)
}

type OTPService interface {
	Lifecycle

	Generate(ctx context.Context, key string) (string, error)
	Verify(ctx context.Context, key, code string) (bool, error)
	Revoke(ctx context.Context, key string) error
}
type QRService interface {
	Lifecycle

	DecodeQR(ctx context.Context, data *QRResult) (any, error)
	EncodeQR(ctx context.Context, data any, qrType string) (*QRResult, error)
}
type ReportService interface {
	Lifecycle
	Generate(ctx context.Context, render RenderOptions) (io.ReadCloser, error)
}
type CronService interface {
	Lifecycle

	Generate(ctx context.Context, render RenderOptions) (io.ReadCloser, error)
	CreateJob(jobID string, schedule string, taskName string, payload []byte, loc *time.Location) error
	ExecuteJob(taskName string, payload []byte) error
	RemoveJob(jobID string) error
}
type SecurityService interface {
	Lifecycle

	Hash(password string) (string, error)
	VerifyHash(hash string, password string) (bool, error)

	Encrypt(ctx context.Context, data string, ttl time.Duration) (string, error)
	Decrypt(ctx context.Context, token string) (string, error)

	GenerateUUIDv5(name string) (string, error)
	Firewall(ctx context.Context, callback func(ip, host string)) error
}

type StorageService interface {
	Lifecycle

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
	Lifecycle
	Format(ctx context.Context, req SMSRequest) (*SMSRequest, error)
	Send(ctx context.Context, req SMSRequest) error
}
type SMTPService interface {
	Lifecycle
	Format(ctx context.Context, req SMTPRequest) (*SMTPRequest, error)
	Send(ctx context.Context, req SMTPRequest) error
}

// SQL and NoSQL services for database interactions.
type SQLService interface {
	Lifecycle
	Run(ctx context.Context) error
	Client() *bun.DB
	StartTransaction(ctx context.Context) (*bun.DB, func(error) error)
	Ping(ctx context.Context) error

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
	Lifecycle
	Run(ctx context.Context) error
	Client() meilisearch.ServiceManager
	SwapIndexes(ctx context.Context, indexA, indexB string) (*meilisearch.TaskInfo, error)
	Ping(ctx context.Context) error

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
	Lifecycle
	Run(ctx context.Context) error

	Client() (*bun.DB, meilisearch.ServiceManager)
	StartTransaction(ctx context.Context) (*bun.DB, func(error) error)
	Ping(ctx context.Context) error
	Sync(ctx context.Context) error

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
type StreamService interface {
	Lifecycle
	Run(ctx context.Context) error

	// Registration & Production
	RegisterHandler(topic string, handler func(ctx context.Context, topic string, key []byte, payload []byte) error) error
	Publish(ctx context.Context, topic string, key []byte, payload any) error
}

// APIService represents the interface for the API service, which includes security, caching, CQRS, storage, and logging capabilities.
type APIService interface {
	Lifecycle
	Run(ctx context.Context) error
}
