package service

import "context"

type APIService interface {
	Init(ctx context.Context) error
	Shutdown(ctx context.Context) error
}
type AuthService interface {
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
type SQLService interface {
	Init(ctx context.Context) error
	Shutdown(ctx context.Context) error
}
type NOSQLService interface {
	Init(ctx context.Context) error
	Shutdown(ctx context.Context) error
}
type CQRSService interface {
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
type SMSService interface {
	Init(ctx context.Context) error
	Shutdown(ctx context.Context) error
}
type SMTPService interface {
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
