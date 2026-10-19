package main

import (
	"go.uber.org/fx"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/api"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/broadcast"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/broker"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cron"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability/otelobs"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/otp"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pdf"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/qr"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/queue"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/security"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sms"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/smtp"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sql"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/storage"
)

// Modules is every service the application runs, in the order they come up.
// Each module reads its own settings from the environment (see .env.example),
// builds its service, and registers its own start and stop. fx starts a service
// after the ones it was built from and stops it before them, so the list is
// grouped by what needs what:
//
//   - observability first, so it flushes last;
//   - the foundations: cache, database, broker, storage, broadcast, security,
//     qr, pdf, which depend on nothing but the environment;
//   - what builds on them: the queue (on the cache), then cron, mail, SMS and
//     OTP (on the queue and cache), then CDC;
//   - the API last, so it is the first to refuse new work.
//
// Leave a module out of the list to run without that service, as long as nothing
// else in the list needs it.
func Modules() fx.Option {
	return fx.Options(
		otelobs.Module,

		cache.Module,
		sql.Module,
		broker.Module,
		storage.Module,
		broadcast.Module,
		security.Module,
		qr.Module,
		pdf.Module,

		queue.Module,
		cron.Module,
		smtp.Module,
		sms.Module,
		otp.Module,
		cdc.Module,

		api.Module,
	)
}
