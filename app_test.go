package main

import (
	"testing"

	"go.uber.org/fx"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/broadcast"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/broker"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cron"
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

// asked requests every service. fx builds a provider only when something asks
// for what it makes, so validating the modules without this would check nothing.
func asked() fx.Option {
	return fx.Invoke(func(
		cache.ICacheService, sql.ISQLService, broker.IBrokerService, storage.IStorageService,
		broadcast.IBroadcastService, security.ISecurityService, qr.IQRServices, pdf.IPDFServices,
		queue.IQueueService, cron.ICronServices, smtp.ISMTPService, sms.ISMSServices,
		otp.IOTPService, cdc.ICDCService,
	) {
	})
}

// Every dependency of every module is met by another module. ValidateApp checks
// the graph without running a constructor: no database, no cache, no network.
func TestModulesFormAValidGraph(t *testing.T) {
	if err := fx.ValidateApp(Modules(), asked()); err != nil {
		t.Fatalf("the module graph is not valid: %v", err)
	}
}

// The check above must be able to fail: without the queue, mail, SMS, cron and
// OTP have nothing to send through.
func TestAMissingModuleIsReported(t *testing.T) {
	err := fx.ValidateApp(cache.Module, smtp.Module, asked())
	if err == nil {
		t.Fatal("a graph with no queue was accepted: the graph test proves nothing")
	}
}
