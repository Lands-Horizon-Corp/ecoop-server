package fromenv_test

import (
	"errors"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/broadcast"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/broker"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/otp"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/pdf"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/qr"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/queue"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/security"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sms"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/smtp"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/sql"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/storage"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// No build tag and no server: FromEnv is a function of a lookup, so each
// package's settings are checked here, including what it refuses.

func look(kv map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := kv[k]; return v, ok }
}

func with(base map[string]string, kv ...string) map[string]string {
	out := map[string]string{}
	maps.Copy(out, base)
	for i := 0; i+1 < len(kv); i += 2 {
		out[kv[i]] = kv[i+1]
	}
	return out
}

// mustFail asserts err is an env error that names every one of the variables.
func mustFail(t *testing.T, what string, err error, vars ...string) {
	t.Helper()
	if err == nil {
		t.Errorf("%s: accepted", what)
		return
	}
	if !errors.Is(err, utils.ErrEnvMissing) && !errors.Is(err, utils.ErrEnvInvalid) {
		t.Errorf("%s: %v is neither missing nor invalid", what, err)
	}
	for _, v := range vars {
		if !strings.Contains(err.Error(), v) {
			t.Errorf("%s: the error does not name %s: %v", what, v, err)
		}
	}
}

func TestCache(t *testing.T) {
	t.Parallel()
	c, err := cache.FromEnv(look(nil))
	if err != nil || len(c.Addresses) != 1 || c.Addresses[0] != "127.0.0.1:6379" || c.Password != "" {
		t.Errorf("defaults = %+v, %v", c, err)
	}
	c, err = cache.FromEnv(look(map[string]string{"CACHE_HOST": "cache.internal", "CACHE_PORT": "6380", "CACHE_PASSWORD": "pw"}))
	if err != nil || c.Addresses[0] != "cache.internal:6380" || c.Password != "pw" {
		t.Errorf("set = %+v, %v", c, err)
	}
	if c, _ := cache.FromEnv(look(map[string]string{"CACHE_HOST": "::1"})); c.Addresses[0] != "[::1]:6379" {
		t.Errorf("an IPv6 host was not bracketed: %q", c.Addresses[0])
	}
}

func TestQueue(t *testing.T) {
	t.Parallel()
	if c, err := queue.FromEnv(look(nil)); err != nil || c.Namespace != "" {
		t.Errorf("defaults = %+v, %v", c, err)
	}
	if c, _ := queue.FromEnv(look(map[string]string{"QUEUE_NAMESPACE": "ecoop"})); c.Namespace != "ecoop" {
		t.Errorf("namespace = %q", c.Namespace)
	}
}

func TestSecurity(t *testing.T) {
	t.Parallel()
	ok := map[string]string{"APP_SECRET": "s"}
	c, err := security.FromEnv(look(ok))
	// RFC 9106's second recommended Argon2id profile, written out: comparing the
	// constants with themselves would not notice one being weakened.
	if err != nil || c.Secret != "s" || c.MemoryKiB != 65536 || c.Iterations != 3 ||
		c.Parallelism != 4 || c.SaltBytes != 16 || c.KeyBytes != 32 {
		t.Errorf("defaults = %+v, %v", c, err)
	}
	_, err = security.FromEnv(look(nil))
	mustFail(t, "no secret", err, "APP_SECRET")
	for _, tc := range []struct{ key, val string }{
		{"SECURITY_ITERATIONS", "0"}, {"SECURITY_PARALLELISM", "0"}, {"SECURITY_PARALLELISM", "256"},
		{"SECURITY_SALT_BYTES", "8"}, {"SECURITY_KEY_BYTES", "15"}, {"SECURITY_MEMORY_KIB", "4"},
		{"SECURITY_ITERATIONS", "many"}, {"SECURITY_MEMORY_KIB", "-1"},
	} {
		_, err := security.FromEnv(look(with(ok, tc.key, tc.val)))
		mustFail(t, tc.key+"="+tc.val, err, tc.key)
	}
	// Every weak value is reported in one go.
	_, err = security.FromEnv(look(with(ok, "SECURITY_ITERATIONS", "0", "SECURITY_SALT_BYTES", "4", "SECURITY_KEY_BYTES", "4")))
	mustFail(t, "three weak values", err, "SECURITY_ITERATIONS", "SECURITY_SALT_BYTES", "SECURITY_KEY_BYTES")
	// Whatever FromEnv accepts, the service accepts.
	c, _ = security.FromEnv(look(with(ok, "SECURITY_MEMORY_KIB", "8", "SECURITY_ITERATIONS", "1", "SECURITY_PARALLELISM", "1")))
	if c.MemoryKiB != 8 || c.Iterations != 1 || c.Parallelism != 1 {
		t.Errorf("minimums were changed: %+v", c)
	}
}

func TestSQL(t *testing.T) {
	t.Parallel()
	ok := map[string]string{"POSTGRES_USER": "u", "POSTGRES_PASSWORD": "p@ss/word", "POSTGRES_DB": "d"}
	c, err := sql.FromEnv(look(ok))
	if err != nil {
		t.Fatal(err)
	}
	if c.DSN != "postgres://u:p%40ss%2Fword@127.0.0.1:5432/d?sslmode=disable" {
		t.Errorf("DSN = %q (a password with @ or / must be escaped)", c.DSN)
	}
	if c.MaxOpenConns != 20 || c.MaxIdleConns != 5 || c.AutoMigrate || c.MigrationsDir != "" {
		t.Errorf("defaults = %+v", c)
	}
	c, _ = sql.FromEnv(look(with(ok, "POSTGRES_MAX_CONNECTIONS", "3", "POSTGRES_AUTO_MIGRATE", "true", "POSTGRES_MIGRATIONS_DIR", "/m", "POSTGRES_SSLMODE", "require")))
	if c.MaxOpenConns != 3 || c.MaxIdleConns != 1 || !c.AutoMigrate || c.MigrationsDir != "/m" || !strings.HasSuffix(c.DSN, "sslmode=require") {
		t.Errorf("set = %+v", c)
	}
	_, err = sql.FromEnv(look(nil))
	mustFail(t, "nothing set", err, "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB")
	_, err = sql.FromEnv(look(with(ok, "POSTGRES_MAX_CONNECTIONS", "0")))
	mustFail(t, "no connections", err, "POSTGRES_MAX_CONNECTIONS")
	_, err = sql.FromEnv(look(with(ok, "POSTGRES_AUTO_MIGRATE", "yes please")))
	mustFail(t, "a typo in a flag", err, "POSTGRES_AUTO_MIGRATE")
}

func TestSQLReadModelIsASeparateDatabaseAndNeverMigrated(t *testing.T) {
	t.Parallel()
	env := map[string]string{
		"POSTGRES_USER": "w", "POSTGRES_PASSWORD": "wp", "POSTGRES_DB": "write",
		"POSTGRES_READ_USER": "r", "POSTGRES_READ_PASSWORD": "rp", "POSTGRES_READ_DB": "read",
		"POSTGRES_AUTO_MIGRATE": "true", "POSTGRES_MIGRATIONS_DIR": "/m",
		"POSTGRES_READ_AUTO_MIGRATE": "true", "POSTGRES_READ_MIGRATIONS_DIR": "/m",
	}
	w, err := sql.FromEnv(look(env))
	if err != nil {
		t.Fatal(err)
	}
	r, err := sql.ReadFromEnv(look(env))
	if err != nil {
		t.Fatal(err)
	}
	if w.DSN == r.DSN || !strings.Contains(r.DSN, "/read") || !strings.Contains(r.DSN, ":5433/") || !strings.Contains(w.DSN, ":5432/") {
		t.Errorf("writer %q and reader %q are not two databases", w.DSN, r.DSN)
	}
	if r.AutoMigrate || r.MigrationsDir != "" {
		t.Errorf("the read model must never be migrated by the service: %+v", r)
	}
	if !w.AutoMigrate || w.MigrationsDir != "/m" {
		t.Errorf("the writer lost its migration settings: %+v", w)
	}
	_, err = sql.ReadFromEnv(look(map[string]string{"POSTGRES_USER": "w", "POSTGRES_PASSWORD": "wp", "POSTGRES_DB": "write"}))
	mustFail(t, "a reader with only the writer's settings", err, "POSTGRES_READ_USER", "POSTGRES_READ_PASSWORD", "POSTGRES_READ_DB")
}

func TestStorage(t *testing.T) {
	t.Parallel()
	ok := map[string]string{"STORAGE_ACCESS_KEY": "a", "STORAGE_SECRET_KEY": "s", "STORAGE_BUCKET": "b"}
	c, err := storage.FromEnv(look(ok))
	if err != nil || c.Endpoint != "127.0.0.1:9000" || c.Driver != "rustfs" || c.Region != "us-east-1" ||
		c.SSL || c.MaxFileSize != 100<<20 || c.URLTimeout != 30*time.Second {
		t.Errorf("defaults = %+v, %v", c, err)
	}
	c, _ = storage.FromEnv(look(with(ok, "STORAGE_HOST", "s3.local", "STORAGE_PORT", "443", "STORAGE_SECURED", "true",
		"STORAGE_MAX_FILE_SIZE", "1024", "STORAGE_URL_TIMEOUT", "5s", "STORAGE_PUBLIC_ENDPOINT", "cdn.test", "STORAGE_DRIVER", "s3")))
	if c.Endpoint != "s3.local:443" || !c.SSL || c.MaxFileSize != 1024 || c.URLTimeout != 5*time.Second || c.PublicEndpoint != "cdn.test" || c.Driver != "s3" {
		t.Errorf("set = %+v", c)
	}
	_, err = storage.FromEnv(look(nil))
	mustFail(t, "nothing set", err, "STORAGE_ACCESS_KEY", "STORAGE_SECRET_KEY", "STORAGE_BUCKET")
	for _, v := range []string{"0", "-5", "big"} {
		_, err := storage.FromEnv(look(with(ok, "STORAGE_MAX_FILE_SIZE", v)))
		mustFail(t, "STORAGE_MAX_FILE_SIZE="+v, err, "STORAGE_MAX_FILE_SIZE")
	}
}

func TestSMTP(t *testing.T) {
	t.Parallel()
	ok := map[string]string{"SMTP_FROM": "noreply@ecoop.test"}
	c, err := smtp.FromEnv(look(ok))
	if err != nil || c.Host != "127.0.0.1" || c.Port != 1025 || c.Secured || c.From != "noreply@ecoop.test" {
		t.Errorf("defaults = %+v, %v", c, err)
	}
	_, err = smtp.FromEnv(look(nil))
	mustFail(t, "no sender", err, "SMTP_FROM")
	for _, v := range []string{"0", "70000", "smtp"} {
		_, err := smtp.FromEnv(look(with(ok, "SMTP_PORT", v)))
		mustFail(t, "SMTP_PORT="+v, err, "SMTP_PORT")
	}
	_, err = smtp.FromEnv(look(with(ok, "SMTP_SECURED", "sure")))
	mustFail(t, "a typo in the security flag", err, "SMTP_SECURED")
}

func TestSMS(t *testing.T) {
	t.Parallel()
	ok := map[string]string{"SMS_SENDER": "+15005550006"}
	c, err := sms.FromEnv(look(ok))
	if err != nil || c.MaxCharacters != 160 || c.Secured || c.Sender != "+15005550006" {
		t.Errorf("defaults = %+v, %v", c, err)
	}
	_, err = sms.FromEnv(look(nil))
	mustFail(t, "no sender", err, "SMS_SENDER")
	// Credentials matter only when messages are really sent.
	_, err = sms.FromEnv(look(with(ok, "SMS_SECURED", "true")))
	mustFail(t, "secured without credentials", err, "SMS_ACCOUNT_SID", "SMS_AUTH_TOKEN")
	if _, err := sms.FromEnv(look(with(ok, "SMS_SECURED", "true", "SMS_ACCOUNT_SID", "AC1", "SMS_AUTH_TOKEN", "t"))); err != nil {
		t.Errorf("secured with credentials: %v", err)
	}
	_, err = sms.FromEnv(look(with(ok, "SMS_MAX_CHARACTERS", "0")))
	mustFail(t, "no characters", err, "SMS_MAX_CHARACTERS")
}

func TestOTPAndQRNeedTheAppSecret(t *testing.T) {
	t.Parallel()
	if c, err := otp.FromEnv(look(map[string]string{"APP_SECRET": "s"})); err != nil || c.Secret != "s" {
		t.Errorf("otp = %+v, %v", c, err)
	}
	if c, err := qr.FromEnv(look(map[string]string{"APP_SECRET": "s"})); err != nil || c.Secret != "s" {
		t.Errorf("qr = %+v, %v", c, err)
	}
	_, err := otp.FromEnv(look(nil))
	mustFail(t, "otp without a secret", err, "APP_SECRET")
	_, err = qr.FromEnv(look(nil))
	mustFail(t, "qr without a secret", err, "APP_SECRET")
}

func TestPDF(t *testing.T) {
	t.Parallel()
	ok := map[string]string{"PDF_ENGINE_API_BEARER_TOKEN": "t"}
	c, err := pdf.FromEnv(look(ok))
	if err != nil || c.URL != "http://127.0.0.1:6767" || c.Token != "t" {
		t.Errorf("defaults = %+v, %v", c, err)
	}
	if c, _ := pdf.FromEnv(look(with(ok, "PDF_ENGINE_API_HOST", "pdf", "PDF_ENGINE_API_PORT", "80"))); c.URL != "http://pdf:80" {
		t.Errorf("host and port: %q", c.URL)
	}
	if c, _ := pdf.FromEnv(look(with(ok, "PDF_ENGINE_API_URL", "https://pdf.internal", "PDF_ENGINE_API_HOST", "ignored"))); c.URL != "https://pdf.internal" {
		t.Errorf("an explicit URL must win: %q", c.URL)
	}
	_, err = pdf.FromEnv(look(nil))
	mustFail(t, "no token", err, "PDF_ENGINE_API_BEARER_TOKEN")
}

func TestBroker(t *testing.T) {
	t.Parallel()
	c, err := broker.FromEnv(look(map[string]string{"BROKER_GROUP_ID": "g"}))
	if err != nil || len(c.Brokers) != 1 || c.Brokers[0] != "127.0.0.1:9094" || c.GroupID != "g" {
		t.Errorf("defaults = %+v, %v", c, err)
	}
	_, err = broker.FromEnv(look(nil))
	mustFail(t, "no group", err, "BROKER_GROUP_ID")
}

func TestCDC(t *testing.T) {
	t.Parallel()
	c, err := cdc.FromEnv(look(map[string]string{"BROKER_GROUP_ID": "app"}))
	if err != nil || c.GroupID != "app" || c.TopicPrefix != "ecoop" || c.Brokers[0] != "127.0.0.1:9094" {
		t.Errorf("defaults = %+v, %v", c, err)
	}
	if c, _ := cdc.FromEnv(look(map[string]string{"BROKER_GROUP_ID": "app", "CDC_GROUP_ID": "cdc", "CDC_TOPIC_PREFIX": "bank"})); c.GroupID != "cdc" || c.TopicPrefix != "bank" {
		t.Errorf("explicit settings: %+v", c)
	}
	_, err = cdc.FromEnv(look(nil))
	mustFail(t, "no group at all", err, "BROKER_GROUP_ID")
}

func TestBroadcast(t *testing.T) {
	t.Parallel()
	ok := map[string]string{"BROADCAST_APP_ID": "i", "BROADCAST_APP_KEY": "k", "BROADCAST_APP_SECRET": "s"}
	c, err := broadcast.FromEnv(look(ok))
	if err != nil || c.Host != "127.0.0.1" || c.Port != "6001" || c.Secured || c.MaxPayloadKB != 10 {
		t.Errorf("defaults = %+v, %v", c, err)
	}
	_, err = broadcast.FromEnv(look(nil))
	mustFail(t, "nothing set", err, "BROADCAST_APP_ID", "BROADCAST_APP_KEY", "BROADCAST_APP_SECRET")
	_, err = broadcast.FromEnv(look(with(ok, "BROADCAST_MAX_PAYLOAD_KB", "0")))
	mustFail(t, "no payload", err, "BROADCAST_MAX_PAYLOAD_KB")
}

// No FromEnv reads a secret into an error: a bad value names its variable only.
func TestNoErrorEchoesASecret(t *testing.T) {
	t.Parallel()
	const secret = "s3cr3t-value-xyz"
	bad := map[string]string{
		"APP_SECRET": secret, "SECURITY_ITERATIONS": secret, "POSTGRES_USER": secret, "POSTGRES_PASSWORD": secret,
		"POSTGRES_DB": secret, "POSTGRES_MAX_CONNECTIONS": secret, "STORAGE_ACCESS_KEY": secret, "STORAGE_SECRET_KEY": secret,
		"STORAGE_BUCKET": secret, "STORAGE_MAX_FILE_SIZE": secret, "SMTP_FROM": secret, "SMTP_PORT": secret, "SMS_SENDER": secret,
		"SMS_SECURED": secret, "BROADCAST_APP_ID": secret, "BROADCAST_APP_KEY": secret, "BROADCAST_APP_SECRET": secret,
		"BROADCAST_MAX_PAYLOAD_KB": secret, "PDF_ENGINE_API_BEARER_TOKEN": secret, "CACHE_PASSWORD": secret,
	}
	var errs []error
	collect := func(_ any, err error) { errs = append(errs, err) }
	collect(security.FromEnv(look(bad)))
	collect(sql.FromEnv(look(bad)))
	collect(storage.FromEnv(look(bad)))
	collect(smtp.FromEnv(look(bad)))
	collect(sms.FromEnv(look(bad)))
	collect(broadcast.FromEnv(look(bad)))
	collect(pdf.FromEnv(look(bad)))
	collect(cache.FromEnv(look(bad)))
	for _, err := range errs {
		if err != nil && strings.Contains(err.Error(), secret) {
			t.Errorf("a value reached an error: %v", err)
		}
	}
}
