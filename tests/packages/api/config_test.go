package api_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/api"
)

func secured() api.Config {
	return api.Config{
		Secured:        true,
		AllowedOrigins: []string{"https://app.ecoop.test"},
		RateLimit:      10, RateBurst: 20,
	}
}

func env(kv map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := kv[k]; return v, ok }
}

func TestConfig_DefaultsAreFilled(t *testing.T) {
	t.Parallel()
	c := api.Config{}.WithDefaults()
	if c.Addr != api.DefaultAddr || c.MaxBodyBytes != api.DefaultMaxBodyBytes || c.MaxStreams != api.DefaultMaxStreams ||
		c.StreamTimeout != api.DefaultStreamTimeout || c.ReadTimeout != api.DefaultReadTimeout ||
		c.ShutdownTimeout != api.DefaultShutdownTimeout || len(c.Methods) == 0 {
		t.Errorf("defaults missing: %+v", c)
	}
	if c.RateLimit != 0 {
		t.Errorf("a development profile got a rate limit it did not ask for: %v", c.RateLimit)
	}
	if got := (api.Config{Secured: true}).WithDefaults(); got.RateLimit != api.DefaultRateLimit || got.RateBurst != api.DefaultRateBurst {
		t.Errorf("Secured did not get the default limiter: %+v", got)
	}
}

func TestConfig_ValidProfilesPass(t *testing.T) {
	t.Parallel()
	if err := (api.Config{}).Validate(); err != nil {
		t.Errorf("the zero (development) config = %v", err)
	}
	if err := secured().Validate(); err != nil {
		t.Errorf("a proper Secured config = %v", err)
	}
}

func TestConfig_UnsafeSettingsAreRefused(t *testing.T) {
	t.Parallel()
	with := func(f func(*api.Config)) api.Config { c := secured(); f(&c); return c }
	cases := []struct {
		name string
		cfg  api.Config
		want error
	}{
		{"Secured without origins", with(func(c *api.Config) { c.AllowedOrigins = nil }), api.ErrNoOrigins},
		{"http origin when Secured", with(func(c *api.Config) { c.AllowedOrigins = []string{"http://app.ecoop.test"} }), api.ErrInsecureOrigin},
		{"wildcard origin", with(func(c *api.Config) { c.AllowedOrigins = []string{"https://*.ecoop.test"} }), api.ErrInvalidOrigin},
		{"origin with a path", with(func(c *api.Config) { c.AllowedOrigins = []string{"https://app.ecoop.test/x"} }), api.ErrInvalidOrigin},
		{"origin with a trailing slash", with(func(c *api.Config) { c.AllowedOrigins = []string{"https://app.ecoop.test/"} }), api.ErrInvalidOrigin},
		{"origin with credentials", with(func(c *api.Config) { c.AllowedOrigins = []string{"https://u:p@app.ecoop.test"} }), api.ErrInvalidOrigin},
		{"origin that is not a URL", with(func(c *api.Config) { c.AllowedOrigins = []string{"app.ecoop.test"} }), api.ErrInvalidOrigin},
		{"host with a scheme", with(func(c *api.Config) { c.AllowedHosts = []string{"https://api.ecoop.test"} }), api.ErrInvalidHost},
		{"host with a wildcard", with(func(c *api.Config) { c.AllowedHosts = []string{"*.ecoop.test"} }), api.ErrInvalidHost},
		{"host with a bad port", with(func(c *api.Config) { c.AllowedHosts = []string{"api.ecoop.test:99999"} }), api.ErrInvalidHost},
		{"pprof when Secured", with(func(c *api.Config) { c.EnablePprof = true }), api.ErrPprofSecured},
		{"no rate limit when Secured", with(func(c *api.Config) { c.RateLimit, c.RateBurst = -1, 0 }), api.ErrNoRateLimit},
		{"trust every IPv4", with(func(c *api.Config) { c.TrustedProxies = []string{"0.0.0.0/0"} }), api.ErrTrustEveryone},
		{"trust every IPv6", with(func(c *api.Config) { c.TrustedProxies = []string{"::/0"} }), api.ErrTrustEveryone},
		{"a proxy that is not an address", with(func(c *api.Config) { c.TrustedProxies = []string{"my-proxy"} }), api.ErrInvalidProxy},
		{"negative body limit", with(func(c *api.Config) { c.MaxBodyBytes = -1 }), api.ErrInvalidLimit},
		{"negative timeout", with(func(c *api.Config) { c.IdleTimeout = -time.Second }), api.ErrInvalidLimit},
		{"negative stream cap", with(func(c *api.Config) { c.MaxStreams = -1 }), api.ErrInvalidLimit},
		{"write shorter than read", with(func(c *api.Config) { c.ReadTimeout, c.WriteTimeout = 20*time.Second, 5*time.Second }), api.ErrWriteBeforeRead},
		{"unknown method", with(func(c *api.Config) { c.Methods = []string{"GET", "TRACE"} }), api.ErrInvalidMethod},
		{"bad listen address", with(func(c *api.Config) { c.Addr = "no-port" }), api.ErrInvalidAddr},
	}
	for _, tc := range cases {
		err := tc.cfg.Validate()
		if err == nil {
			t.Errorf("%s: accepted", tc.name)
			continue
		}
		if !errors.Is(err, tc.want) {
			t.Errorf("%s: error = %v, want %v", tc.name, err, tc.want)
		}
	}
}

func TestConfig_NaNAndInfiniteRatesAreRefused(t *testing.T) {
	t.Parallel()
	for _, v := range []string{"NaN", "Inf", "+Inf", "-Inf", "abc", "-1"} {
		if _, err := api.FromEnv(env(map[string]string{"API_RATE_LIMIT": v})); !errors.Is(err, api.ErrInvalidLimit) {
			t.Errorf("API_RATE_LIMIT=%s = %v, want ErrInvalidLimit", v, err)
		}
	}
}

func TestConfig_ValidateReportsEveryProblemAtOnce(t *testing.T) {
	t.Parallel()
	err := api.Config{Secured: true, EnablePprof: true, TrustedProxies: []string{"0.0.0.0/0"}, MaxBodyBytes: -1}.Validate()
	for _, want := range []error{api.ErrNoOrigins, api.ErrPprofSecured, api.ErrTrustEveryone, api.ErrInvalidLimit} {
		if !errors.Is(err, want) {
			t.Errorf("one run should report %v, got %v", want, err)
		}
	}
}

func TestConfig_ErrorsNeverEchoALongValue(t *testing.T) {
	t.Parallel()
	long := strings.Repeat("x", 10_000)
	err := api.Config{AllowedOrigins: []string{long}, AllowedHosts: []string{long}, TrustedProxies: []string{long}}.Validate()
	if err == nil || len(err.Error()) > 1000 {
		t.Errorf("an error should be bounded, was %d bytes", len(err.Error()))
	}
}

func TestConfig_OriginsAndHostsAreNormalised(t *testing.T) {
	t.Parallel()
	c := api.Config{
		AllowedOrigins: []string{" HTTPS://App.Ecoop.Test:8443 "},
		AllowedHosts:   []string{"API.Ecoop.Test"},
		TrustedProxies: []string{"10.0.0.1", "10.0.0.0/8", "fd00::/8"},
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("normalisable values refused: %v", err)
	}
}

func TestFromEnv_ReadsEverySetting(t *testing.T) {
	t.Parallel()
	c, err := api.FromEnv(env(map[string]string{
		"API_ADDR":             "127.0.0.1:9000",
		"API_SECURED":          "TRUE",
		"API_ALLOWED_ORIGINS":  "https://a.ecoop.test, https://b.ecoop.test",
		"API_ALLOWED_HOSTS":    "api.ecoop.test",
		"API_TRUSTED_PROXIES":  "10.0.0.0/8,192.168.0.1",
		"API_MAX_BODY_BYTES":   "2048",
		"API_MAX_STREAMS":      "7",
		"API_STREAM_TIMEOUT":   "90s",
		"API_RATE_LIMIT":       "2.5",
		"API_RATE_BURST":       "9",
		"API_READ_TIMEOUT":     "5s",
		"API_WRITE_TIMEOUT":    "6s",
		"API_IDLE_TIMEOUT":     "7s",
		"API_SHUTDOWN_TIMEOUT": "8s",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if c.Addr != "127.0.0.1:9000" || !c.Secured || len(c.AllowedOrigins) != 2 || len(c.AllowedHosts) != 1 ||
		len(c.TrustedProxies) != 2 || c.MaxBodyBytes != 2048 || c.MaxStreams != 7 || c.StreamTimeout != 90*time.Second ||
		c.RateLimit != 2.5 || c.RateBurst != 9 || c.ReadTimeout != 5*time.Second || c.WriteTimeout != 6*time.Second ||
		c.IdleTimeout != 7*time.Second || c.ShutdownTimeout != 8*time.Second {
		t.Errorf("a setting was not read: %+v", c)
	}
	if !c.BlockSuspiciousPaths {
		t.Error("scanner paths are blocked unless turned off")
	}
}

func TestFromEnv_EmptyEnvironmentIsTheDevelopmentProfile(t *testing.T) {
	t.Parallel()
	c, err := api.FromEnv(env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if c.Secured || c.EnablePprof || c.Addr != api.DefaultAddr || !c.BlockSuspiciousPaths {
		t.Errorf("unexpected default profile: %+v", c)
	}
}

func TestFromEnv_MalformedValuesAreErrorsNotDefaults(t *testing.T) {
	t.Parallel()
	for k, v := range map[string]string{
		"API_MAX_BODY_BYTES": "lots", "API_MAX_STREAMS": "-3", "API_RATE_BURST": "1.5",
		"API_READ_TIMEOUT": "fifteen", "API_STREAM_TIMEOUT": "-1s", "API_SHUTDOWN_TIMEOUT": "30",
		"API_ADDR": "8080", "API_TRUSTED_PROXIES": "0.0.0.0/0", "API_ALLOWED_ORIGINS": "not a url",
	} {
		if _, err := api.FromEnv(env(map[string]string{k: v})); err == nil {
			t.Errorf("%s=%q was accepted", k, v)
		}
	}
}

func TestFromEnv_SecuredIsHeldToTheSecuredRules(t *testing.T) {
	t.Parallel()
	if _, err := api.FromEnv(env(map[string]string{"API_SECURED": "true"})); !errors.Is(err, api.ErrNoOrigins) {
		t.Errorf("Secured without origins = %v", err)
	}
	if _, err := api.FromEnv(env(map[string]string{
		"API_SECURED": "true", "API_ALLOWED_ORIGINS": "https://a.ecoop.test", "API_ENABLE_PPROF": "true",
	})); !errors.Is(err, api.ErrPprofSecured) {
		t.Errorf("Secured with pprof = %v", err)
	}
	c, err := api.FromEnv(env(map[string]string{"API_SECURED": "true", "API_ALLOWED_ORIGINS": "https://a.ecoop.test"}))
	if err != nil {
		t.Fatal(err)
	}
	if c.RateLimit <= 0 {
		t.Error("Secured must come with a rate limit even when none is configured")
	}
}

func TestFromEnv_OnlyExactlyFalseTurnsBlockingOff(t *testing.T) {
	t.Parallel()
	for v, want := range map[string]bool{"false": false, "FALSE": false, "": true, "0": true, "no": true, "true": true} {
		c, err := api.FromEnv(env(map[string]string{"API_BLOCK_SUSPICIOUS": v}))
		if err != nil {
			t.Fatal(err)
		}
		if c.BlockSuspiciousPaths != want {
			t.Errorf("API_BLOCK_SUSPICIOUS=%q = %v, want %v", v, c.BlockSuspiciousPaths, want)
		}
	}
}
