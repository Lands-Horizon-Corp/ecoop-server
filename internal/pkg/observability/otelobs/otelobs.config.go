// Package otelobs exports what internal/pkg/observability records through
// OpenTelemetry (OTLP over HTTP): spans, metrics and trace-context propagation.
// It is the one package allowed to import OpenTelemetry; every other package
// observes through the observability contract and cannot tell what sits behind
// it.
//
// Nothing here uses a global. The tracer provider, the meter provider and the
// propagator belong to one Backend, so two Backends in one process (a test, a
// second service) never see each other's data.
package otelobs

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is how to export. FromEnv builds one from the standard OTEL_*
// variables; every field may also be set directly.
type Config struct {
	// Endpoint is the OTLP/HTTP base URL of the collector, for example
	// http://otel-collector:4318. Empty means export nothing (Disabled).
	Endpoint string
	// Headers are sent with every export request (an API key for a hosted
	// backend). Never logged.
	Headers map[string]string
	// Insecure allows http:// endpoints. Without it only https is accepted, so a
	// collector on the internet is never reached in the clear by accident.
	Insecure bool

	// Sampler is one of the OTEL_TRACES_SAMPLER names. Empty is
	// parentbased_always_on.
	Sampler string
	// SamplerArg is the ratio for the *traceidratio samplers, from 0 to 1.
	SamplerArg float64
	// SamplerArgSet is true when SamplerArg was given.
	SamplerArgSet bool

	// ExportTimeout bounds one export request. Default 10s.
	ExportTimeout time.Duration
	// MetricInterval is how often metrics are exported. Default 15s.
	MetricInterval time.Duration
	// BatchTimeout is how long spans wait to be batched. Default 5s.
	BatchTimeout time.Duration
	// MaxQueue is how many spans may wait for export before the oldest new ones
	// are dropped. Telemetry never blocks the application: a full queue drops.
	// Default 2048.
	MaxQueue int
	// MaxBatch is the most spans in one export. Default 512.
	MaxBatch int
	// ShutdownTimeout bounds the flush in Shutdown when its context has no
	// deadline. Default 10s.
	ShutdownTimeout time.Duration
}

// Defaults.
const (
	DefaultExportTimeout   = 10 * time.Second
	DefaultMetricInterval  = 15 * time.Second
	DefaultBatchTimeout    = 5 * time.Second
	DefaultMaxQueue        = 2048
	DefaultMaxBatch        = 512
	DefaultShutdownTimeout = 10 * time.Second
)

// Errors returned by Validate and FromEnv.
var (
	ErrInvalidEndpoint = errors.New("otelobs: invalid OTLP endpoint")
	ErrInsecure        = errors.New("otelobs: the endpoint is http:// but Insecure is not set")
	ErrInvalidSampler  = errors.New("otelobs: invalid sampler")
	ErrInvalidDuration = errors.New("otelobs: invalid duration")
	ErrInvalidNumber   = errors.New("otelobs: invalid number")
)

var samplers = map[string]bool{
	"": true, "always_on": true, "always_off": true, "traceidratio": true,
	"parentbased_always_on": true, "parentbased_always_off": true, "parentbased_traceidratio": true,
}

// Disabled reports whether nothing is to be exported: no endpoint, or
// OTEL_SDK_DISABLED=true. A disabled Config is valid, and Open returns a Backend
// that records locally and exports nothing.
func (c Config) Disabled() bool { return strings.TrimSpace(c.Endpoint) == "" }

// Validate checks the Config without touching the network. It is what stops a
// typo from reaching production quietly: the OpenTelemetry SDK itself only logs
// a bad sampler and then samples everything.
func (c Config) Validate() error {
	var errs []error
	if !c.Disabled() {
		u, err := url.Parse(strings.TrimSpace(c.Endpoint))
		switch {
		case err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https"):
			errs = append(errs, fmt.Errorf("%w: %q (want http(s)://host:port)", ErrInvalidEndpoint, redactURL(c.Endpoint)))
		case u.Scheme == "http" && !c.Insecure:
			errs = append(errs, ErrInsecure)
		}
	}
	if !samplers[strings.ToLower(strings.TrimSpace(c.Sampler))] {
		errs = append(errs, fmt.Errorf("%w: %q", ErrInvalidSampler, c.Sampler))
	}
	if c.SamplerArgSet && (c.SamplerArg < 0 || c.SamplerArg > 1 || c.SamplerArg != c.SamplerArg) {
		errs = append(errs, fmt.Errorf("%w: ratio %v is not between 0 and 1", ErrInvalidSampler, c.SamplerArg))
	}
	for name, d := range map[string]time.Duration{
		"ExportTimeout": c.ExportTimeout, "MetricInterval": c.MetricInterval,
		"BatchTimeout": c.BatchTimeout, "ShutdownTimeout": c.ShutdownTimeout,
	} {
		if d < 0 {
			errs = append(errs, fmt.Errorf("%w: %s is negative", ErrInvalidDuration, name))
		}
	}
	if c.MaxQueue < 0 || c.MaxBatch < 0 {
		errs = append(errs, fmt.Errorf("%w: queue and batch sizes cannot be negative", ErrInvalidNumber))
	}
	if c.MaxBatch > 0 && c.MaxQueue > 0 && c.MaxBatch > c.MaxQueue {
		errs = append(errs, fmt.Errorf("%w: MaxBatch %d is larger than MaxQueue %d", ErrInvalidNumber, c.MaxBatch, c.MaxQueue))
	}
	return errors.Join(errs...)
}

// withDefaults fills what was left zero.
func (c Config) withDefaults() Config {
	if c.ExportTimeout == 0 {
		c.ExportTimeout = DefaultExportTimeout
	}
	if c.MetricInterval == 0 {
		c.MetricInterval = DefaultMetricInterval
	}
	if c.BatchTimeout == 0 {
		c.BatchTimeout = DefaultBatchTimeout
	}
	if c.MaxQueue == 0 {
		c.MaxQueue = DefaultMaxQueue
	}
	if c.MaxBatch == 0 {
		c.MaxBatch = DefaultMaxBatch
	}
	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = DefaultShutdownTimeout
	}
	c.Sampler = strings.ToLower(strings.TrimSpace(c.Sampler))
	if c.Sampler == "" {
		c.Sampler = "parentbased_always_on"
	}
	c.Endpoint = strings.TrimRight(strings.TrimSpace(c.Endpoint), "/")
	return c
}

// FromEnv reads the standard OpenTelemetry variables. lookup is os.LookupEnv in
// production, and a map in a test. It returns an error for a malformed value
// instead of ignoring it.
//
//	OTEL_SDK_DISABLED               "true" exports nothing
//	OTEL_EXPORTER_OTLP_ENDPOINT     base URL of the collector (http://host:4318)
//	OTEL_EXPORTER_OTLP_HEADERS      k=v,k2=v2
//	OTEL_EXPORTER_OTLP_INSECURE     "true" allows an http:// endpoint
//	OTEL_EXPORTER_OTLP_TIMEOUT      export timeout, in milliseconds
//	OTEL_TRACES_SAMPLER             always_on, always_off, traceidratio,
//	                                parentbased_always_on, parentbased_always_off,
//	                                parentbased_traceidratio
//	OTEL_TRACES_SAMPLER_ARG         the ratio, 0 to 1
//	OTEL_METRIC_EXPORT_INTERVAL     metric export interval, in milliseconds
//	OTEL_BSP_SCHEDULE_DELAY         batch delay, in milliseconds
//	OTEL_BSP_MAX_QUEUE_SIZE         spans that may wait for export
//	OTEL_BSP_MAX_EXPORT_BATCH_SIZE  spans per export
func FromEnv(lookup func(string) (string, bool)) (Config, error) {
	if lookup == nil {
		lookup = os.LookupEnv
	}
	get := func(k string) string {
		v, _ := lookup(k)
		return strings.TrimSpace(v)
	}
	var c Config
	var errs []error
	if strings.EqualFold(get("OTEL_SDK_DISABLED"), "true") {
		return c, nil
	}
	c.Endpoint = get("OTEL_EXPORTER_OTLP_ENDPOINT")
	c.Insecure = strings.EqualFold(get("OTEL_EXPORTER_OTLP_INSECURE"), "true")
	if h := get("OTEL_EXPORTER_OTLP_HEADERS"); h != "" {
		hdr, err := parseHeaders(h)
		if err != nil {
			errs = append(errs, err)
		}
		c.Headers = hdr
	}
	c.Sampler = get("OTEL_TRACES_SAMPLER")
	if a := get("OTEL_TRACES_SAMPLER_ARG"); a != "" {
		v, err := strconv.ParseFloat(a, 64)
		if err != nil {
			errs = append(errs, fmt.Errorf("%w: OTEL_TRACES_SAMPLER_ARG %q is not a number", ErrInvalidSampler, a))
		}
		c.SamplerArg, c.SamplerArgSet = v, err == nil
	}
	ms := func(key string, dst *time.Duration) {
		if v := get(key); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < 0 {
				errs = append(errs, fmt.Errorf("%w: %s %q (want milliseconds)", ErrInvalidDuration, key, v))
				return
			}
			*dst = time.Duration(n) * time.Millisecond
		}
	}
	num := func(key string, dst *int) {
		if v := get(key); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < 0 {
				errs = append(errs, fmt.Errorf("%w: %s %q", ErrInvalidNumber, key, v))
				return
			}
			*dst = n
		}
	}
	ms("OTEL_EXPORTER_OTLP_TIMEOUT", &c.ExportTimeout)
	ms("OTEL_METRIC_EXPORT_INTERVAL", &c.MetricInterval)
	ms("OTEL_BSP_SCHEDULE_DELAY", &c.BatchTimeout)
	num("OTEL_BSP_MAX_QUEUE_SIZE", &c.MaxQueue)
	num("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", &c.MaxBatch)
	if err := c.Validate(); err != nil {
		errs = append(errs, err)
	}
	return c, errors.Join(errs...)
}

func parseHeaders(s string) (map[string]string, error) {
	out := map[string]string{}
	var errs []error
	for kv := range strings.SplitSeq(s, ",") {
		kv = strings.TrimSpace(kv)
		if kv == "" {
			continue
		}
		k, v, ok := strings.Cut(kv, "=")
		k = strings.TrimSpace(k)
		if !ok || k == "" {
			// The value of a header is a credential: say which entry is wrong,
			// never what it contains.
			errs = append(errs, fmt.Errorf("%w: OTEL_EXPORTER_OTLP_HEADERS entry %q has no key=value form", ErrInvalidEndpoint, redactEntry(kv)))
			continue
		}
		out[k] = strings.TrimSpace(v)
	}
	return out, errors.Join(errs...)
}

// redactURL removes credentials and the query from a URL before it is put in an
// error, because errors are logged.
func redactURL(raw string) string {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" {
		if len(raw) > 32 {
			return raw[:32] + "..."
		}
		return raw
	}
	return u.Scheme + "://" + u.Host + u.Path
}

func redactEntry(s string) string {
	if len(s) > 4 {
		return s[:4] + "..."
	}
	return s
}
