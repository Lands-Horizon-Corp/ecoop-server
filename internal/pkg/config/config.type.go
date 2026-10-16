package config

import (
	"context"
	"encoding"
	"errors"
	"reflect"
	"time"
)

var (
	ErrNotStruct    = errors.New("config: target must be a struct")
	ErrNotLoaded    = errors.New("config: not loaded")
	ErrMissing      = errors.New("config: missing required environment variable")
	ErrInvalidValue = errors.New("config: invalid field value")
	ErrNoSecurity   = errors.New("config: needs a security service to encrypt values in memory")

	textUnmarshaler = reflect.TypeFor[encoding.TextUnmarshaler]()
	durationType    = reflect.TypeFor[time.Duration]()
	timeType        = reflect.TypeFor[time.Time]()
)

const (
	EnvFile     = ".env"
	HelmFile    = "helm.yaml"
	SecretsFile = "secrets.yml"

	DefaultReloadInterval = 1 * time.Minute
)

type (
	IConfigService[T any] interface {
		Start(ctx context.Context) error
		Stop(ctx context.Context) error
		Config() T
	}
)
