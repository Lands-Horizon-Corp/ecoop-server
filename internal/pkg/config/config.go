package config

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/joho/godotenv"
)

type ConfigService[T any] struct {
	sealed atomic.Pointer[[]byte]
	key    []byte

	ReloadInterval time.Duration
	OnReload       func(error)

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

func NewConfigService[T any]() (*ConfigService[T], error) {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("failed to generate encryption key: %w", err)
	}
	return &ConfigService[T]{
		key: key,
	}, nil
}

func (c *ConfigService[T]) Start(ctx context.Context) error {
	return c.load()
}

func (c *ConfigService[T]) Run(ctx context.Context) error {
	if c == nil || c.key == nil {
		return ErrNotLoaded
	}
	if c.sealed.Load() == nil {
		return ErrNotLoaded
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cancel != nil {
		return nil
	}
	interval := c.ReloadInterval
	if interval <= 0 {
		interval = DefaultReloadInterval
	}
	ctx, c.cancel = context.WithCancel(ctx)
	c.done = make(chan struct{})
	last := fingerprint()
	go func(done chan struct{}) {
		defer close(done)
		tick := time.NewTicker(interval)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			if now := fingerprint(); now != last {
				last = now
				err := c.load()
				if c.OnReload != nil {
					c.OnReload(err)
				}
			}
		}
	}(c.done)
	return nil
}

func (c *ConfigService[T]) Stop(ctx context.Context) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	cancel, done := c.cancel, c.done
	c.cancel, c.done = nil, nil
	c.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func fingerprint() [sha256.Size]byte {
	h := sha256.New()
	for _, f := range []string{EnvFile, HelmFile, SecretsFile} {
		b, err := os.ReadFile(f)
		fmt.Fprintf(h, "%s|%t|%d|", f, err == nil, len(b))
		h.Write(b)
	}
	var sum [sha256.Size]byte
	h.Sum(sum[:0])
	return sum
}

func (c *ConfigService[T]) load() error {
	if c == nil {
		return ErrNotLoaded
	}
	if reflect.TypeFor[T]().Kind() != reflect.Struct {
		return fmt.Errorf("%w: %s", ErrNotStruct, reflect.TypeFor[T]())
	}
	mergedFiles := make(map[string]string)
	values, err := godotenv.Read(EnvFile)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("reading env file %s: %w", EnvFile, err)
	}
	maps.Copy(mergedFiles, values)
	for _, f := range []string{HelmFile, SecretsFile} {
		values, err := readYAML(f)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("reading yaml file %s: %w", f, err)
		}
		maps.Copy(mergedFiles, values)
	}

	lookup := func(key string) (string, bool) {
		if v, ok := os.LookupEnv(key); ok && v != "" {
			return v, true
		}
		v, ok := mergedFiles[key]
		return v, ok && v != ""
	}
	var cfg T
	errs, _ := fillStruct(reflect.ValueOf(&cfg).Elem(), "", lookup)
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	plain, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("encoding config: %w", err)
	}
	sealed, err := c.encrypt(plain)
	clear(plain)
	if err != nil {
		return fmt.Errorf("encrypting config: %w", err)
	}

	c.sealed.Store(&sealed)
	return nil
}

func (c *ConfigService[T]) Get(ctx context.Context) (T, error) {
	var zero T
	if c == nil {
		return zero, ErrNotLoaded
	}
	ptr := c.sealed.Load()
	if ptr == nil {
		return zero, fmt.Errorf("%w: service not started", ErrNotLoaded)
	}
	plainBytes, err := c.decrypt(*ptr)
	if err != nil {
		return zero, fmt.Errorf("decrypting config failed: %w", err)
	}
	defer clear(plainBytes)
	var cfg T
	if err := json.Unmarshal(plainBytes, &cfg); err != nil {
		return zero, fmt.Errorf("decoding config failed: %w", err)
	}
	return cfg, nil
}

func (c *ConfigService[T]) encrypt(plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(c.key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

func (c *ConfigService[T]) decrypt(ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(c.key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(ciphertext) < gcm.NonceSize() {
		return nil, errors.New("ciphertext too short")
	}
	nonce, sealed := ciphertext[:gcm.NonceSize()], ciphertext[gcm.NonceSize():]
	return gcm.Open(nil, nonce, sealed, nil)
}
