package security

import (
	"os"

	"go.uber.org/fx"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// Defaults are RFC 9106's second recommended Argon2id profile: 64 MiB of memory,
// three passes, four lanes, a 16-byte salt and a 32-byte hash.
const (
	DefaultMemoryKiB   = 64 * 1024
	DefaultIterations  = 3
	DefaultParallelism = 4
	DefaultSaltBytes   = 16
	DefaultKeyBytes    = 32
)

// Config is how a SecurityService hashes and signs.
type Config struct {
	// Secret is the server-side secret mixed into every hash (a pepper).
	Secret string
	// MemoryKiB, Iterations and Parallelism are the Argon2id cost. SaltBytes and
	// KeyBytes are the lengths of the salt and of the derived hash.
	MemoryKiB   uint32
	Iterations  uint32
	Parallelism uint8
	SaltBytes   uint32
	KeyBytes    uint32
}

// FromEnv builds the Config from the environment:
//
//	APP_SECRET            required
//	SECURITY_MEMORY_KIB   default 65536
//	SECURITY_ITERATIONS   default 3
//	SECURITY_PARALLELISM  default 4 (1 to 255)
//	SECURITY_SALT_BYTES   default 16 (at least 16)
//	SECURITY_KEY_BYTES    default 32 (at least 16)
//
// A cost below the minimums is an error at startup: a weak hash configured by
// mistake would otherwise go unnoticed until the first password is stored.
func FromEnv(lookup func(string) (string, bool)) (Config, error) {
	e := utils.NewEnv(lookup)
	memory := e.Int("SECURITY_MEMORY_KIB", DefaultMemoryKiB)
	iterations := e.Int("SECURITY_ITERATIONS", DefaultIterations)
	parallelism := e.Int("SECURITY_PARALLELISM", DefaultParallelism)
	salt := e.Int("SECURITY_SALT_BYTES", DefaultSaltBytes)
	key := e.Int("SECURITY_KEY_BYTES", DefaultKeyBytes)
	if memory < 8 {
		e.Invalid("SECURITY_MEMORY_KIB", "at least 8")
	}
	if iterations < 1 {
		e.Invalid("SECURITY_ITERATIONS", "at least 1")
	}
	if parallelism < 1 || parallelism > 255 {
		e.Invalid("SECURITY_PARALLELISM", "between 1 and 255")
	}
	if salt < minSaltLength {
		e.Invalid("SECURITY_SALT_BYTES", "at least 16")
	}
	if key < minKeyLength {
		e.Invalid("SECURITY_KEY_BYTES", "at least 16")
	}
	cfg := Config{
		Secret:      e.Required("APP_SECRET"),
		MemoryKiB:   uint32(max(memory, 0)),
		Iterations:  uint32(max(iterations, 0)),
		Parallelism: uint8(min(max(parallelism, 0), 255)),
		SaltBytes:   uint32(max(salt, 0)),
		KeyBytes:    uint32(max(key, 0)),
	}
	return cfg, e.Err()
}

// Module provides ISecurityService. It has no lifecycle: it holds no connection.
var Module = fx.Module("security",
	fx.Provide(
		func() (Config, error) { return FromEnv(os.LookupEnv) },
		New,
	),
)

// Params is what New needs from the application.
type Params struct {
	fx.In

	Config   Config
	Observer observability.IObserver `optional:"true"`
}

// New builds the service from its Config.
func New(p Params) ISecurityService {
	var opts []Option
	if p.Observer != nil {
		opts = append(opts, WithObserver(p.Observer))
	}
	c := p.Config
	return NewSecurityService(c.Secret, c.MemoryKiB, c.Iterations, c.Parallelism, c.SaltBytes, c.KeyBytes, opts...)
}
