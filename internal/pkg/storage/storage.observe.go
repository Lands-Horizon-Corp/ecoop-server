package storage

import (
	"context"
	"errors"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/observability"
)

// WithObserver gives the service an observer for its metrics, traces and logs.
// Without one it observes nothing. Only the operation name and its outcome are
// ever recorded: never a file name, a storage key, a bucket, a URL or a size.
func WithObserver(o observability.IObserver) Option {
	return func(s *StorageService) { s.obs = observability.Pick(o) }
}

// begin starts the Operation of one call. A service built as a bare struct
// literal has no observer, so it falls back to Nop without writing.
func (s *StorageService) begin(ctx context.Context, name string) (context.Context, observability.Operation) {
	if s.obs == nil {
		return observability.Nop().Begin(ctx, observability.ComponentStorage, name)
	}
	return s.obs.Begin(ctx, observability.ComponentStorage, name)
}

// finish ends op for an upload. A file the caller sent that is empty or too
// large is an answer to the caller, not a fault of the service, so it ends as
// skipped and never raises the error rate.
func finish(op observability.Operation, err error) {
	if errors.Is(err, ErrEmptyFile) || errors.Is(err, ErrTooLarge) {
		op.SetOutcome(observability.OutcomeSkipped)
		op.End(nil)
		return
	}
	op.End(err)
}
