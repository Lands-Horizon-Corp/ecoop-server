//go:build integration

package cqrs_test

import (
	"context"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cdc"
)

// call hands one batch to the subscribed handler once, whatever is committed,
// and returns the handler's verdict. The handler itself retries what is
// transient, so it returns only when the batch is done or its context ends.
func (f *fakeCDC) call(ctx context.Context, batch []cdc.Record) error {
	f.mu.Lock()
	h := f.handler
	f.mu.Unlock()
	if h == nil {
		return cdc.ErrNotRunning
	}
	f.deliveries.Add(1)
	return h(ctx, batch)
}

// rewindToZero is a consumer restart in which Kafka had committed nothing: the
// next delivery starts every partition from the beginning of its log.
func (f *fakeCDC) rewindToZero(w *world) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for part := range w.parts {
		f.committed[part] = 0
	}
}

// setCommitted records that Kafka now resumes this partition from next.
func (f *fakeCDC) setCommitted(partition int32, next int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.committed[partition] = next
}
