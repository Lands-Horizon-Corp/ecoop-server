package sql

import (
	"context"
	"fmt"

	"github.com/pressly/goose/v3"
)

// Rollback reverts the most recently applied migration.
func (s *SQLService) Rollback(ctx context.Context) error {
	migrator, err := s.provider()
	if err != nil {
		return err
	}
	if _, err := migrator.Down(ctx); err != nil {
		return fmt.Errorf("failed to roll back migration: %w", err)
	}
	return nil
}

// RollbackTo reverts every applied migration newer than version, leaving the database at version.
func (s *SQLService) RollbackTo(ctx context.Context, version int64) error {
	migrator, err := s.provider()
	if err != nil {
		return err
	}
	if _, err := migrator.DownTo(ctx, version); err != nil {
		return fmt.Errorf("failed to roll back to version %d: %w", version, err)
	}
	return nil
}

// Redo reverts the most recently applied migration and applies it again.
func (s *SQLService) Redo(ctx context.Context) error {
	migrator, err := s.provider()
	if err != nil {
		return err
	}
	if _, err := migrator.Down(ctx); err != nil {
		return fmt.Errorf("failed to roll back migration for redo: %w", err)
	}
	if _, err := migrator.UpByOne(ctx); err != nil {
		return fmt.Errorf("failed to re-apply migration for redo: %w", err)
	}
	return nil
}

// RollbackSteps reverts the most recent steps migrations, one at a time.
func (s *SQLService) RollbackSteps(ctx context.Context, steps int) error {
	if steps <= 0 {
		return fmt.Errorf("%w, got %d", ErrInvalidSteps, steps)
	}
	migrator, err := s.provider()
	if err != nil {
		return err
	}
	if err := requireSteps(ctx, migrator, steps, goose.StateApplied); err != nil {
		return err
	}
	for i := range steps {
		if _, err := migrator.Down(ctx); err != nil {
			return fmt.Errorf("failed to roll back step %d of %d: %w", i+1, steps, err)
		}
	}
	return nil
}

// UpSteps applies the next steps pending migrations, one at a time.
func (s *SQLService) UpSteps(ctx context.Context, steps int) error {
	if steps <= 0 {
		return fmt.Errorf("%w, got %d", ErrInvalidSteps, steps)
	}
	migrator, err := s.provider()
	if err != nil {
		return err
	}
	if err := requireSteps(ctx, migrator, steps, goose.StatePending); err != nil {
		return err
	}
	for i := range steps {
		if _, err := migrator.UpByOne(ctx); err != nil {
			return fmt.Errorf("failed to apply step %d of %d: %w", i+1, steps, err)
		}
	}
	return nil
}

// requireSteps fails before anything runs when fewer than steps migrations are in the given state.
func requireSteps(ctx context.Context, migrator *goose.Provider, steps int, state goose.State) error {
	statuses, err := migrator.Status(ctx)
	if err != nil {
		return fmt.Errorf("failed to get migration status: %w", err)
	}
	available := 0
	for _, st := range statuses {
		if st.State == state {
			available++
		}
	}
	if steps > available {
		return fmt.Errorf("%w: %d requested, %d available", ErrTooManySteps, steps, available)
	}
	return nil
}
