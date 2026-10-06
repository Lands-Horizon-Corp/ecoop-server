package sql

import (
	"context"
	"fmt"

	"github.com/pressly/goose/v3"
	"go.opentelemetry.io/otel/attribute"
)

func (s *SQLService) Rollback(ctx context.Context) error {
	return s.observe("sql.rollback", func() error { return s.rollback(ctx) })
}

func (s *SQLService) rollback(ctx context.Context) error {
	migrator, err := s.provider()
	if err != nil {
		return err
	}
	if _, err := migrator.Down(ctx); err != nil {
		return fmt.Errorf("failed to roll back migration: %w", err)
	}
	return nil
}

func (s *SQLService) RollbackTo(ctx context.Context, version int64) error {
	return s.observe("sql.rollback_to", func() error { return s.rollbackTo(ctx, version) }, attribute.Int64("db.migration.version", version))
}

func (s *SQLService) rollbackTo(ctx context.Context, version int64) error {
	migrator, err := s.provider()
	if err != nil {
		return err
	}
	if _, err := migrator.DownTo(ctx, version); err != nil {
		return fmt.Errorf("failed to roll back to version %d: %w", version, err)
	}
	return nil
}

func (s *SQLService) Redo(ctx context.Context) error {
	return s.observe("sql.redo", func() error { return s.redo(ctx) })
}

func (s *SQLService) redo(ctx context.Context) error {
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

func (s *SQLService) RollbackSteps(ctx context.Context, steps int) error {
	return s.observe("sql.rollback_steps", func() error { return s.rollbackSteps(ctx, steps) }, attribute.Int("db.migration.steps", steps))
}

func (s *SQLService) rollbackSteps(ctx context.Context, steps int) error {
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

func (s *SQLService) UpSteps(ctx context.Context, steps int) error {
	return s.observe("sql.up_steps", func() error {
		return s.upSteps(ctx, steps)
	}, attribute.Int("db.migration.steps", steps))
}

func (s *SQLService) upSteps(ctx context.Context, steps int) error {
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
