package sql

import (
	"context"
	"fmt"

	"github.com/pressly/goose/v3"
)

func (s *SQLService) Migrate(ctx context.Context) error {
	migrator, err := s.provider()
	if err != nil {
		return err
	}
	if _, err := migrator.Up(ctx); err != nil {
		return fmt.Errorf("failed to migrate up: %w", err)
	}
	return nil
}

func (s *SQLService) Fresh(ctx context.Context) error {
	migrator, err := s.provider()
	if err != nil {
		return err
	}
	if _, err := migrator.DownTo(ctx, 0); err != nil {
		return fmt.Errorf("failed to roll back all migrations: %w", err)
	}
	if _, err := migrator.Up(ctx); err != nil {
		return fmt.Errorf("failed to re-apply migrations: %w", err)
	}
	return nil
}

func (s *SQLService) Create(ctx context.Context, name string) error {
	if err := goose.Create(nil, migrationsDir, name, "sql"); err != nil {
		return fmt.Errorf("failed to create migration %q: %w", name, err)
	}
	return nil
}
