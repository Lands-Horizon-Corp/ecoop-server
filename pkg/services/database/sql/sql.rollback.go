package sql

import "context"

func (s *SQLService) Rollback(ctx context.Context) error {
	panic("unimplemented")
}

func (s *SQLService) RollbackTo(ctx context.Context, version int64) error {
	panic("unimplemented")
}

func (s *SQLService) Redo(ctx context.Context) error {
	panic("unimplemented")
}

func (s *SQLService) RollbackSteps(ctx context.Context, steps int) error {
	panic("unimplemented")
}

func (s *SQLService) UpSteps(ctx context.Context, steps int) error {
	panic("unimplemented")
}
