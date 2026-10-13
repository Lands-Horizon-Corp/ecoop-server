package pagination

import "github.com/uptrace/bun"

func (c *PaginationService[TData, TID]) checkReady() error {
	if c.ReadSQLService == nil {
		return ErrReadServiceRequired
	}
	if c.ReadSQLService.Client() == nil {
		return ErrReadDBNotInitialized
	}
	if c.ColumnDefaultID == "" {
		return ErrColumnDefaultIDMissing
	}
	return nil
}

func CheckDB(db bun.IDB, notInit error) error {
	switch v := db.(type) {
	case nil:
		return notInit
	case *bun.DB:
		if v == nil {
			return notInit
		}
	case bun.Tx:
		if v.Tx == nil {
			return ErrNilTx
		}
	case *bun.Tx:
		if v == nil || v.Tx == nil {
			return ErrNilTx
		}
	}
	return nil
}
