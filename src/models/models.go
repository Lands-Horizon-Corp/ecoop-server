package models

// All lists every bun model the application owns. Diff compares exactly this list with the database,
// so a model missing here is generated as DROP TABLE.
func All() []any {
	return []any{
		// (*User)(nil),
	}
}
