package sql

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"ariga.io/atlas/sql/migrate"
	"ariga.io/atlas/sql/postgres"
	"ariga.io/atlas/sql/schema"
	"ariga.io/atlas/sql/sqltool"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// planner plans migrations with Atlas from two scratch databases on the
// target's server: folder, the migrations folder replayed by goose, and
// models, the models' tables created by Bun (with whatever indexes and
// checks their AfterCreateTable hooks add). Atlas diffs them, and the
// difference is the next migration. Both are dropped by close.
type planner struct {
	s      *SQLService
	dir    string
	folder *sql.DB
	drv    migrate.Driver // over folder
	want   *schema.Schema // the models' schema
	drops  []func()
}

// newPlanner builds both scratch databases and checks the live database
// (mig) still matches its migrations: a change made by hand is reported as
// ErrDrift rather than planned into a migration.
func (s *SQLService) newPlanner(ctx context.Context, mig *sql.DB, dir string) (_ *planner, err error) {
	p := &planner{s: s, dir: dir}
	defer func() {
		if err != nil {
			p.close()
		}
	}()
	if p.folder, err = p.scratch(ctx, mig); err != nil {
		return nil, err
	}
	if err := upLocked(ctx, p.folder, dir); err != nil {
		return nil, err
	}
	if p.drv, err = postgres.Open(p.folder); err != nil {
		return nil, fmt.Errorf("sql: planning: %w", err)
	}
	if err := p.drift(ctx, mig); err != nil {
		return nil, err
	}
	models, err := p.scratch(ctx, mig)
	if err != nil {
		return nil, err
	}
	if err := p.createModels(ctx, models); err != nil {
		return nil, err
	}
	if p.want, err = inspect(ctx, models, "public"); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *planner) close() {
	for _, drop := range slices.Backward(p.drops) {
		drop()
	}
}

// scratch creates an empty database next to the target and returns a pool
// on it; close drops it.
func (p *planner) scratch(ctx context.Context, mig *sql.DB) (*sql.DB, error) {
	name := "sql_scratch_" + strings.ToLower(rand.Text()[:16])
	if _, err := mig.ExecContext(ctx, `CREATE DATABASE "`+name+`"`); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrScratchDatabase, err)
	}
	p.drops = append(p.drops, func() {
		_, _ = mig.ExecContext(context.WithoutCancel(ctx), `DROP DATABASE IF EXISTS "`+name+`" WITH (FORCE)`)
	})
	u, err := url.Parse(p.s.dsn)
	if err != nil {
		return nil, ErrInvalidDSN
	}
	u.Path = "/" + name
	c, err := connector(u.String(), pgdriver.WithReadTimeout(0))
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(c)
	p.drops = append(p.drops, func() { _ = db.Close() })
	return db, nil
}

// drift compares the live database with the replayed folder.
func (p *planner) drift(ctx context.Context, mig *sql.DB) error {
	var current string
	if err := mig.QueryRowContext(ctx, "SELECT current_schema()").Scan(&current); err != nil {
		return fmt.Errorf("sql: planning: %w", err)
	}
	live, err := inspect(ctx, mig, current)
	if err != nil {
		return err
	}
	folder, err := inspect(ctx, p.folder, "public")
	if err != nil {
		return err
	}
	live.Name = folder.Name
	changes, err := p.drv.SchemaDiff(live, folder)
	if err != nil || len(changes) == 0 {
		return err
	}
	plan, err := p.plan(ctx, changes)
	if err != nil {
		return err
	}
	var fix []string
	for _, c := range plan.Changes {
		fix = append(fix, c.Cmd+";")
	}
	return fmt.Errorf("%w; to match them it would take:\n%s", ErrDrift, strings.Join(fix, "\n"))
}

// createModels creates every model's table with its foreign keys. A table
// whose foreign key names a table not created yet waits for the next round.
func (p *planner) createModels(ctx context.Context, db *sql.DB) error {
	b := bun.NewDB(db, pgdialect.New()) // closed with db
	exts, err := extensions(ctx, p.folder)
	if err != nil {
		return err
	}
	for _, e := range exts {
		if _, err := db.ExecContext(ctx, `CREATE EXTENSION IF NOT EXISTS "`+e+`"`); err != nil {
			return fmt.Errorf("sql: planning: extension %s: %w", e, err)
		}
	}
	pending := slices.Clone(p.s.models)
	for len(pending) > 0 {
		var later []any
		var last error
		for _, m := range pending {
			_, err := b.NewCreateTable().Model(m).WithForeignKeys().Exec(ctx)
			if pg, ok := errors.AsType[pgdriver.Error](err); ok && pg.Field('C') == "42P01" {
				later, last = append(later, m), err
				continue
			}
			if err != nil {
				return fmt.Errorf("%w: %T: %w", ErrInvalidModel, m, err)
			}
		}
		if len(later) == len(pending) {
			return fmt.Errorf("%w: foreign keys form a cycle or name a table no model has: %w", ErrInvalidModel, last)
		}
		pending = later
	}
	return nil
}

// extensions lists the extensions the migrations installed, which the
// models may need.
func extensions(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, "SELECT extname FROM pg_extension WHERE extname <> 'plpgsql' ORDER BY 1")
	if err != nil {
		return nil, fmt.Errorf("sql: planning: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var e string
		if err := rows.Scan(&e); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// inspect reads one schema with Atlas, leaving out goose's table.
func inspect(ctx context.Context, db *sql.DB, name string) (*schema.Schema, error) {
	drv, err := postgres.Open(db)
	if err != nil {
		return nil, fmt.Errorf("sql: planning: %w", err)
	}
	sch, err := drv.InspectSchema(ctx, name, nil)
	if err != nil {
		return nil, fmt.Errorf("sql: planning: inspecting %s: %w", name, err)
	}
	sch.Tables = slices.DeleteFunc(sch.Tables, func(t *schema.Table) bool { return t.Name == GooseTable })
	return sch, nil
}

func (p *planner) plan(ctx context.Context, changes []schema.Change) (*migrate.Plan, error) {
	plan, err := p.drv.PlanChanges(ctx, "auto", changes, func(o *migrate.PlanOptions) {
		none := "" // unqualified, so a file applies in whatever schema the database uses
		o.SchemaQualifier = &none
	})
	if err != nil {
		return nil, fmt.Errorf("sql: planning: %w", err)
	}
	return plan, nil
}

// generate diffs the replayed folder against the models and writes the
// next step as a goose migration, shaped for a live database:
//
//   - A new index on an existing table gets a file of its own, built with
//     CREATE INDEX CONCURRENTLY outside a transaction, so writes go on while
//     it builds. It comes before the rest of the change, so an index it
//     replaces is dropped only once the new one exists; an index on a column
//     this change adds waits for the next pass, after the column.
//   - Every other file runs in one transaction that sets lock_timeout, so a
//     lock it cannot get quickly makes it fail (and be retried) instead of
//     queueing all traffic behind it.
//   - When it drops a table, column, check, foreign key or index that the
//     models do not replace, the file gets ReviewSuffix instead, which goose
//     ignores, so nothing is lost until a person reviews it and renames it.
//
// It returns the file to apply, or "" when there is none.
func (p *planner) generate(ctx context.Context) (string, error) {
	have, err := inspect(ctx, p.folder, "public")
	if err != nil {
		return "", err
	}
	changes, err := p.drv.SchemaDiff(have, p.want)
	if err != nil {
		return "", fmt.Errorf("sql: planning: %w", err)
	}
	if len(changes) == 0 {
		return "", nil
	}
	changes = explicitDrops(have, renames(changes))
	changes = dependencyOrder(convertUsing(changes))
	review := destructive(changes, p.want)
	ready, rest, err := p.concurrentIndexes(ctx, have, changes)
	if err != nil {
		return "", err
	}
	concurrent := len(ready) > 0
	if concurrent {
		changes, review = ready, false
	} else {
		changes = rest
	}
	plan, err := p.plan(ctx, changes)
	if err != nil {
		return "", err
	}
	if err := p.renameSequences(ctx, changes, plan); err != nil {
		return "", err
	}
	if err := p.enumReverses(ctx, plan); err != nil {
		return "", err
	}
	if concurrent {
		for _, c := range plan.Changes {
			c.Cmd = concurrently(c.Cmd)
			if r, ok := c.Reverse.(string); ok {
				c.Reverse = concurrently(r)
			}
		}
	}
	files, err := sqltool.GooseFormatter.Format(plan)
	if err != nil {
		return "", fmt.Errorf("sql: planning: %w", err)
	}
	body := string(files[0].Bytes())
	if concurrent {
		body = "-- +goose NO TRANSACTION\n" + body
	} else {
		timeout := fmt.Sprintf("SET LOCAL lock_timeout = '%dms';\n", MigrationLockTimeout.Milliseconds())
		body = strings.Replace(body, "-- +goose Up\n", "-- +goose Up\n"+timeout, 1)
		body = strings.Replace(body, "-- +goose Down\n", "-- +goose Down\n"+timeout, 1)
	}
	if review {
		if pending, err := pendingReview(p.dir, body); err != nil || pending != "" {
			if pending != "" {
				fmt.Fprintf(p.s.output, "sql: destructive changes already waiting for review in %s\n", pending)
			}
			return "", err
		}
	}
	version, err := nextVersion(p.dir)
	if err != nil {
		return "", err
	}
	name := fmt.Sprintf("%d_auto.sql", version)
	header := "-- Generated by Migrate from the models. Review before committing.\n"
	if review {
		name += ReviewSuffix
		header = "-- Generated by Migrate from the models. It drops data or a safeguard (a table,\n" +
			"-- column, check, foreign key or index), so it is not applied: review it, then\n" +
			"-- rename it to end in .sql to apply it.\n"
	}
	file := filepath.Join(p.dir, name)
	if err := utils.WriteNew(file, header+body); err != nil {
		return "", fmt.Errorf("sql: writing %s: %w", name, err)
	}
	if review {
		fmt.Fprintf(p.s.output, "sql: destructive changes written to %s for review; not applied\n", name)
		return "", nil
	}
	return file, nil
}

var addEnumValue = regexp.MustCompile(`^ALTER TYPE "?([\w]+)"? ADD VALUE`)

// enumReverses writes the Down Atlas leaves empty for ALTER TYPE ... ADD
// VALUE (Postgres cannot remove an enum value), so a rollback restores the
// type instead of silently keeping the value: the type is rebuilt with its
// previous values and every column using it is converted back. Checks and
// indexes on those columns are dropped first and recreated after, since
// they would otherwise still point at the old type. The definitions come
// from the replayed folder, which is the state the Down returns to.
func (p *planner) enumReverses(ctx context.Context, plan *migrate.Plan) error {
	done := map[string]bool{}
	for _, c := range plan.Changes {
		m := addEnumValue.FindStringSubmatch(c.Cmd)
		if m == nil || c.Reverse != nil {
			continue
		}
		typ := m[1]
		if done[typ] {
			c.Reverse = []string{} // the first change of this type reverses them all
			continue
		}
		done[typ] = true
		reverse, err := p.rebuildEnum(ctx, typ)
		if err != nil {
			return err
		}
		c.Reverse = reverse
	}
	return nil
}

func (p *planner) rebuildEnum(ctx context.Context, typ string) ([]string, error) {
	q := func(query string, args ...any) ([][]string, error) {
		rows, err := p.folder.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("sql: planning: enum %s: %w", typ, err)
		}
		defer rows.Close()
		cols, _ := rows.Columns()
		var out [][]string
		for rows.Next() {
			vals := make([]string, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				return nil, err
			}
			out = append(out, vals)
		}
		return out, rows.Err()
	}
	values, err := q(`SELECT quote_literal(enumlabel) FROM pg_enum WHERE enumtypid = to_regtype($1) ORDER BY enumsortorder`, typ)
	if err != nil {
		return nil, err
	}
	columns, err := q(`SELECT quote_ident(c.relname), quote_ident(a.attname), coalesce(pg_get_expr(d.adbin, d.adrelid), '')
  FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid AND c.relkind IN ('r', 'p')
  LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
 WHERE a.atttypid = to_regtype($1) AND NOT a.attisdropped ORDER BY 1, 2`, typ)
	if err != nil {
		return nil, err
	}
	// Checks and plain indexes that use those columns (constraint indexes go
	// with their constraint).
	dependents, err := q(`SELECT DISTINCT kind, name, quote_ident(rel), def FROM (
  SELECT 'constraint' AS kind, quote_ident(con.conname) AS name, cl.relname AS rel, pg_get_constraintdef(con.oid) AS def
    FROM pg_constraint con JOIN pg_class cl ON cl.oid = con.conrelid
   WHERE con.contype IN ('c', 'u', 'x') AND EXISTS (SELECT 1 FROM pg_attribute a
     WHERE a.attrelid = con.conrelid AND a.attnum = ANY (con.conkey) AND a.atttypid = to_regtype($1))
  UNION ALL
  SELECT 'index', quote_ident(ic.relname), tc.relname, pg_get_indexdef(i.indexrelid)
    FROM pg_index i JOIN pg_class ic ON ic.oid = i.indexrelid JOIN pg_class tc ON tc.oid = i.indrelid
   WHERE NOT EXISTS (SELECT 1 FROM pg_constraint con WHERE con.conindid = i.indexrelid)
     AND EXISTS (SELECT 1 FROM pg_depend d JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
       WHERE d.classid = 'pg_class'::regclass AND d.objid = i.indexrelid AND a.atttypid = to_regtype($1))
) x ORDER BY 1, 2`, typ)
	if err != nil {
		return nil, err
	}
	var before, after []string
	for _, d := range dependents {
		kind, name, rel, def := d[0], d[1], d[2], d[3]
		if kind == "constraint" {
			before = append(before, fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s", rel, name))
			after = append(after, fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s", rel, name, def))
		} else {
			before = append(before, "DROP INDEX "+name)
			after = append(after, strings.Replace(def, " ON public.", " ON ", 1))
		}
	}
	quoted := make([]string, len(values))
	for i, v := range values {
		quoted[i] = v[0]
	}
	old := typ + "__old"
	stmts := make([]string, 0, len(before)+len(columns)+len(after)+3)
	stmts = append(stmts, before...)
	stmts = append(stmts,
		fmt.Sprintf("ALTER TYPE %q RENAME TO %q", typ, old),
		fmt.Sprintf("CREATE TYPE %q AS ENUM (%s)", typ, strings.Join(quoted, ", ")))
	for _, col := range columns {
		table, name, def := col[0], col[1], col[2]
		alter := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT, ALTER COLUMN %s TYPE %q USING %s::text::%q", table, name, name, typ, name, typ)
		if def != "" {
			alter += fmt.Sprintf(", ALTER COLUMN %s SET DEFAULT %s", name, strings.ReplaceAll(def, old, typ))
		}
		stmts = append(stmts, alter)
	}
	stmts = append(stmts, fmt.Sprintf("DROP TYPE %q", old))
	return append(stmts, after...), nil
}

// dependencyOrder sorts each table's changes so every one runs after what
// it depends on, in Up and, reversed, in Down: dropped constraints and
// indexes, dropped columns, renames, added columns, changed columns, then
// added or changed constraints and indexes. Atlas keeps the diff's order,
// and its Down reverses it, so "add check, add column" would come back as
// "drop column (taking the check with it), drop check", which fails.
func dependencyOrder(changes []schema.Change) []schema.Change {
	rank := func(c schema.Change) int {
		switch c.(type) {
		case *schema.DropForeignKey, *schema.DropCheck, *schema.DropIndex:
			return 0
		case *schema.DropColumn:
			return 1
		case *schema.RenameColumn, *schema.RenameIndex:
			return 2
		case *schema.AddColumn:
			return 3
		case *schema.AddCheck, *schema.AddIndex, *schema.AddForeignKey,
			*schema.ModifyCheck, *schema.ModifyIndex, *schema.ModifyForeignKey:
			return 5
		}
		return 4
	}
	for _, c := range changes {
		if m, ok := c.(*schema.ModifyTable); ok {
			slices.SortStableFunc(m.Changes, func(a, b schema.Change) int { return rank(a) - rank(b) })
		}
	}
	return changes
}

// convertUsing gives every column type change an explicit USING cast:
// Postgres only converts implicitly between compatible types (text to an
// enum, for one, needs the cast spelled out). A column with a default has
// the default dropped by a statement of its own first, since the old
// default cannot be cast either; the type change then sets the new one.
func convertUsing(changes []schema.Change) []schema.Change {
	var out []schema.Change
	for _, c := range changes {
		m, ok := c.(*schema.ModifyTable)
		if !ok {
			out = append(out, c)
			continue
		}
		var dropDefaults []schema.Change
		for _, ch := range m.Changes {
			mc, ok := ch.(*schema.ModifyColumn)
			if !ok || mc.Change&schema.ChangeType == 0 || mc.To.Type == nil {
				continue
			}
			if typ, err := postgres.FormatType(mc.To.Type.Type); err == nil {
				mc.Extra = append(mc.Extra, &postgres.ConvertUsing{X: fmt.Sprintf("%q::%s", mc.To.Name, typ)})
			}
			if mc.From.Default != nil {
				bare := *mc.From
				bare.Default = nil
				dropDefaults = append(dropDefaults, &schema.ModifyColumn{From: mc.From, To: &bare, Change: schema.ChangeDefault})
				mc.From = &bare
				if mc.To.Default != nil {
					mc.Change |= schema.ChangeDefault
				}
			}
		}
		if len(dropDefaults) > 0 {
			out = append(out, &schema.ModifyTable{T: m.T, Changes: dropDefaults})
		}
		out = append(out, m)
	}
	return out
}

var createIndex = regexp.MustCompile(`^CREATE (UNIQUE )?INDEX `)

// concurrently turns CREATE/DROP INDEX into their CONCURRENTLY forms.
func concurrently(stmt string) string {
	if m := createIndex.FindString(stmt); m != "" {
		return m + "CONCURRENTLY " + stmt[len(m):]
	}
	if strings.HasPrefix(stmt, "DROP INDEX ") {
		return "DROP INDEX CONCURRENTLY " + stmt[len("DROP INDEX "):]
	}
	return stmt
}

// concurrentIndexes splits out the new indexes on tables that already exist:
// ready are those whose columns exist now, built CONCURRENTLY in a file of
// their own; an index on a column this change adds is left out of rest too,
// for the next pass. Indexes Postgres builds as a constraint (UNIQUE from a
// model tag) stay in rest: a constraint cannot be added concurrently.
func (p *planner) concurrentIndexes(ctx context.Context, have *schema.Schema, changes []schema.Change) (ready, rest []schema.Change, err error) {
	for _, c := range changes {
		m, ok := c.(*schema.ModifyTable)
		if !ok {
			rest = append(rest, c)
			continue
		}
		table, _ := have.Table(m.T.Name)
		var keep []schema.Change
		for _, ch := range m.Changes {
			add, ok := ch.(*schema.AddIndex)
			if !ok || table == nil {
				keep = append(keep, ch)
				continue
			}
			plan, err := p.plan(ctx, []schema.Change{&schema.ModifyTable{T: m.T, Changes: []schema.Change{add}}})
			if err != nil {
				return nil, nil, err
			}
			if len(plan.Changes) != 1 || !createIndex.MatchString(plan.Changes[0].Cmd) {
				keep = append(keep, ch)
				continue
			}
			exists := !slices.ContainsFunc(add.I.Parts, func(part *schema.IndexPart) bool {
				_, found := table.Column(columnName(part))
				return part.C != nil && !found
			})
			if exists {
				ready = append(ready, &schema.ModifyTable{T: m.T, Changes: []schema.Change{add}})
			} // else: the next pass, once the column exists
		}
		if len(keep) > 0 {
			rest = append(rest, &schema.ModifyTable{T: m.T, Changes: keep})
		}
	}
	return ready, rest, nil
}

func columnName(p *schema.IndexPart) string {
	if p.C == nil {
		return ""
	}
	return p.C.Name
}

// explicitDrops drops the foreign keys and indexes on a dropped column in a
// change of their own, before it. Postgres drops them with the column, and
// Atlas leaves them out of that change, so its Down would restore the column
// without them; as their own change, Down puts them back after the column.
func explicitDrops(have *schema.Schema, changes []schema.Change) []schema.Change {
	var out []schema.Change
	for _, c := range changes {
		m, ok := c.(*schema.ModifyTable)
		t, found := have, false
		var table *schema.Table
		if ok {
			table, found = t.Table(m.T.Name)
		}
		if !found {
			out = append(out, c)
			continue
		}
		dropped := map[string]bool{}
		for _, ch := range m.Changes {
			if d, ok := ch.(*schema.DropColumn); ok {
				dropped[d.C.Name] = true
			}
		}
		onDropped := func(ch schema.Change) bool {
			switch ch := ch.(type) {
			case *schema.DropForeignKey:
				return slices.ContainsFunc(ch.F.Columns, func(c *schema.Column) bool { return dropped[c.Name] })
			case *schema.DropIndex:
				return slices.ContainsFunc(ch.I.Parts, func(p *schema.IndexPart) bool { return p.C != nil && dropped[p.C.Name] })
			}
			return false
		}
		var extra, rest []schema.Change
		moved := map[string]bool{}
		for _, ch := range m.Changes {
			if onDropped(ch) {
				extra = append(extra, ch)
				if fk, ok := ch.(*schema.DropForeignKey); ok {
					moved["fk:"+fk.F.Symbol] = true
				} else {
					moved["ix:"+ch.(*schema.DropIndex).I.Name] = true
				}
			} else {
				rest = append(rest, ch)
			}
		}
		for _, fk := range table.ForeignKeys {
			if d := (&schema.DropForeignKey{F: fk}); !moved["fk:"+fk.Symbol] && onDropped(d) {
				extra = append(extra, d)
			}
		}
		for _, ix := range table.Indexes {
			if d := (&schema.DropIndex{I: ix}); !moved["ix:"+ix.Name] && onDropped(d) {
				extra = append(extra, d)
			}
		}
		if len(extra) > 0 {
			out = append(out, &schema.ModifyTable{T: m.T, Changes: extra})
			m.Changes = rest
		}
		out = append(out, m)
	}
	return out
}

// renames turns a dropped table or column and an added one of the same shape
// into a rename, which Atlas's library leaves as a drop and an add: a column
// dropped and a column added in one table with the same type, nullability
// and default; a table dropped and a table added with the same columns.
// Only one-to-one matches count; anything ambiguous stays a drop and an add,
// and goes to review.
func renames(changes []schema.Change) []schema.Change {
	for _, c := range changes {
		if m, ok := c.(*schema.ModifyTable); ok {
			m.Changes = pair(m.Changes,
				func(c schema.Change) (string, bool) {
					if d, ok := c.(*schema.DropColumn); ok {
						return columnShape(d.C), true
					}
					return "", false
				},
				func(c schema.Change) (string, bool) {
					if a, ok := c.(*schema.AddColumn); ok {
						return columnShape(a.C), true
					}
					return "", false
				},
				func(d, a schema.Change) schema.Change {
					return &schema.RenameColumn{From: d.(*schema.DropColumn).C, To: a.(*schema.AddColumn).C}
				})
		}
	}
	changes = pair(changes,
		func(c schema.Change) (string, bool) {
			if d, ok := c.(*schema.DropTable); ok {
				return tableShape(d.T), true
			}
			return "", false
		},
		func(c schema.Change) (string, bool) {
			if a, ok := c.(*schema.AddTable); ok {
				return tableShape(a.T), true
			}
			return "", false
		},
		func(d, a schema.Change) schema.Change {
			return &schema.RenameTable{From: d.(*schema.DropTable).T, To: a.(*schema.AddTable).T}
		})
	return followRenames(changes)
}

// followRenames removes foreign key changes that only re-point a key at a
// renamed table: Postgres keys follow the rename by themselves, and the
// change's Down would name the table before it is renamed back.
func followRenames(changes []schema.Change) []schema.Change {
	renamed := map[string]string{}
	for _, c := range changes {
		if r, ok := c.(*schema.RenameTable); ok {
			renamed[r.From.Name] = r.To.Name
		}
	}
	if len(renamed) == 0 {
		return changes
	}
	follows := func(c schema.Change) bool {
		m, ok := c.(*schema.ModifyForeignKey)
		return ok && m.From.RefTable != nil && m.To.RefTable != nil &&
			renamed[m.From.RefTable.Name] == m.To.RefTable.Name && m.From.Symbol == m.To.Symbol
	}
	return slices.DeleteFunc(changes, func(c schema.Change) bool {
		m, ok := c.(*schema.ModifyTable)
		if !ok {
			return false
		}
		m.Changes = slices.DeleteFunc(m.Changes, follows)
		return len(m.Changes) == 0
	})
}

// pair replaces each drop and add with the same shape, when that shape
// matches exactly one of each, by rename(drop, add).
func pair(changes []schema.Change, drop, add func(schema.Change) (string, bool), rename func(d, a schema.Change) schema.Change) []schema.Change {
	shapes := func(match func(schema.Change) (string, bool)) map[string][]int {
		out := map[string][]int{}
		for i, c := range changes {
			if shape, ok := match(c); ok {
				out[shape] = append(out[shape], i)
			}
		}
		return out
	}
	drops, adds := shapes(drop), shapes(add)
	remove := map[int]bool{}
	for shape, d := range drops {
		if a := adds[shape]; len(d) == 1 && len(a) == 1 {
			changes[d[0]] = rename(changes[d[0]], changes[a[0]])
			remove[a[0]] = true
		}
	}
	out := changes[:0]
	for i, c := range changes {
		if !remove[i] {
			out = append(out, c)
		}
	}
	return out
}

func columnShape(c *schema.Column) string {
	var def string
	if x, ok := c.Default.(*schema.RawExpr); ok {
		def = x.X
	} else if x, ok := c.Default.(*schema.Literal); ok {
		def = x.V
	}
	return fmt.Sprintf("%s|%v|%s", typeShape(c.Type), c.Type.Null, def)
}

// typeShape is a column type as SQL, which leaves out a serial's sequence
// (named after its table; a renamed table's sequence is renamed with it).
func typeShape(t *schema.ColumnType) string {
	if t == nil {
		return ""
	}
	if f, err := postgres.FormatType(t.Type); err == nil {
		return f
	}
	return fmt.Sprintf("%T", t.Type)
}

// tableShape is a table's columns: names and shapes, in order.
func tableShape(t *schema.Table) string {
	var b strings.Builder
	for _, c := range t.Columns {
		fmt.Fprintf(&b, "%s:%s;", c.Name, columnShape(c))
	}
	return b.String()
}

func (p *planner) renameSequences(ctx context.Context, changes []schema.Change, plan *migrate.Plan) error {
	for _, c := range changes {
		r, ok := c.(*schema.RenameTable)
		if !ok {
			continue
		}
		if err := p.renameSequenceForTable(ctx, r, plan); err != nil {
			return err
		}
	}
	return nil
}

func (p *planner) renameSequenceForTable(ctx context.Context, r *schema.RenameTable, plan *migrate.Plan) error {
	rows, err := p.folder.QueryContext(ctx, `SELECT s.relname, a.attname FROM pg_depend d JOIN pg_class s ON s.oid = d.objid AND s.relkind = 'S' JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid WHERE d.refobjid = to_regclass($1) ORDER BY 1`, `"`+r.From.Name+`"`)
	if err != nil {
		return fmt.Errorf("sql: planning: sequences of %s: %w", r.From.Name, err)
	}
	defer rows.Close()
	for rows.Next() {
		var seq, col string
		if err := rows.Scan(&seq, &col); err != nil {
			return fmt.Errorf("sql: scanning sequence of %s: %w", r.From.Name, err)
		}
		if seq != r.From.Name+"_"+col+"_seq" {
			continue
		}
		renamed := r.To.Name + "_" + col + "_seq"
		plan.Changes = append(plan.Changes, &migrate.Change{
			Cmd:     fmt.Sprintf(`ALTER SEQUENCE %q RENAME TO %q`, seq, renamed),
			Reverse: fmt.Sprintf(`ALTER SEQUENCE %q RENAME TO %q`, renamed, seq),
			Comment: fmt.Sprintf("rename %q sequence with its table", seq),
		})
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("sql: iterating sequences of %s: %w", r.From.Name, err)
	}
	return nil
}

// destructive reports changes that drop data or a safeguard: a table or a
// column; a check or a foreign key not added back by the same change (as a
// rename or a redefinition does); or an index the models no longer cover. An
// index counts as covered when the models declare another on the same table
// whose columns include all of its own (unique if it was unique), so
// replacing an index with a wider or covering one needs no review. An index
// the models do not declare at all (one written by hand) shows up here too,
// rather than being dropped unnoticed.
func destructive(changes []schema.Change, want *schema.Schema) bool {
	for _, c := range changes {
		switch c := c.(type) {
		case *schema.DropTable:
			return true
		case *schema.ModifyTable:
			var lostFK, lostCheck int
			for _, m := range c.Changes {
				switch m := m.(type) {
				case *schema.DropColumn:
					return true
				case *schema.DropCheck:
					lostCheck++
				case *schema.AddCheck:
					lostCheck--
				case *schema.DropForeignKey:
					lostFK++
				case *schema.AddForeignKey:
					lostFK--
				case *schema.DropIndex:
					if !covered(m.I, want, c.T.Name) {
						return true
					}
				}
			}
			if lostFK > 0 || lostCheck > 0 {
				return true
			}
		}
	}
	return false
}

// covered reports whether the models declare another index on table whose
// columns include all of ix's (and that is unique if ix was).
func covered(ix *schema.Index, want *schema.Schema, table string) bool {
	t, ok := want.Table(table)
	if !ok {
		return false
	}
	var cols []string
	for _, p := range ix.Parts {
		if p.C == nil {
			return false // an expression index is only covered by itself
		}
		cols = append(cols, p.C.Name)
	}
	for _, other := range t.Indexes {
		if other.Name == ix.Name || (ix.Unique && !other.Unique) {
			continue
		}
		var has []string
		for _, p := range other.Parts {
			has = append(has, columnName(p))
		}
		if !slices.ContainsFunc(cols, func(c string) bool { return !slices.Contains(has, c) }) {
			return true
		}
	}
	return false
}
