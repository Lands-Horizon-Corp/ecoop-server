package cqrs

import (
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/uptrace/bun/schema"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

var timeType = reflect.TypeFor[time.Time]()

// model is what replication needs to know about TData, read once from its bun
// layout: which column is the key, which one orders versions, and how every
// column is typed. Building it validates the model, so a model replication
// cannot handle fails when the service is built, not on its first change.
type model[TData any] struct {
	schema    string // Debezium topic schema ("public" unless the table tag names one)
	name      string // table name without schema
	qualified string // the table as bun writes it, quoted: "schema"."name"
	tableKey  string // schema.name: how the row-state table tells tables apart

	idCol   string
	idField *schema.Field

	verCol   string
	verField *schema.Field // nil when versioning is off

	cols      []string        // every mapped column, sorted: the order rows are written in
	colFields []*schema.Field // the fields of cols, in the same order
	setCols   []string        // the columns an upsert overwrites: all but the key
	fields    map[string]*schema.Field
}

func newModel[TData any](idCol, verCol string) (*model[TData], error) {
	tbl := utils.BunTable[TData]()
	if tbl == nil {
		return nil, fmt.Errorf("%w: %s", ErrModelNotStruct, reflect.TypeFor[TData]())
	}
	idf := tbl.LookupField(idCol)
	if idf == nil {
		return nil, fmt.Errorf("%w: %s has no column %q", ErrNoPrimaryKey, tbl.TypeName, idCol)
	}
	switch {
	case len(tbl.PKs) > 1:
		return nil, fmt.Errorf("%w: %s has a composite primary key", ErrNoPrimaryKey, tbl.TypeName)
	case len(tbl.PKs) == 1 && tbl.PKs[0].Name != idf.Name:
		return nil, fmt.Errorf("%w: %s is keyed by %q, not %q", ErrNoPrimaryKey, tbl.TypeName, tbl.PKs[0].Name, idCol)
	}

	m := &model[TData]{
		schema:    tbl.Schema,
		name:      strings.TrimPrefix(tbl.Name, tbl.Schema+"."),
		qualified: string(tbl.SQLName),
		idCol:     idf.Name,
		idField:   idf,
		fields:    make(map[string]*schema.Field, len(tbl.Fields)),
	}
	if m.schema == "" {
		m.schema = defaultSchema
	}
	m.tableKey = m.schema + "." + m.name
	for _, f := range tbl.Fields {
		if err := checkColumn(tbl, f); err != nil {
			return nil, err
		}
		m.fields[f.Name] = f
		m.cols = append(m.cols, f.Name)
		if f.Name != idf.Name {
			m.setCols = append(m.setCols, f.Name)
		}
	}
	slices.Sort(m.cols)
	slices.Sort(m.setCols)
	m.colFields = make([]*schema.Field, len(m.cols))
	for i, name := range m.cols {
		m.colFields[i] = m.fields[name]
	}

	if verCol != "" {
		vf := tbl.LookupField(verCol)
		switch {
		case vf == nil:
			return nil, fmt.Errorf("%w: %s has no version column %q", ErrUnsupportedColumn, tbl.TypeName, verCol)
		case !orderable(vf.IndirectType):
			return nil, fmt.Errorf("%w: version column %q is a %s; it must be an integer or time.Time",
				ErrUnsupportedColumn, verCol, vf.IndirectType)
		}
		m.verCol, m.verField = vf.Name, vf
	}
	return m, nil
}

// checkColumn refuses column types whose Debezium representation depends on a
// precision the consumer cannot see, so a value would be silently misread.
func checkColumn(tbl *schema.Table, f *schema.Field) error {
	if f.Append == nil || f.Scan == nil {
		return fmt.Errorf("%w: %s.%s (%s) cannot be read or written by bun",
			ErrUnsupportedColumn, tbl.TypeName, f.Name, f.IndirectType)
	}
	if f.Tag.HasOption("array") || strings.HasSuffix(strings.TrimSpace(f.CreateTableSQLType), "[]") {
		return fmt.Errorf("%w: %s.%s is an array; Debezium sends it as a JSON array, which cannot be "+
			"written back as a Postgres array (store it as jsonb)", ErrUnsupportedColumn, tbl.TypeName, f.Name)
	}
	if ambiguousTemporal(f.CreateTableSQLType) {
		return fmt.Errorf("%w: %s.%s is %s; use timestamptz (a plain timestamp, time or interval reaches the "+
			"read model as a number whose unit depends on the column's precision)",
			ErrUnsupportedColumn, tbl.TypeName, f.Name, f.CreateTableSQLType)
	}
	return nil
}

// ambiguousTemporal reports whether sqlType is a timestamp without time zone,
// a time of day or an interval: Debezium sends these as integers in a unit
// (milliseconds or microseconds) chosen by the column's precision.
func ambiguousTemporal(sqlType string) bool {
	t := strings.ToLower(strings.TrimSpace(sqlType))
	if strings.Contains(t, "timestamptz") || strings.Contains(t, "timetz") ||
		(strings.Contains(t, "with time zone") && !strings.Contains(t, "without")) {
		return false
	}
	base, _, _ := strings.Cut(t, "(")
	switch strings.TrimSpace(base) {
	case "timestamp", "timestamp without time zone", "time", "time without time zone", "interval":
		return true
	}
	return false
}

// orderable reports whether a type can be a version: values that only grow.
func orderable(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return true
	}
	return t == timeType
}

// peek is the value at idx inside v, or false when a pointer on the way there
// is nil. Unlike bun's own accessor it never allocates, so reading a model
// whose embedded struct is a nil pointer neither panics nor changes it.
func peek(v reflect.Value, idx []int) (reflect.Value, bool) {
	for _, i := range idx {
		for v.Kind() == reflect.Pointer {
			if v.IsNil() {
				return reflect.Value{}, false
			}
			v = v.Elem()
		}
		v = v.Field(i)
	}
	return v, true
}

// identify is the primary key of data as a map key and as a query argument.
// A row without a key (the key is the zero value, or sits behind a nil
// pointer) cannot be replicated: it would write a row with an empty key.
func (m *model[TData]) identify(data *TData) (key string, id any, err error) {
	if data == nil {
		return "", nil, fmt.Errorf("%w: nil row", ErrMalformedChange)
	}
	v, ok := peek(reflect.ValueOf(data).Elem(), m.idField.Index)
	if !ok || v.IsZero() || !v.CanInterface() {
		return "", nil, fmt.Errorf("%w: the row has no %q", ErrMalformedChange, m.idCol)
	}
	return keyString(v), v.Interface(), nil
}

// keyString renders a key value as a stable string for grouping.
func keyString(v reflect.Value) string {
	for v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface {
		if v.IsNil() {
			return ""
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.String:
		return v.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(v.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(v.Uint(), 10)
	}
	if v.CanInterface() {
		if t, ok := v.Interface().(time.Time); ok {
			return t.UTC().Format(time.RFC3339Nano)
		}
		return fmt.Sprint(v.Interface())
	}
	return ""
}

// version is the row's version as a number, and whether it has one. A nil
// pointer is the oldest version (0): a row that lost its version never
// overwrites one that kept it.
func (m *model[TData]) version(data *TData) (int64, bool) {
	if m.verField == nil || data == nil {
		return 0, false
	}
	v, ok := peek(reflect.ValueOf(data).Elem(), m.verField.Index)
	if !ok {
		return 0, true
	}
	return versionKey(v)
}

// versionKnown reports whether the row says what version it was: versioning is
// on and the version is set (neither nil nor zero).
func (m *model[TData]) versionKnown(data *TData) bool {
	v, ok := m.version(data)
	return ok && v != 0
}

// versionNewer reports whether a carries a strictly newer version than b.
// Equal versions are not newer: the later change of the stream wins.
func (m *model[TData]) versionNewer(a, b *TData) bool {
	va, okA := m.version(a)
	vb, okB := m.version(b)
	return okA && okB && va > vb
}

// versionKey turns an integer or time version into an exactly comparable
// number. Times count microseconds (what Postgres stores), which cannot
// overflow for any date Postgres accepts, unlike nanoseconds.
func versionKey(v reflect.Value) (int64, bool) {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return 0, true
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int(), true
	case reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return int64(v.Uint()), true
	}
	if v.CanInterface() {
		if t, ok := v.Interface().(time.Time); ok {
			if t.IsZero() {
				return 0, true // no time at all is the oldest version
			}
			return t.UnixMicro(), true
		}
	}
	return 0, false
}
