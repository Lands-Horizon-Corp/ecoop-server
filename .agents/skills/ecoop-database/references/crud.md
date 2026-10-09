# CRUD end to end

A complete model, from struct tags to HTTP handlers. Every pattern here is executed by
`test/regressions/service.bankdb_28_crud_cookbook_test.go`; keep the two in step.

## Contents

- 1. The model
- 2. Request and response DTOs
- 3. Registration
- 4. Create
- 5. Read one
- 6. List
- 7. Update: PATCH, PUT, counters
- 8. Optimistic concurrency
- 9. Delete: soft and hard
- 10. Bulk operations
- 11. Errors to HTTP status
- 12. Hertz handlers
- Checklist for a new model

## 1. The model

```go
type Member struct {
	bun.BaseModel `bun:"table:members"`
	ID        string    `bun:"id,pk" json:"id"`
	Name      string    `bun:"name,notnull" json:"name" validate:"required,max=120"`
	Email     string    `bun:"email,notnull" json:"email" validate:"required,email"`
	Tier      string    `bun:"tier,nullzero,notnull,default:'basic'" json:"tier" validate:"omitempty,oneof=basic silver gold"`
	Version   int64     `bun:"version,nullzero,notnull,default:1" json:"version"`
	UpdatedAt time.Time `bun:"updated_at,nullzero,notnull,default:current_timestamp" json:"updated_at"`
	DeletedAt time.Time `bun:"deleted_at,soft_delete,nullzero" json:"deleted_at"`
}
```

| Tag | Why |
|---|---|
| `bun:"id,pk"` | primary key; the default `ColumnDefaultID` is `id` |
| explicit column names | the CDC decoder maps Debezium columns by bun name; always name columns |
| `nullzero,default:…` | the zero value means "use the database default", returned by `RETURNING *` |
| `updated_at` | the default sort (`updated_at DESC`) and a natural version |
| `version` | optimistic concurrency and `ColumnVersion` for the read model |
| `soft_delete,nullzero` | `DeleteByID` stamps `deleted_at`; every service read hides the row |
| `validate:"…"` | checked on `Create*` / `Update*` before SQL |
| JSONB | `bun:"meta,type:jsonb,nullzero"` (without `nullzero` a nil map is JSON `null`) |
| money | `int64` minor units (`bigint`), never `float64` |

Mirror the table in a goose migration with the constraints the model implies (`NOT NULL`,
`UNIQUE (email)`, `CHECK`s, foreign keys). The database is the last line of defence.

## 2. Request and response DTOs

```go
type CreateMemberRequest struct {
	ID    string `json:"id" validate:"required"`
	Name  string `json:"name" validate:"required,max=120"`
	Email string `json:"email" validate:"required,email"`
}

// PATCH body: a nil pointer means "leave unchanged".
type PatchMemberRequest struct {
	Name  *string `json:"name" validate:"omitempty,max=120"`
	Email *string `json:"email" validate:"omitempty,email"`
	Tier  *string `json:"tier" validate:"omitempty,oneof=basic silver gold"`
}

type MemberResponse struct {
	ID, Name, Email, Tier string
	Version               int64
}
```

Never return the model itself from an API: the response type decides what leaves the service.

## 3. Registration

```go
database.Register(db, database.Registration[Member, MemberResponse, CreateMemberRequest, string]{
	Channel:       "cqrs.public.members",
	ColumnVersion: "version",
	ToResource: func(m *Member) *MemberResponse {
		return &MemberResponse{ID: m.ID, Name: m.Name, Email: m.Email, Tier: m.Tier, Version: m.Version}
	},
	FromRequest: func(r *CreateMemberRequest) *Member {
		return &Member{ID: r.ID, Name: r.Name, Email: r.Email} // server-owned fields stay zero → defaults
	},
})
members, _ := database.Get[Member, MemberResponse, CreateMemberRequest, string](db) // after Start
```

## 4. Create

```go
res, err := members.CreateWithValidationFormat(ctx, req) // validate DTO → FromRequest → INSERT … RETURNING * → ToResource
```

- Defaults (`tier`, `version`, `updated_at`) come back filled in; return `res` straight away.
- Do **not** follow a create with `GetByID`: that reads the reader, which may not have the row yet.
- Client-chosen IDs make retries safe (a retry is `ErrDuplicate`, not a second row). Server-generated
  IDs need an idempotency key (see writes-and-transactions.md).

## 5. Read one

```go
res, err := members.GetByIDFormat(ctx, id) // reader; ErrNotFound when absent or soft-deleted
```

Read-your-write (right after a change, on the writer, no lock):

```go
err := database.RunInTx(ctx, db, nil, func(ctx context.Context, tx bun.Tx) error {
	res, err = members.GetByIDWithTxFormat(database.WithoutRowLocks(ctx), &tx, id)
	return err
})
```

## 6. List

```go
page, err := members.PaginateFilterFormat(ctx, scope, pagination.Pagination{PageSize: 20, Cursor: cursor, Filter: clientFilter})
// page.Data, page.NextCursor, page.PreviousCursor
```

Scope = what the caller is allowed to see (set in code); client filter = what they asked for.
Details in filters.md. Soft-deleted rows never appear.

## 7. Update

### PATCH (partial)

Lock, apply only the sent fields, bump the version, write back, all in one transaction:

```go
func PatchMember(ctx context.Context, db *database.DatabaseService, members MembersSvc, id string, expectedVersion int64, p PatchMemberRequest) (*MemberResponse, error) {
	if err := validate.StructCtx(ctx, p); err != nil {
		return nil, database.MapError(err) // ErrInvalidInput
	}
	var out *MemberResponse
	err := database.RunInTx(ctx, db, nil, func(ctx context.Context, tx bun.Tx) error {
		m, err := members.GetByIDWithTx(ctx, &tx, id) // FOR UPDATE: concurrent PATCHes queue up
		if err != nil {
			return err
		}
		if expectedVersion > 0 && m.Version != expectedVersion {
			return ErrStaleVersion
		}
		if p.Name != nil {
			m.Name = *p.Name
		}
		if p.Email != nil {
			m.Email = *p.Email
		}
		if p.Tier != nil {
			m.Tier = *p.Tier
		}
		m.Version++
		m.UpdatedAt = time.Now()
		out, err = members.UpdateByIDWithTxFormat(ctx, tx, id, *m)
		return err
	})
	if errors.Is(err, ErrStaleVersion) {
		return nil, err
	}
	return out, database.MapError(err)
}
```

`UpdateByID` writes **every** column of the struct, so it must always receive a fully loaded row.
Never call it with a struct built from a partial request.

### PUT (full replace) — use with care

`members.UpdateByIDWithValidationFormat(ctx, id, req)` validates the request, builds the row with
`FromRequest` and writes **every** column. Columns the request does not carry are reset to their
database defaults: `tier` back to `basic`, and `version` back to `1`. A version that goes backwards
breaks `ColumnVersion` on the read model, which keeps ignoring the row's changes until the version
climbs past the old value. Use it only for models without server-owned fields; otherwise implement
PUT like the PATCH above, with every field set.

### Counters

`members.IncrementByID(ctx, id, "points", 10)`: one atomic statement, no lost updates, no transaction
needed.

## 8. Optimistic concurrency

The client sends the `version` it loaded. The PATCH above refuses the write when the row has moved on
(`ErrStaleVersion` → HTTP 409), so a teller can't silently overwrite another teller's edit. The lock
keeps the check and the write atomic. Without an `expectedVersion`, the last writer wins.

## 9. Delete

- Soft (recommended for anything auditable): give the model `deleted_at,soft_delete`. `DeleteByID` and
  `DeleteMany` stamp it; reads, counts and lists exclude the row; CDC replicates it as an update. A
  second delete is `ErrNotFound`.
- Hard: models without a soft-delete column are really deleted. A row still referenced by a foreign
  key fails with `ErrForeignKey`, mapped to 409.
- Restoring a soft-deleted row means writing `deleted_at` back to NULL with your own query
  (`tx.NewUpdate().Model(&m).WhereAllWithDeleted()...`), not through `UpdateByID`.

## 10. Bulk operations

```go
created, err := members.CreateMany(ctx, rows)      // one INSERT; every row validated first
updated, err := members.UpdateMany(ctx, fullRows)  // one bulk UPDATE by primary key; pass fully loaded rows
err = members.DeleteMany(ctx, ids)                 // one DELETE (or soft delete) … WHERE id IN (…)
```

- A batch is all-or-nothing: one invalid row rejects the whole statement and nothing is written.
- `…WithTx` variants join an existing transaction; `CreateManyWithValidation` takes request DTOs.
- Tens of thousands of rows per call are fine. For millions, chunk (for example, 5k per call).

## 11. Errors to HTTP status

```go
func httpStatus(err error) int {
	switch {
	case err == nil:
		return http.StatusOK
	case errors.Is(err, database.ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, database.ErrDuplicate), errors.Is(err, database.ErrForeignKey),
		errors.Is(err, ErrStaleVersion), errors.Is(err, database.ErrSerialization):
		return http.StatusConflict
	case errors.Is(err, database.ErrInvalidInput), errors.Is(err, database.ErrConstraint),
		errors.Is(err, database.ErrOutOfRange):
		return http.StatusBadRequest // includes validation failures and ErrInvalidEncoding
	case errors.Is(err, database.ErrForbidden):
		return http.StatusForbidden
	case errors.Is(err, database.ErrTimeout), errors.Is(err, database.ErrUnavailable):
		return http.StatusServiceUnavailable
	}
	return http.StatusInternalServerError
}
```

Always `database.MapError(err)` first. For 400s, `errors.As(err, &validator.ValidationErrors{})` gives
the failing fields (`fe.Field()`, `fe.Tag()`) for the response body. Log with
`m.SafeFields()`, never with `err` itself.

## 12. Hertz handlers

```go
func (h *MemberHandler) Create(ctx context.Context, c *app.RequestContext) {
	var req CreateMemberRequest
	if err := c.Bind(&req); err != nil {
		c.JSON(http.StatusBadRequest, errorBody(err))
		return
	}
	res, err := h.members.CreateWithValidationFormat(ctx, req)
	if err = database.MapError(err); err != nil {
		c.JSON(httpStatus(err), errorBody(err))
		return
	}
	c.JSON(http.StatusCreated, res)
}

func (h *MemberHandler) List(ctx context.Context, c *app.RequestContext) {
	scope := pagination.StructuredFilter{} // e.g. this branch's members only
	page, err := h.members.PaginateWithHertzFormat(ctx, nil, scope, c) // pageSize, cursor, filter, sort params
	if err = database.MapError(err); err != nil {
		c.JSON(httpStatus(err), errorBody(err))
		return
	}
	c.JSON(http.StatusOK, page)
}
```

Put the tenant in the request context once, in middleware (`database.WithTenant(ctx, branchID)`), so
every handler is confined without thinking about it.

## Checklist for a new model

1. A migration for the table, with constraints, indexes for your list sorts (`(sort cols…, id)`), and
   the RLS policy if the data belongs to a branch.
2. A model with explicit bun column names, `updated_at`, `version` and soft delete when auditable.
3. Create, patch and response DTOs with `validate` tags.
4. `database.Register(...)` with `ColumnVersion`, `ToResource` and `FromRequest`, before `Start`.
5. Handlers that map errors with `MapError` + `httpStatus` and log `SafeFields()`.
6. A test on the bank harness (`service.bankdb_*`) for the model's rules.
