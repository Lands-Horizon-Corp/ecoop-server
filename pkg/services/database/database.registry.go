package database

import (
	"context"
	"fmt"
	"reflect"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/broadcast"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/cqrs"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/pagination"
	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/database/sql"
)

type registration struct {
	name    string
	service any
	build   func(writer, reader sql.SQLServices)
	run     func(ctx context.Context) error
}

type Registration[TData any, TResponse any, TRequest any, TID comparable] struct {
	Channel           broadcast.Channel
	ColumnDefaultID   string
	ColumnDefaultSort string
	ColumnVersion     string // optional; see cqrs.CQRSService.ColumnVersion
	Preloads          []string

	ToResource  func(*TData) *TResponse
	TocCSV      func(*TData) *map[string]any
	FromRequest func(*TRequest) *TData
	Created     func(*TData) broadcast.Events
	Updated     func(*TData) broadcast.Events
	Deleted     func(*TData) broadcast.Events
	Dispatch    func(channel broadcast.Channel, events broadcast.Events, payload *TResponse) error
}

func Register[TData any, TResponse any, TRequest any, TID comparable](
	db *DatabaseService,
	re Registration[TData, TResponse, TRequest, TID],
) error {
	key := reflect.TypeFor[TData]()
	if db == nil {
		return ErrNilService
	}
	if db.started {
		return fmt.Errorf("%w: %s", ErrAlreadyStarted, key)
	}
	if _, exists := db.registry[key]; exists {
		return fmt.Errorf("%w: %s", ErrAlreadyRegistered, key)
	}

	r := &registration{name: key.String()}
	r.build = func(writer, reader sql.SQLServices) {
		c := cqrs.CQRSService[TData, TResponse, TRequest, TID]{
			Channel:           re.Channel,
			ColumnDefaultID:   re.ColumnDefaultID,
			ColumnDefaultSort: re.ColumnDefaultSort,
			ColumnVersion:     re.ColumnVersion,
			Preloads:          re.Preloads,

			ToResource:  re.ToResource,
			TocCSV:      re.TocCSV,
			FromRequest: re.FromRequest,
			Created:     re.Created,
			Updated:     re.Updated,
			Deleted:     re.Deleted,
			Dispatch:    re.Dispatch,

			ReadSQLService:       reader,
			WriteSQLService:      writer,
			Log:                  db.cqrsLogger,
			BroadcastService:     db.broadcast,
			MessageBrokerService: db.messageBroker,

			PaginationService: pagination.NewPaginationService(pagination.PaginationService[TData, TID]{
				ReadSQLService:    reader,
				WriteSQLService:   writer,
				Log:               db.cqrsLogger,
				ColumnDefaultID:   re.ColumnDefaultID,
				ColumnDefaultSort: re.ColumnDefaultSort,
				Preloads:          re.Preloads,
			}),
			Validator: db.Validator,

			BatchSize:     db.BatchSize,
			FlushInterval: db.FlushInterval,
		}
		if c.Log == nil {
			c.Log = db.writerLogger
		}
		svc := cqrs.NewCQRS(c)
		r.service = svc
		r.run = svc.Run
	}
	db.registry[key] = r
	db.models = append(db.models, (*TData)(nil))
	return nil
}

func Get[TData any, TResponse any, TRequest any, TID comparable](
	db *DatabaseService,
) (cqrs.CQRSServices[TData, TResponse, TRequest, TID], error) {
	key := reflect.TypeFor[TData]()
	if db == nil {
		return nil, ErrNilService
	}
	if !db.started {
		return nil, ErrNotStarted
	}
	r, ok := db.registry[key]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotRegistered, key)
	}
	svc, ok := r.service.(cqrs.CQRSServices[TData, TResponse, TRequest, TID])
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTypeMismatch, key)
	}
	return svc, nil
}
