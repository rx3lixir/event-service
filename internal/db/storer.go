package db

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Интерфейс для абстракции методов базы данных от pgxpool
type DBTX interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// PostgresStore реализует EventStore с использованием PostgreSQL.
type PostgresStore struct {
	db DBTX
}

// NewPostgresStore создает новый экземпляр PostgresStore.
func NewPosgresStore(pool DBTX) *PostgresStore {
	return &PostgresStore{
		db: pool,
	}
}

// EventStore определяет методы для работы с хранилищем событий.
type EventStore interface {
	CreateEvent(ctx context.Context, event *Event) (*Event, error)
	UpdateEvent(ctx context.Context, event *Event) (*Event, error)
	GetEvents(ctx context.Context) ([]*Event, error)
	GetEventByID(ctx context.Context, id int64) (*Event, error)
	DeleteEvent(ctx context.Context, id int64) (*Event, error)
	GetEventsByCategory(ctx context.Context, categoryID int64) ([]*Event, error)

	CreateCategory(ctx context.Context, category *Category) error
	ListCategories(parentCtx context.Context) ([]*Category, error)
	GetCategoryByID(parentCtx context.Context, id int) (*Category, error)
	UpdateCategory(parentCtx context.Context, category *Category) error
	DeleteCategory(parentCtx context.Context, id int) error
}

// CreatePostgresPool создает и проверяет пул соединений к PostgreSQL.
func CreatePostgresPool(parentCtx context.Context, dburl string) (*pgxpool.Pool, error) {
	ctx, cancel := context.WithTimeout(parentCtx, time.Second*3)
	defer cancel()

	pool, err := pgxpool.New(ctx, dburl)
	if err != nil {
		return nil, err
	}

	// Проверяем соединение
	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}

	return pool, nil
}
