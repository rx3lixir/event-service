package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	// INSERT INTO events ... RETURNING id, created_at, updated_at
	// created_at должно иметь DEFAULT CURRENT_TIMESTAMP в схеме БД,
	// updated_at может быть NULL или DEFAULT CURRENT_TIMESTAMP и обновляться через NOW() в UPDATE.
	// Количество VALUES ($1-$9) должно соответствовать количеству передаваемых полей.
	createEventQuery = `INSERT INTO events (name, description, category_id, date, time, location, price, image, source) 
						VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) 
						RETURNING id, created_at, updated_at`

	// UPDATE events SET ..., updated_at = NOW() WHERE id = $N RETURNING updated_at
	// Количество SET полей + id ($1-$10)
	updateEventQuery = `UPDATE events 
						SET name = $1, description = $2, category_id = $3, date = $4, time = $5, 
						    location = $6, price = $7, image = $8, source = $9, updated_at = NOW() 
						WHERE id = $10 
						RETURNING updated_at` // Можно возвращать все поля: RETURNING id, name, ..., updated_at

	// SELECT запросы
	getEventsQueryBaseFields = `SELECT id, name, description, category_id, date, time, location, price, image, source, created_at, updated_at FROM events`
	getEventsQuery           = getEventsQueryBaseFields
	getEventByIdQuery        = getEventsQueryBaseFields + ` WHERE id = $1`
	getEventsByCategoryQuery = getEventsQueryBaseFields + ` WHERE category_id = $1`
	deleteEventQuery         = `DELETE FROM events WHERE id = $1`
)

// CreateEvent создает новое событие.
func (s *PostgresStore) CreateEvent(parentCtx context.Context, event *Event) (*Event, error) {
	ctx, cancel := context.WithTimeout(parentCtx, 3*time.Second)
	defer cancel()

	// ВАЖНО: убедись, что event.Time не конфликтует с ключевым словом TIME в SQL, если это так, используй кавычки: "time"
	err := s.db.QueryRow(
		ctx,
		createEventQuery,
		event.Name,
		event.Description,
		event.CategoryID,
		event.Date,
		event.Time, // Если имя колонки "time", оно должно быть в кавычках в SQL
		event.Location,
		event.Price,
		event.Image,
		event.Source,
	).Scan(&event.Id, &event.CreatedAt, &event.UpdatedAt) // Сканируем ID и таймстемпы, установленные БД

	if err != nil {
		// Можно добавить более специфическую обработку ошибок PostgreSQL (например, unique_violation)
		return nil, fmt.Errorf("failed to create event: %w", err)
	}

	return event, nil
}

// UpdateEvent обновляет существующее событие.
func (s *PostgresStore) UpdateEvent(parentCtx context.Context, event *Event) (*Event, error) {
	ctx, cancel := context.WithTimeout(parentCtx, 3*time.Second)
	defer cancel()

	var newUpdatedAt time.Time // Для сканирования значения из RETURNING updated_at

	err := s.db.QueryRow(
		ctx,
		updateEventQuery,
		event.Name,
		event.Description,
		event.CategoryID,
		event.Date,
		event.Time, // Если имя колонки "time", оно должно быть в кавычках в SQL
		event.Location,
		event.Price,
		event.Image,
		event.Source,
		event.Id,
	).Scan(&newUpdatedAt)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Это означает, что событие с таким ID не найдено для обновления
			return nil, fmt.Errorf("event with ID %d not found for update: %w", event.Id, err)
		}
		return nil, fmt.Errorf("failed to update event %d: %w", event.Id, err)
	}

	event.UpdatedAt = &newUpdatedAt // Обновляем поле в объекте event

	// Если updateEventQuery возвращал все поля (RETURNING *), то можно было бы пересканировать весь event:
	// _, err := s.GetEventByID(ctx, event.Id) // или scanIntoEvent, если QueryRow вернул все поля.
	// Это гарантирует, что все поля в объекте event актуальны, а не только UpdatedAt.
	// Но для этого updateEventQuery должен возвращать все поля.

	return event, nil
}

// GetEvents извлекает все события.
func (s *PostgresStore) GetEvents(parentCtx context.Context) ([]*Event, error) {
	ctx, cancel := context.WithTimeout(parentCtx, 3*time.Second)
	defer cancel()

	rows, err := s.db.Query(ctx, getEventsQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query events: %w", err)
	}
	defer rows.Close()

	events := []*Event{}
	for rows.Next() {
		event, err := scanEvent(rows) // Используем общий сканер
		if err != nil {
			return nil, fmt.Errorf("failed to scan event during GetEvents: %w", err)
		}
		events = append(events, event)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating event rows: %w", err)
	}

	return events, nil
}

// GetEventByID извлекает событие по ID.
func (s *PostgresStore) GetEventByID(parentCtx context.Context, id int64) (*Event, error) {
	ctx, cancel := context.WithTimeout(parentCtx, 3*time.Second)
	defer cancel()

	row := s.db.QueryRow(ctx, getEventByIdQuery, id)
	event, err := scanEvent(row) // Используем общий сканер для pgx.Row

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("event with ID %d not found: %w", id, err)
		}
		return nil, fmt.Errorf("failed to get event by ID %d: %w", id, err)
	}

	return event, nil
}

// DeleteEvent удаляет событие по ID.
// Возвращает удаленный объект, если он был найден и удален.
func (s *PostgresStore) DeleteEvent(parentCtx context.Context, id int64) (*Event, error) {
	ctx, cancel := context.WithTimeout(parentCtx, 3*time.Second)
	defer cancel()

	// Для соответствия proto `DeleteEvent(Req) returns (EventRes)`,
	// мы сначала получаем событие, затем удаляем.
	// Это не атомарно. Если нужна атомарность и возврат удаленных данных,
	// можно использовать `DELETE ... RETURNING *;` если СУБД поддерживает.

	eventToDelete, err := s.GetEventByID(ctx, id) // Сначала получаем, чтобы было что вернуть
	if err != nil {
		// Ошибка уже включает "not found" если это так
		return nil, fmt.Errorf("cannot delete event, failed to retrieve event ID %d: %w", id, err)
	}

	cmdTag, err := s.db.Exec(ctx, deleteEventQuery, id)
	if err != nil {
		return nil, fmt.Errorf("failed to execute delete for event %d: %w", id, err)
	}

	if cmdTag.RowsAffected() == 0 {
		// Это странная ситуация, если GetEventByID выше нашел событие,
		// но Exec не нашел строк для удаления (возможно, гонка состояний).
		return nil, fmt.Errorf("event with ID %d was found but not deleted (0 rows affected)", id)
	}

	return eventToDelete, nil // Возвращаем ранее полученные данные удаленного события
}

// GetEventsByCategory извлекает события по ID категории.
func (s *PostgresStore) GetEventsByCategory(parentCtx context.Context, categoryID int64) ([]*Event, error) {
	ctx, cancel := context.WithTimeout(parentCtx, 3*time.Second)
	defer cancel()

	rows, err := s.db.Query(ctx, getEventsByCategoryQuery, categoryID)
	if err != nil {
		return nil, fmt.Errorf("failed to query events by category %d: %w", categoryID, err)
	}
	defer rows.Close()

	events := []*Event{}
	for rows.Next() {
		event, err := scanEvent(rows) // Используем общий сканер
		if err != nil {
			return nil, fmt.Errorf("failed to scan event during GetEventsByCategory: %w", err)
		}
		events = append(events, event)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating event rows for category %d: %w", categoryID, err)
	}

	return events, nil
}

// pgxScanner интерфейс для абстракции pgx.Rows и pgx.Row для функции scanEvent.
type pgxScanner interface {
	Scan(dest ...any) error
}

// scanEvent сканирует одну строку в структуру Event.
// Работает как с pgx.Rows (через rows.Scan), так и с pgx.Row (через row.Scan).
func scanEvent(scanner pgxScanner) (*Event, error) {
	event := new(Event)
	// Убедись, что порядок и количество полей соответствуют SELECT запросам
	err := scanner.Scan(
		&event.Id,
		&event.Name,
		&event.Description,
		&event.CategoryID,
		&event.Date,
		&event.Time, // Если имя колонки "time", оно должно быть в кавычках в SQL SELECT
		&event.Location,
		&event.Price,
		&event.Image,
		&event.Source,
		&event.CreatedAt,
		&event.UpdatedAt, // UpdatedAt это *time.Time, Scan обработает NULL корректно
	)
	if err != nil {
		return nil, err // Ошибка будет обработана вызывающей функцией (например, pgx.ErrNoRows)
	}
	return event, nil
}
