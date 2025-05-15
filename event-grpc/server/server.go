package server

import (
	"context"
	"errors"
	"log/slog"

	"github.com/jackc/pgx/v5"
	eventPb "github.com/rx3lixir/event-service/event-grpc/gen/go"
	"github.com/rx3lixir/event-service/internal/db"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Server struct {
	storer *db.PostgresStore
	eventPb.UnimplementedEventServiceServer
}

func NewServer(storer *db.PostgresStore) *Server {
	return &Server{
		storer: storer,
	}
}

// CreateEvent создает новое событие.
func (s *Server) CreateEvent(ctx context.Context, req *eventPb.CreateEventReq) (*eventPb.EventRes, error) {
	slog.Info("CreateEvent request received", "name", req.GetName())

	// Валидация запроса
	if err := validateCreateEventReq(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	params := ProtoToCreateEventParams(req)
	dbEventToCreate := db.NewEventFromCreateRequest(params)

	createdEvent, err := s.storer.CreateEvent(ctx, dbEventToCreate)
	if err != nil {
		slog.Error("Failed to create event", "error", err)
		return nil, wrapError(err)
	}

	slog.Info("Event created successfully", "id", createdEvent.Id)
	return DBEventToProtoEventRes(createdEvent), nil
}

// GetEvent получает событие по ID.
func (s *Server) GetEvent(ctx context.Context, req *eventPb.GetEventReq) (*eventPb.EventRes, error) {
	slog.Info("GetEvent request received", "id", req.GetId())

	event, err := s.storer.GetEventByID(ctx, req.GetId())
	if err != nil {
		slog.Error("Failed to get event", "id", req.GetId(), "error", err)
		return nil, err
	}

	return DBEventToProtoEventRes(event), nil
}

func (s *Server) ListEvents(ctx context.Context, req *eventPb.ListEventsReq) (*eventPb.ListEventsRes, error) {
	slog.Info("ListEvents request received",
		"has_category_filter", req.CategoryID != nil,
		"has_date_filter", req.Date != nil)

	var events []*db.Event
	var err error

	// Если указана категория, фильтруем по ней
	if req.CategoryID != nil {
		events, err = s.storer.GetEventsByCategory(ctx, req.GetCategoryID())
		if err != nil {
			slog.Error("Failed to get events by category", "category_id", req.GetCategoryID(), "error", err)
			return nil, wrapError(err)
		}
	} else {
		// Иначе возвращаем все события
		events, err = s.storer.GetEvents(ctx)
		if err != nil {
			slog.Error("Failed to get all events", "error", err)
			return nil, wrapError(err)
		}
	}

	// TODO: фильтрация по дате и остальные фильтры

	slog.Info("Events retrieved successfully", "count", len(events))

	// Преобразуем срез событий в protobuf
	protoEvents := DBEventsToProtoEventsList(events)
	return &eventPb.ListEventsRes{
		Events: protoEvents,
	}, nil
}

// UpdateEvent обновляет существующее событие
func (s *Server) UpdateEvent(ctx context.Context, req *eventPb.UpdateEventReq) (*eventPb.EventRes, error) {
	slog.Info("UpdateEvent request received", "id", req.GetId())

	// Валидация запроса
	if err := validateUpdateEventReq(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Получаем ID и параметры обновления
	id, updateParams := ProtoToUpdateEventParams(req)

	// Получаем текущее событие
	currentEvent, err := s.storer.GetEventByID(ctx, id)
	if err != nil {
		slog.Error("Failed to get event for update", "id", id, "error", err)
		return nil, wrapError(err)
	}

	// Применяем обновления
	currentEvent.ApplyUpdate(updateParams)

	// Обновляем в базе
	updatedEvent, err := s.storer.UpdateEvent(ctx, currentEvent)
	if err != nil {
		slog.Error("Failed to update event", "id", id, "error", err)
		return nil, wrapError(err)
	}

	slog.Info("Event updated successfully", "id", updatedEvent.Id)

	return DBEventToProtoEventRes(updatedEvent), nil
}

func (s *Server) DeleteEvent(ctx context.Context, req *eventPb.DeleteEventReq) (*emptypb.Empty, error) {
	slog.Info("DeleteEvent request received", "id", req.GetId())

	_, err := s.storer.DeleteEvent(ctx, req.GetId())
	if err != nil {
		slog.Error("Failed to delete event", "id", req.GetId(), "error", err)
		return nil, wrapError(err)
	}

	slog.Info("Event deleted successfully", "id", req.GetId())

	return &emptypb.Empty{}, nil
}

// wrapError преобразует ошибки БД в gRPC ошибки со статусами.
func wrapError(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return status.Error(codes.NotFound, "resource not found")
	}

	// Здесь можно добавить обработку других специфичных для PostgreSQL ошибок
	// Например, нарушение unique constraint, foreign key constraint и т.д.

	return status.Error(codes.Internal, "internal server error")
}

// validateCreateEventReq проверяет корректность запроса на создание события.
func validateCreateEventReq(req *eventPb.CreateEventReq) error {
	if req.GetName() == "" {
		return errors.New("event name is required")
	}

	// Здесь можно добавить другие проверки
	// - Формат даты
	// - Формат времени
	// - Валидность категории
	// - и т.д.

	return nil
}

// validateUpdateEventReq проверяет корректность запроса на обновление события.
func validateUpdateEventReq(req *eventPb.UpdateEventReq) error {
	if req.GetId() <= 0 {
		return errors.New("invalid event ID")
	}

	if req.GetName() == "" {
		return errors.New("event name is required")
	}

	// Другие проверки, как и в validateCreateEventReq

	return nil
}
