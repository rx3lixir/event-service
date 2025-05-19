package server

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	eventPb "github.com/rx3lixir/event-service/event-grpc/gen/go"
	"github.com/rx3lixir/event-service/internal/db"
	"github.com/rx3lixir/event-service/internal/logger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Server struct {
	storer *db.PostgresStore
	eventPb.UnimplementedEventServiceServer
	log logger.Logger
}

func NewServer(storer *db.PostgresStore, log logger.Logger) *Server {
	return &Server{
		storer: storer,
		log:    log,
	}
}

// CreateEvent создает новое событие.
func (s *Server) CreateEvent(ctx context.Context, req *eventPb.CreateEventReq) (*eventPb.EventRes, error) {
	s.log.Info("CreateEvent request received",
		"method", "CreateEvent",
		"event_name", req.GetName(),
	)

	// Валидация запроса
	if err := validateCreateEventReq(req); err != nil {
		s.log.Error("invalid create event request",
			"method", "CreateEvent",
			"error", err,
			"name", req.GetName(),
		)
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	params := ProtoToCreateEventParams(req)
	dbEventToCreate := db.NewEventFromCreateRequest(params)

	createdEvent, err := s.storer.CreateEvent(ctx, dbEventToCreate)
	if err != nil {
		s.log.Error("failed to create event",
			"method", "CreateEvent",
			"error", err,
			"event-name", req.GetName(),
		)
		return nil, wrapError(err)
	}

	s.log.Info("event created successfully",
		"method", "CreateEvent",
		"event_id", createdEvent.Id,
		"name", createdEvent.Name,
		"category", createdEvent.CategoryID,
	)
	return DBEventToProtoEventRes(createdEvent), nil
}

// GetEvent получает событие по ID.
func (s *Server) GetEvent(ctx context.Context, req *eventPb.GetEventReq) (*eventPb.EventRes, error) {
	s.log.Info("starting get event",
		"method", "GetEvent",
		"event_id", req.GetId(),
	)

	event, err := s.storer.GetEventByID(ctx, req.GetId())
	if err != nil {
		s.log.Error("failed to get event",
			"method", "GetEvent",
			"event_id", req.GetId(),
			"error", err,
		)
		return nil, wrapError(err)
	}

	s.log.Debug("event retrieved successfully",
		"method", "GetEvent",
		"event_id", event.Id,
		"name", event.Name,
	)

	return DBEventToProtoEventRes(event), nil
}

func (s *Server) ListEvents(ctx context.Context, req *eventPb.ListEventsReq) (*eventPb.ListEventsRes, error) {
	s.log.Info("starting list events",
		"method", "ListEvents",
	)

	var events []*db.Event
	var err error

	// Возвращаем события
	events, err = s.storer.GetEvents(ctx)
	if err != nil {
		s.log.Error("failed to list events",
			"method", "ListEvents",
			"error", err,
		)
		return nil, wrapError(err)
	}

	// TODO: фильтрация по дате и остальные фильтры

	s.log.Info("Events retrieved successfully", "count", len(events))

	// Преобразуем срез событий в protobuf
	protoEvents := DBEventsToProtoEventsList(events)
	return &eventPb.ListEventsRes{
		Events: protoEvents,
	}, nil
}

// UpdateEvent обновляет существующее событие
func (s *Server) UpdateEvent(ctx context.Context, req *eventPb.UpdateEventReq) (*eventPb.EventRes, error) {
	s.log.Info("starting update event",
		"method", "UpdateEvent",
		"event_id", req.GetId(),
	)

	// Валидация запроса
	if err := validateUpdateEventReq(req); err != nil {
		s.log.Error("invalid update event request",
			"method", "UpdateEvent",
			"event_id", req.GetId(),
			"error", err,
		)
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Получаем ID и параметры обновления
	id, updateParams := ProtoToUpdateEventParams(req)

	// Получаем текущее событие
	currentEvent, err := s.storer.GetEventByID(ctx, id)
	if err != nil {
		s.log.Error("failed to get event for update",
			"method", "UpdateEvent",
			"event_id", id,
			"error", err,
		)
		return nil, wrapError(err)
	}

	// Применяем обновления
	currentEvent.ApplyUpdate(updateParams)

	// Обновляем в базе
	updatedEvent, err := s.storer.UpdateEvent(ctx, currentEvent)
	if err != nil {
		s.log.Error("Failed to update event", "id", id, "error", err)
		return nil, wrapError(err)
	}

	s.log.Info("event updated successfully",
		"method", "UpdateEvent",
		"event_id", updatedEvent.Id,
	)

	return DBEventToProtoEventRes(updatedEvent), nil
}

func (s *Server) DeleteEvent(ctx context.Context, req *eventPb.DeleteEventReq) (*emptypb.Empty, error) {
	s.log.Info("starting delete event",
		"method", "DeleteEvent",
		"event_id", req.GetId(),
	)

	_, err := s.storer.DeleteEvent(ctx, req.GetId())
	if err != nil {
		s.log.Error("failed to delete event",
			"method", "DeleteEvent",
			"event_id", req.GetId(),
			"error", err,
		)
		return nil, wrapError(err)
	}

	s.log.Info("event deleted successfully",
		"method", "DeleteEvent",
		"event_id", req.GetId(),
	)

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
