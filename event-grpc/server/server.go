package server

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	eventPb "github.com/rx3lixir/event-service/event-grpc/gen/go"
	"github.com/rx3lixir/event-service/internal/db"
	"github.com/rx3lixir/event-service/internal/elasticsearch"
	"github.com/rx3lixir/event-service/pkg/logger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Server struct {
	storer    *db.PostgresStore
	esService *elasticsearch.Service
	eventPb.UnimplementedEventServiceServer
	log logger.Logger
}

func NewServer(storer *db.PostgresStore, esService *elasticsearch.Service, log logger.Logger) *Server {
	return &Server{
		storer:    storer,
		esService: esService,
		log:       log,
	}
}

// CreateEvent создает новое событие и индексирует его в Elasticsearch.
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

	// Создаем событие в PostgreSQL
	createdEvent, err := s.storer.CreateEvent(ctx, dbEventToCreate)
	if err != nil {
		s.log.Error("failed to create event in PostgreSQL",
			"method", "CreateEvent",
			"error", err,
			"event-name", req.GetName(),
		)
		return nil, wrapError(err)
	}

	// Индексируем событие в Elasticsearch
	if err := s.esService.IndexEvent(ctx, createdEvent); err != nil {
		s.log.Error("failed to index event in Elasticsearch",
			"method", "CreateEvent",
			"event_id", createdEvent.Id,
			"error", err,
		)
		// Не возвращаем ошибку, так как событие уже создано в PostgreSQL
		// В продакшене можно добавить retry механизм или очередь
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

// ListEvents получает список событий.
// Если есть поисковый запрос (search_text), использует Elasticsearch.
// Иначе использует PostgreSQL с фильтрами.
func (s *Server) ListEvents(ctx context.Context, req *eventPb.ListEventsReq) (*eventPb.ListEventsRes, error) {
	s.log.Info("starting list events",
		"method", "ListEvents",
		"has_search_text", req.SearchText != nil && req.GetSearchText() != "",
		"search_text", req.GetSearchText(),
		"category_ids", req.GetCategoryIDs(),
		"min_price", req.GetMinPrice(),
		"max_price", req.GetMaxPrice(),
		"date_from", req.GetDateFrom(),
		"date_to", req.GetDateTo(),
		"location", req.GetLocation(),
		"source", req.GetSource(),
		"limit", req.GetLimit(),
		"offset", req.GetOffset(),
		"include_count", req.GetIncludeCount(),
	)

	// Если есть поисковый запрос, используем Elasticsearch
	if req.SearchText != nil && req.GetSearchText() != "" {
		return s.searchEventsWithElasticsearch(ctx, req)
	}

	// Иначе используем PostgreSQL
	return s.listEventsWithPostgreSQL(ctx, req)
}

// searchEventsWithElasticsearch выполняет поиск через Elasticsearch
func (s *Server) searchEventsWithElasticsearch(ctx context.Context, req *eventPb.ListEventsReq) (*eventPb.ListEventsRes, error) {
	s.log.Debug("using Elasticsearch for search",
		"search_text", req.GetSearchText(),
	)

	// Конвертируем в фильтр Elasticsearch
	filter, err := ProtoToElasticsearchFilter(req)
	if err != nil {
		s.log.Error("invalid Elasticsearch filter parameters",
			"method", "ListEvents",
			"error", err,
		)
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Выполняем поиск
	result, err := s.esService.SearchEvents(ctx, filter)
	if err != nil {
		s.log.Error("failed to search events in Elasticsearch",
			"method", "ListEvents",
			"error", err,
			"filter", filter,
		)
		return nil, status.Error(codes.Internal, "search failed")
	}

	s.log.Info("Elasticsearch search completed",
		"method", "ListEvents",
		"events_found", result.Total,
		"events_returned", len(result.Events),
		"search_time", result.SearchTime,
	)

	return ElasticsearchResultToListEventsRes(result), nil
}

// listEventsWithPostgreSQL выполняет запрос через PostgreSQL
func (s *Server) listEventsWithPostgreSQL(ctx context.Context, req *eventPb.ListEventsReq) (*eventPb.ListEventsRes, error) {
	s.log.Debug("using PostgreSQL for filtered list")

	// Конвертируем в фильтр PostgreSQL
	filter, err := ProtoToEventFilter(req)
	if err != nil {
		s.log.Error("invalid PostgreSQL filter parameters",
			"method", "ListEvents",
			"error", err,
		)
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	var events []*db.Event
	var totalCount *int64

	// Если клиент запросил общее кол-во событий - считаем
	if req.GetIncludeCount() {
		eventsResult, count, err := s.storer.GetEventsWithFilterAndCount(ctx, filter)
		if err != nil {
			s.log.Error("failed to list events with filter and count",
				"method", "ListEvents",
				"error", err,
				"filter", filter,
			)
			return nil, wrapError(err)
		}
		events = eventsResult
		totalCount = &count

		s.log.Info("PostgreSQL events retrieved successfully with count",
			"method", "ListEvents",
			"events_count", len(events),
			"total_count", count,
			"has_filters", !filter.IsEmpty(),
		)
	} else {
		// Обычный запрос без подсчета общего количества
		eventsResult, err := s.storer.GetEventsWithFilter(ctx, filter)
		if err != nil {
			s.log.Error("failed to list events with filter",
				"method", "ListEvents",
				"error", err,
				"filter", filter,
			)
			return nil, wrapError(err)
		}
		events = eventsResult

		s.log.Info("PostgreSQL events retrieved successfully",
			"method", "ListEvents",
			"events_count", len(events),
			"has_filters", !filter.IsEmpty(),
		)
	}

	// Конвертируем результат в gRPC ответ
	response := EventsToListEventsRes(
		events,
		totalCount,
		filter.GetLimit(),
		filter.GetOffset(),
	)

	return response, nil
}

// UpdateEvent обновляет существующее событие в PostgreSQL и Elasticsearch
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

	// Обновляем в PostgreSQL
	updatedEvent, err := s.storer.UpdateEvent(ctx, currentEvent)
	if err != nil {
		s.log.Error("Failed to update event in PostgreSQL", "id", id, "error", err)
		return nil, wrapError(err)
	}

	// Обновляем в Elasticsearch
	if err := s.esService.UpdateEvent(ctx, updatedEvent); err != nil {
		s.log.Error("failed to update event in Elasticsearch",
			"method", "UpdateEvent",
			"event_id", updatedEvent.Id,
			"error", err,
		)
		// Не возвращаем ошибку, так как событие уже обновлено в PostgreSQL
	}

	s.log.Info("event updated successfully",
		"method", "UpdateEvent",
		"event_id", updatedEvent.Id,
	)

	return DBEventToProtoEventRes(updatedEvent), nil
}

// DeleteEvent удаляет событие из PostgreSQL и Elasticsearch
func (s *Server) DeleteEvent(ctx context.Context, req *eventPb.DeleteEventReq) (*emptypb.Empty, error) {
	s.log.Info("starting delete event",
		"method", "DeleteEvent",
		"event_id", req.GetId(),
	)

	// Удаляем из PostgreSQL
	_, err := s.storer.DeleteEvent(ctx, req.GetId())
	if err != nil {
		s.log.Error("failed to delete event from PostgreSQL",
			"method", "DeleteEvent",
			"event_id", req.GetId(),
			"error", err,
		)
		return nil, wrapError(err)
	}

	// Удаляем из Elasticsearch
	if err := s.esService.DeleteEvent(ctx, req.GetId()); err != nil {
		s.log.Error("failed to delete event from Elasticsearch",
			"method", "DeleteEvent",
			"event_id", req.GetId(),
			"error", err,
		)
		// Не возвращаем ошибку, так как событие уже удалено из PostgreSQL
	}

	s.log.Info("event deleted successfully",
		"method", "DeleteEvent",
		"event_id", req.GetId(),
	)

	return &emptypb.Empty{}, nil
}

// CreateCategory создает новую категорию.
func (s *Server) CreateCategory(ctx context.Context, req *eventPb.CreateCategoryReq) (*eventPb.CategoryRes, error) {
	s.log.Info("starting create category",
		"method", "CreateCategory",
		"category_name", req.GetName(),
	)

	// Валидация запроса
	if err := validateCreateCategoryReq(req); err != nil {
		s.log.Error("invalid create category request",
			"method", "CreateCategory",
			"error", err,
			"name", req.GetName(),
		)
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	params := ProtoToCreateCategoryParams(req)
	categoryToCreate := db.NewCategory(params)

	err := s.storer.CreateCategory(ctx, categoryToCreate)
	if err != nil {
		s.log.Error("failed to create category",
			"method", "CreateCategory",
			"error", err,
			"category_name", req.GetName(),
		)
		return nil, wrapError(err)
	}

	s.log.Info("category created successfully",
		"method", "CreateCategory",
		"category_id", categoryToCreate.Id,
		"name", categoryToCreate.Name,
	)
	return DBCategoryToProtoCategoryRes(categoryToCreate), nil
}

// GetCategory получает категорию по ID.
func (s *Server) GetCategory(ctx context.Context, req *eventPb.GetCategoryReq) (*eventPb.CategoryRes, error) {
	s.log.Info("starting get category",
		"method", "GetCategory",
		"category_id", req.GetId(),
	)

	category, err := s.storer.GetCategoryByID(ctx, int(req.GetId()))
	if err != nil {
		s.log.Error("failed to get category",
			"method", "GetCategory",
			"category_id", req.GetId(),
			"error", err,
		)
		return nil, wrapError(err)
	}

	s.log.Debug("category retrieved successfully",
		"method", "GetCategory",
		"category_id", category.Id,
		"name", category.Name,
	)

	return DBCategoryToProtoCategoryRes(category), nil
}

// ListCategories возвращает список всех категорий.
func (s *Server) ListCategories(ctx context.Context, req *eventPb.ListCategoriesReq) (*eventPb.ListCategoriesRes, error) {
	s.log.Info("starting list categories",
		"method", "ListCategories",
	)

	categories, err := s.storer.ListCategories(ctx)
	if err != nil {
		s.log.Error("failed to list categories",
			"method", "ListCategories",
			"error", err,
		)
		return nil, wrapError(err)
	}

	s.log.Info("categories retrieved successfully",
		"count", len(categories),
	)

	protoCategories := DBCategoriesToProtoList(categories)
	return &eventPb.ListCategoriesRes{
		Categories: protoCategories,
	}, nil
}

// UpdateCategory обновляет существующую категорию.
func (s *Server) UpdateCategory(ctx context.Context, req *eventPb.UpdateCategoryReq) (*eventPb.CategoryRes, error) {
	s.log.Info("starting update category",
		"method", "UpdateCategory",
		"category_id", req.GetId(),
	)

	// Валидация запроса
	if err := validateUpdateCategoryReq(req); err != nil {
		s.log.Error("invalid update category request",
			"method", "UpdateCategory",
			"category_id", req.GetId(),
			"error", err,
		)
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Получаем ID и имя категории из запроса
	id, name := ProtoToUpdateCategoryParams(req)

	// Получаем текущую категорию
	currentCategory, err := s.storer.GetCategoryByID(ctx, id)
	if err != nil {
		s.log.Error("failed to get category for update",
			"method", "UpdateCategory",
			"category_id", id,
			"error", err,
		)
		return nil, wrapError(err)
	}

	// Обновляем имя
	currentCategory.Name = name

	// Обновляем в базе
	err = s.storer.UpdateCategory(ctx, currentCategory)
	if err != nil {
		s.log.Error("failed to update category",
			"id", id,
			"error", err,
		)
		return nil, wrapError(err)
	}

	s.log.Info("category updated successfully",
		"method", "UpdateCategory",
		"category_id", currentCategory.Id,
	)

	return DBCategoryToProtoCategoryRes(currentCategory), nil
}

// DeleteCategory удаляет категорию по ID.
func (s *Server) DeleteCategory(ctx context.Context, req *eventPb.DeleteCategoryReq) (*emptypb.Empty, error) {
	s.log.Info("starting delete category",
		"method", "DeleteCategory",
		"category_id", req.GetId(),
	)

	// Проверяем, существует ли категория (опционально)
	_, err := s.storer.GetCategoryByID(ctx, int(req.GetId()))
	if err != nil {
		s.log.Error("failed to get category for deletion",
			"method", "DeleteCategory",
			"category_id", req.GetId(),
			"error", err,
		)
		return nil, wrapError(err)
	}

	// Удаляем категорию
	err = s.storer.DeleteCategory(ctx, int(req.GetId()))
	if err != nil {
		s.log.Error("failed to delete category",
			"method", "DeleteCategory",
			"category_id", req.GetId(),
			"error", err,
		)
		return nil, wrapError(err)
	}

	s.log.Info("category deleted successfully",
		"method", "DeleteCategory",
		"category_id", req.GetId(),
	)

	return &emptypb.Empty{}, nil
}

// validateCreateCategoryReq проверяет корректность запроса на создание категории.
func validateCreateCategoryReq(req *eventPb.CreateCategoryReq) error {
	if req.GetName() == "" {
		return errors.New("category name is required")
	}
	return nil
}

// validateUpdateCategoryReq проверяет корректность запроса на обновление категории.
func validateUpdateCategoryReq(req *eventPb.UpdateCategoryReq) error {
	if req.GetId() <= 0 {
		return errors.New("invalid category ID")
	}
	if req.GetName() == "" {
		return errors.New("category name is required")
	}
	return nil
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
	// и т.д.

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
