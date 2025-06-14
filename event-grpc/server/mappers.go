package server

import (
	"fmt"
	"time"

	eventPb "github.com/rx3lixir/event-service/event-grpc/gen/go"
	"github.com/rx3lixir/event-service/internal/db"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ============================================================================
// СОБЫТИЯ - МАППЕРЫ ИЗ PROTO В DB
// ============================================================================

// ProtoToEventFilter конвертирует ListEventsReq из gRPC в EventFilter.
// Обрабатывает все типы фильтров и валидирует входные данные.
func ProtoToEventFilter(req *eventPb.ListEventsReq) (*db.EventFilter, error) {
	if req == nil {
		return db.NewEventFilter(), nil
	}

	opts := []db.FilterOption{}

	// Фильтр по категориям
	if len(req.GetCategoryIDs()) > 0 {
		opts = append(opts, db.WithCategory(req.GetCategoryIDs()...))
	}

	// Фильтр по диапазону цен
	if req.MinPrice != nil && req.MaxPrice != nil {
		minPrice := req.GetMinPrice()
		maxPrice := req.GetMaxPrice()
		opts = append(opts, db.WithPriceRange(&minPrice, &maxPrice))
	} else if req.MinPrice != nil {
		minPrice := req.GetMinPrice()
		opts = append(opts, db.WithMinPrice(minPrice))
	} else if req.MaxPrice != nil {
		maxPrice := req.GetMaxPrice()
		opts = append(opts, db.WithMaxPrice(maxPrice))
	}

	// Фильтр по диапазону дат с валидацией формата
	if err := applyDateFilters(req, &opts); err != nil {
		return nil, err
	}

	// Фильтр по локации
	if req.Location != nil {
		opts = append(opts, db.WithLocation(req.GetLocation()))
	}

	// Фильтр по источнику
	if req.Source != nil {
		opts = append(opts, db.WithSource(req.GetSource()))
	}

	// Полнотекстовый поиск
	if req.SearchText != nil && req.GetSearchText() != "" {
		opts = append(opts, db.WithSearch(req.GetSearchText()))
	}

	// Пагинация
	if req.Limit != nil || req.Offset != nil {
		limit := int(req.GetLimit())
		offset := int(req.GetOffset())

		// Устанавливаем значения по умолчанию
		if limit == 0 {
			limit = 100 // Значение по умолчанию
		}
		if offset == 0 && req.Offset == nil {
			offset = 0 // Явно устанавливаем 0, если offset не был передан
		}

		opts = append(opts, db.WithPagination(limit, offset))
	}

	return db.NewEventFilter(opts...), nil
}

// applyDateFilters применяет фильтры по датам с валидацией
func applyDateFilters(req *eventPb.ListEventsReq, opts *[]db.FilterOption) error {
	var dateFrom, dateTo *time.Time

	if req.DateFrom != nil {
		if parsed, err := time.Parse("2006-01-02", req.GetDateFrom()); err == nil {
			dateFrom = &parsed
		} else {
			return fmt.Errorf("invalid date_from format, expected YYYY-MM-DD: %s", req.GetDateFrom())
		}
	}

	if req.DateTo != nil {
		if parsed, err := time.Parse("2006-01-02", req.GetDateTo()); err == nil {
			dateTo = &parsed
		} else {
			return fmt.Errorf("invalid date_to format, expected YYYY-MM-DD: %s", req.GetDateTo())
		}
	}

	// Применяем фильтр по датам, если хотя бы одна дата указана
	if dateFrom != nil || dateTo != nil {
		*opts = append(*opts, db.WithDateRange(dateFrom, dateTo))
	}

	return nil
}

// ProtoToCreateEventParams конвертирует CreateEventReq из gRPC в db.CreateEventParams
func ProtoToCreateEventParams(req *eventPb.CreateEventReq) db.CreateEventParams {
	return db.CreateEventParams{
		Name:        req.GetName(),
		Description: req.GetDescription(),
		CategoryID:  req.GetCategoryID(),
		Date:        req.GetDate(),
		Time:        req.GetTime(),
		Location:    req.GetLocation(),
		Price:       float32(req.GetPrice()), // proto float это float64 в Go
		Image:       req.GetImage(),
		Source:      req.GetSource(),
	}
}

// ProtoToUpdateEventParams конвертирует UpdateEventReq из gRPC в db.UpdateEventParams.
// Также возвращает ID события.
func ProtoToUpdateEventParams(req *eventPb.UpdateEventReq) (int64, db.UpdateEventParams) {
	return req.GetId(), db.UpdateEventParams{
		Name:        req.GetName(),
		Description: req.GetDescription(),
		CategoryID:  req.GetCategoryID(),
		Date:        req.GetDate(),
		Time:        req.GetTime(),
		Location:    req.GetLocation(),
		Price:       req.GetPrice(),
		Image:       req.GetImage(),
		Source:      req.GetSource(),
	}
}

// ============================================================================
// СОБЫТИЯ - МАППЕРЫ ИЗ DB В PROTO
// ============================================================================

// DBEventToProtoEventRes конвертирует db.Event в EventRes для gRPC ответа
func DBEventToProtoEventRes(event *db.Event) *eventPb.EventRes {
	if event == nil {
		return nil
	}

	var updatedAtProto *timestamppb.Timestamp
	if event.UpdatedAt != nil {
		updatedAtProto = timestamppb.New(*event.UpdatedAt)
	}

	return &eventPb.EventRes{
		Id:          event.Id,
		Name:        event.Name,
		Description: event.Description,
		CategoryID:  event.CategoryID,
		Date:        event.Date,
		Time:        event.Time,
		Location:    event.Location,
		Price:       float32(event.Price), // Убедись, что db.Event.Price это float32
		Image:       event.Image,
		Source:      event.Source,
		CreatedAt:   timestamppb.New(event.CreatedAt),
		UpdatedAt:   updatedAtProto,
	}
}

// DBEventsToProtoEventsList конвертирует срез []*db.Event в []*eventPb.EventRes
func DBEventsToProtoEventsList(events []*db.Event) []*eventPb.EventRes {
	if events == nil {
		return nil
	}

	protoEvents := make([]*eventPb.EventRes, 0, len(events))
	for _, dbEvent := range events {
		protoEvents = append(protoEvents, DBEventToProtoEventRes(dbEvent))
	}

	return protoEvents
}

// EventsToListEventsRes конвертирует события в gRPC ответ с опциональной пагинацией
func EventsToListEventsRes(events []*db.Event, totalCount *int64, limit, offset int) *eventPb.ListEventsRes {
	response := &eventPb.ListEventsRes{
		Events: DBEventsToProtoEventsList(events),
	}

	// Добавляем мета-информацию о пагинации, если запрашивалось
	if totalCount != nil {
		response.Pagination = CreatePaginationMeta(*totalCount, limit, offset)
	}

	return response
}

// ============================================================================
// КАТЕГОРИИ - МАППЕРЫ ИЗ PROTO В DB
// ============================================================================

// ProtoToCreateCategoryParams конвертирует CreateCategoryReq из gRPC в db.CreateCategoryReq
func ProtoToCreateCategoryParams(req *eventPb.CreateCategoryReq) *db.CreateCategoryReq {
	return &db.CreateCategoryReq{
		Name: req.GetName(),
	}
}

// ProtoToUpdateCategoryParams получает ID и имя категории из запроса
func ProtoToUpdateCategoryParams(req *eventPb.UpdateCategoryReq) (int, string) {
	return int(req.GetId()), req.GetName()
}

// ============================================================================
// КАТЕГОРИИ - МАППЕРЫ ИЗ DB В PROTO
// ============================================================================

// DBCategoryToProtoCategoryRes конвертирует db.Category в CategoryRes для gRPC ответа
func DBCategoryToProtoCategoryRes(category *db.Category) *eventPb.CategoryRes {
	if category == nil {
		return nil
	}

	return &eventPb.CategoryRes{
		Id:        int32(category.Id),
		Name:      category.Name,
		CreatedAt: timestamppb.New(category.CreatedAt),
		UpdatedAt: timestamppb.New(category.UpdatedAt),
	}
}

// DBCategoriesToProtoList конвертирует срез []*db.Category в []*eventPb.CategoryRes
func DBCategoriesToProtoList(categories []*db.Category) []*eventPb.CategoryRes {
	if categories == nil {
		return nil
	}

	protoCategories := make([]*eventPb.CategoryRes, 0, len(categories))
	for _, dbCategory := range categories {
		protoCategories = append(protoCategories, DBCategoryToProtoCategoryRes(dbCategory))
	}

	return protoCategories
}

// ============================================================================
// УТИЛИТЫ И ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
// ============================================================================

// CreatePaginationMeta создает мета-информацию для пагинации
func CreatePaginationMeta(totalCount int64, limit, offset int) *eventPb.PaginationMeta {
	hasMore := totalCount > int64(offset+limit)

	return &eventPb.PaginationMeta{
		TotalCount: totalCount,
		Limit:      int32(limit),
		Offset:     int32(offset),
		HasMore:    hasMore,
	}
}
