package server

import (
	eventPb "github.com/rx3lixir/event-service/event-grpc/gen/go" // Путь к сгенерированному proto коду
	"github.com/rx3lixir/event-service/internal/db"               // Путь к вашему пакету db
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ProtoToCreateEventParams конвертирует CreateEventReq из gRPC в db.CreateEventParams.
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

// DBEventToProtoEventRes конвертирует db.Event в EventRes для gRPC ответа.
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

// DBEventsToProtoEventsList конвертирует срез []*db.Event в []*eventPb.EventRes.
func DBEventsToProtoEventsList(events []*db.Event) []*eventPb.EventRes {
	if events == nil {
		return nil // или return []*eventPb.EventRes{}
	}
	protoEvents := make([]*eventPb.EventRes, 0, len(events))
	for _, dbEvent := range events {
		protoEvents = append(protoEvents, DBEventToProtoEventRes(dbEvent))
	}
	return protoEvents
}

// ProtoToCreateCategoryParams конвертирует CreateCategoryReq из gRPC в db.CreateCategoryReq.
func ProtoToCreateCategoryParams(req *eventPb.CreateCategoryReq) *db.CreateCategoryReq {
	return &db.CreateCategoryReq{
		Name: req.GetName(),
	}
}

// ProtoToUpdateCategoryParams получает ID и имя категории из запроса
func ProtoToUpdateCategoryParams(req *eventPb.UpdateCategoryReq) (int, string) {
	return int(req.GetId()), req.GetName()
}

// DBCategoryToProtoCategoryRes конвертирует db.Category в CategoryRes для gRPC ответа.
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

// DBCategoriesToProtoList конвертирует срез []*db.Category в []*eventPb.CategoryRes.
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
