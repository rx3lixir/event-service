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

// Пример использования ListEventsReq для формирования параметров фильтрации (если нужно)
// Эта функция будет в вашем gRPC хендлере
/*
func GetFilterParamsFromListEventsReq(req *eventPb.ListEventsReq) map[string]interface{} {
	filters := make(map[string]interface{})
	if req.GetCategoryID() > 0 { // proto3 optional int64 будет 0, если не установлено
		filters["category_id"] = req.GetCategoryID()
	}
	if req.HascCategoryID() { // Для proto3 optional полей есть Has методы
	    filters["category_id"] = req.GetCategoryID()
	}


	if req.GetDate() != "" { // proto3 optional string будет "", если не установлено
		filters["date"] = req.GetDate()
	}
    if req.HasDate() {
        filters["date"] = req.GetDate()
    }
	return filters
}
*/
