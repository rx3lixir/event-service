package server

import (
	"context"

	eventPb "github.com/rx3lixir/event-service/event-grpc/gen/go"
	"github.com/rx3lixir/event-service/internal/db"
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

func (s *Server) CreateEvent(ctx context.Context, req *eventPb.CreateEventReq) (*eventPb.EventRes, error) {
	params := ProtoToCreateEventParams(req)

	dbEventToCreate := db.NewEventFromCreateRequest(params)

	createdEvent, err := s.storer.CreateEvent(ctx, dbEventToCreate)
	if err != nil {
		return nil, err
	}

	return DBEventToProtoEventRes(createdEvent), nil
}

func (s *Server)  {
	
}
