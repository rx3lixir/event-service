package models

import (
	"github.com/rx3lixir/event-service/internal/db"
)

// FromDBEvent конвертирует db.Event в EventDocument для OpenSearch
func FromDBEvent(event *db.Event) *EventDocument {
	if event == nil {
		return nil
	}

	return &EventDocument{
		ID:          event.Id,
		Name:        event.Name,
		Description: event.Description,
		CategoryID:  event.CategoryID,
		Date:        event.Date,
		Time:        event.Time,
		Location:    event.Location,
		Price:       event.Price,
		Image:       event.Image,
		Source:      event.Source,
		CreatedAt:   event.CreatedAt,
		UpdatedAt:   event.UpdatedAt,
	}
}

// FromDBEvents конвертирует слайс db.Event в слайс EventDocument
func FromDBEvents(events []*db.Event) []*EventDocument {
	if events == nil {
		return nil
	}

	docs := make([]*EventDocument, 0, len(events))
	for _, event := range events {
		if doc := FromDBEvent(event); doc != nil {
			docs = append(docs, doc)
		}
	}
	return docs
}

// ToDBEvents конвертирует слайс EventDocument в слайс db.Event
func ToDBEvents(docs []*EventDocument) []*db.Event {
	if docs == nil {
		return nil
	}

	events := make([]*db.Event, 0, len(docs))
	for _, doc := range docs {
		if event := doc.ToDBEvent(); event != nil {
			events = append(events, event)
		}
	}
	return events
}
