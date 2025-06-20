package models

import (
	"time"

	"github.com/rx3lixir/event-service/internal/db"
)

type EventDocument struct {
	ID           int64      `json:"id"`
	Name         string     `json:"name"`
	Description  string     `json:"description"`
	CategoryID   int64      `json:"category_id"`
	CategoryName string     `json:"category_name,omitempty"`
	Date         string     `json:"date"`
	Time         string     `json:"time"`
	Location     string     `json:"location"`
	Price        float32    `json:"price"`
	Image        string     `json:"image"`
	Source       string     `json:"source"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    *time.Time `json:"updated_at"`
}

type SearchResult struct {
	Events     []*EventDocument `json:"events"`
	Total      int64            `json:"total"`
	MaxScore   *float64         `json:"max_score,omitempty"`
	SearchTime string           `json:"search_time"`
}

func (e *EventDocument) PrepareForIndex() map[string]any {
	doc := map[string]any{
		"id":          e.ID,
		"name":        e.Name,
		"description": e.Description,
		"category_id": e.CategoryID,
		"date":        e.Date,
		"time":        e.Time,
		"location":    e.Location,
		"price":       e.Price,
		"image":       e.Image,
		"source":      e.Source,
		"created_at":  e.CreatedAt,
		"updated_at":  e.UpdatedAt,
	}

	// Добавляем данные для completion suggester
	doc["name.completion"] = map[string]any{
		"input":  []string{e.Name},
		"weight": 10,
	}

	if e.Location != "" {
		doc["location.completion"] = map[string]any{
			"input":  []string{e.Location},
			"weight": 5,
		}
	}

	return doc
}

// Принимает документ OpenSearch - возвращает Event глобальный
func (e *EventDocument) ToDBEvent() *db.Event {
	return &db.Event{
		Id:          e.ID,
		Name:        e.Name,
		Description: e.Description,
		CategoryID:  e.CategoryID,
		Date:        e.Date,
		Time:        e.Time,
		Location:    e.Location,
		Price:       e.Price,
		Image:       e.Image,
		Source:      e.Source,
		CreatedAt:   e.CreatedAt,
		UpdatedAt:   e.UpdatedAt,
	}
}
