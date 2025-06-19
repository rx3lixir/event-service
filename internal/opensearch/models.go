package opensearch

import (
	"time"

	"github.com/rx3lixir/event-service/internal/db"
)

// EventDocument представляет документ события в OpenSearch
type EventDocument struct {
	ID           int64      `json:"id"`
	Name         string     `json:"name"`
	Description  string     `json:"description"`
	CategoryID   int64      `json:"category_id"`
	CategoryName string     `json:"category_name,omitempty"` // Дополнительное поле для удобства поиска
	Date         string     `json:"date"`
	Time         string     `json:"time"`
	Location     string     `json:"location"`
	Price        float32    `json:"price"`
	Image        string     `json:"image"`
	Source       string     `json:"source"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    *time.Time `json:"updated_at"`
}

// PrepareForIndex подготавливает документ для индексации с completion полями
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

// SearchFilter представляет фильтры для поиска в OpenSearch
type SearchFilter struct {
	// Поисковый текст (полнотекстовый поиск по name, description, location)
	Query string `json:"query,omitempty"`

	// Фильтры
	CategoryIDs []int64    `json:"category_ids,omitempty"`
	MinPrice    *float32   `json:"min_price,omitempty"`
	MaxPrice    *float32   `json:"max_price,omitempty"`
	DateFrom    *time.Time `json:"date_from,omitempty"`
	DateTo      *time.Time `json:"date_to,omitempty"`
	Location    *string    `json:"location,omitempty"`
	Source      *string    `json:"source,omitempty"`

	// Пагинация
	From int `json:"from,omitempty"`
	Size int `json:"size,omitempty"`

	// Сортировка
	SortBy    string `json:"sort_by,omitempty"`    // "date", "price", "created_at", "_score"
	SortOrder string `json:"sort_order,omitempty"` // "asc", "desc"
}

// SearchResult представляет результат поиска
type SearchResult struct {
	Events     []*EventDocument `json:"events"`
	Total      int64            `json:"total"`
	MaxScore   *float64         `json:"max_score,omitempty"`
	SearchTime string           `json:"search_time"`
}

// SuggestionRequest представляет запрос на автокомплит
type SuggestionRequest struct {
	Query      string   `json:"query"`
	MaxResults int      `json:"max_results"`
	Fields     []string `json:"fields"`
}

// SuggestionResponse представляет ответ с предложениями
type SuggestionResponse struct {
	Suggesions []Suggestion `json:"suggestions"`
	Query      string       `json:"query"`
	Total      int          `json:"total"`
}

// Suggestion одно предложение
type Suggestion struct {
	Text     string  `json:"text"`
	Score    float64 `json:"score"`
	Type     string  `json:"type"`
	Category string  `json:"category,omitempty"`
	EventID  *int64  `json:"event_id,omitempty"`
}

// NewEventDocumentFromDB создает EventDocument из db.Event
func NewEventDocumentFromDB(event *db.Event) *EventDocument {
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

// ToDBEvent конвертирует EventDocument обратно в db.Event
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

// NewSearchFilter создает новый фильтр поиска с значениями по умолчанию
func NewSearchFilter() *SearchFilter {
	return &SearchFilter{
		From:      0,
		Size:      20,
		SortBy:    "created_at",
		SortOrder: "desc",
	}
}

// SetQuery устанавливает поисковый запрос
func (f *SearchFilter) SetQuery(query string) *SearchFilter {
	f.Query = query
	return f
}

// SetCategories устанавливает фильтр по категориям
func (f *SearchFilter) SetCategories(categoryIDs ...int64) *SearchFilter {
	f.CategoryIDs = categoryIDs
	return f
}

// SetPriceRange устанавливает диапазон цен
func (f *SearchFilter) SetPriceRange(min, max *float32) *SearchFilter {
	f.MinPrice = min
	f.MaxPrice = max
	return f
}

// SetDateRange устанавливает диапазон дат
func (f *SearchFilter) SetDateRange(from, to *time.Time) *SearchFilter {
	f.DateFrom = from
	f.DateTo = to
	return f
}

// SetLocation устанавливает фильтр по локации
func (f *SearchFilter) SetLocation(location string) *SearchFilter {
	f.Location = &location
	return f
}

// SetSource устанавливает фильтр по источнику
func (f *SearchFilter) SetSource(source string) *SearchFilter {
	f.Source = &source
	return f
}

// SetPagination устанавливает параметры пагинации
func (f *SearchFilter) SetPagination(from, size int) *SearchFilter {
	f.From = from
	f.Size = size
	return f
}

// SetSort устанавливает сортировку
func (f *SearchFilter) SetSort(sortBy, sortOrder string) *SearchFilter {
	f.SortBy = sortBy
	f.SortOrder = sortOrder
	return f
}

// IsEmpty проверяет, есть ли активные фильтры
func (f *SearchFilter) IsEmpty() bool {
	return f.Query == "" &&
		len(f.CategoryIDs) == 0 &&
		f.MinPrice == nil &&
		f.MaxPrice == nil &&
		f.DateFrom == nil &&
		f.DateTo == nil &&
		f.Location == nil &&
		f.Source == nil
}
