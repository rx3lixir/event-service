package db

import "time"

// EventFilter содержит фильтры для событий в PostgreSQL
// Полнотекстовый поиск и некоторые сложные фильтры теперь обрабатываются через Elasticsearch
type EventFilter struct {
	CategoryIDs []int64    // Фильтр по массиву ID категорий
	MinPrice    *float32   // Минимальная цена (включительно)
	MaxPrice    *float32   // Максимальная цена (включительно)
	DateFrom    *time.Time // Дата начала диапазона (включительно)
	DateTo      *time.Time // Дата окончания диапазона (включительно)
	Location    *string    // Фильтр по локации (точное совпадение)
	Source      *string    // Фильтр по источнику события (точное совпадение)

	// Пагинация
	Limit  *int // Лимит количества записей для пагинации
	Offset *int // Смещение для пагинации

	// Примечание: SearchText удален - теперь используется Elasticsearch
}

// FilterOption функциональная опция для конфигурации фильтра.
type FilterOption func(*EventFilter)

// WithCategory добавляет фильтр по одной или нескольким категориям.
// Если передано несколько ID, будет использоваться условие IN.
func WithCategory(categoryIDs ...int64) FilterOption {
	return func(ef *EventFilter) {
		ef.CategoryIDs = categoryIDs
	}
}

// WithPriceRange добавляет фильтр по диапазону цен.
// Можно передать только min (max = nil) или только max (min = nil).
func WithPriceRange(min, max *float32) FilterOption {
	return func(ef *EventFilter) {
		ef.MinPrice = min
		ef.MaxPrice = max
	}
}

// WithMinPrice добавляет фильтр только по минимальной цене.
// Удобная обёртка для WithPriceRange(min, nil).
func WithMinPrice(minPrice float32) FilterOption {
	return func(f *EventFilter) {
		f.MinPrice = &minPrice
	}
}

// WithMaxPrice добавляет фильтр только по максимальной цене.
// Удобная обёртка для WithPriceRange(nil, max).
func WithMaxPrice(maxPrice float32) FilterOption {
	return func(f *EventFilter) {
		f.MaxPrice = &maxPrice
	}
}

// WithDateRange добавляет фильтр по диапазону дат.
// Можно передать только from (to = nil) или только to (from = nil).
func WithDateRange(from, to *time.Time) FilterOption {
	return func(f *EventFilter) {
		f.DateFrom = from
		f.DateTo = to
	}
}

// WithDateFrom добавляет фильтр только по начальной дате.
// Удобная обёртка для WithDateRange(from, nil).
func WithDateFrom(dateFrom time.Time) FilterOption {
	return func(f *EventFilter) {
		f.DateFrom = &dateFrom
	}
}

// WithDateTo добавляет фильтр только по конечной дате.
// Удобная обёртка для WithDateRange(nil, to).
func WithDateTo(dateTo time.Time) FilterOption {
	return func(f *EventFilter) {
		f.DateTo = &dateTo
	}
}

// WithLocation добавляет фильтр по точному совпадению локации.
// Поиск регистрозависимый.
func WithLocation(location string) FilterOption {
	return func(f *EventFilter) {
		f.Location = &location
	}
}

// WithSource добавляет фильтр по точному совпадению источника события.
// Поиск регистрозависимый.
func WithSource(source string) FilterOption {
	return func(f *EventFilter) {
		f.Source = &source
	}
}

// WithPagination добавляет параметры пагинации.
// limit - максимальное количество записей в ответе.
// offset - количество записей, которые нужно пропустить.
func WithPagination(limit, offset int) FilterOption {
	return func(f *EventFilter) {
		f.Limit = &limit
		f.Offset = &offset
	}
}

// WithLimit добавляет только лимит без offset.
// Удобная обёртка для WithPagination(limit, 0).
func WithLimit(limit int) FilterOption {
	return func(f *EventFilter) {
		f.Limit = &limit
		// Offset остается nil, что означает 0
	}
}

// NewEventFilter создает новый фильтр с применением переданных опций.
func NewEventFilter(opts ...FilterOption) *EventFilter {
	filter := &EventFilter{}
	for _, opt := range opts {
		opt(filter)
	}
	return filter
}

// IsEmpty проверяет, является ли фильтр пустым (без условий).
// Полезно для оптимизации - если фильтр пустой, можно использовать более простой запрос.
func (f *EventFilter) IsEmpty() bool {
	return len(f.CategoryIDs) == 0 &&
		f.MinPrice == nil &&
		f.MaxPrice == nil &&
		f.DateFrom == nil &&
		f.DateTo == nil &&
		f.Location == nil &&
		f.Source == nil
}

// HasPagination проверяет, установлены ли параметры пагинации.
func (f *EventFilter) HasPagination() bool {
	return f.Limit != nil || f.Offset != nil
}

// GetLimit возвращает лимит или значение по умолчанию.
func (f *EventFilter) GetLimit() int {
	if f.Limit == nil {
		return 100 // Значение по умолчанию
	}
	return *f.Limit
}

// GetOffset возвращает offset или 0.
func (f *EventFilter) GetOffset() int {
	if f.Offset == nil {
		return 0
	}
	return *f.Offset
}
