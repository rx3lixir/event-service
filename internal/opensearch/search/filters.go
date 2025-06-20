package search

import (
	"time"
)

type Filter struct {
	// Поисковый текст
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
	SortBy    string `json:"sort_by,omitempty"`
	SortOrder string `json:"sort_order,omitempty"`
}

func NewFilter() *Filter {
	return &Filter{
		From:      0,
		Size:      20,
		SortBy:    "created_at",
		SortOrder: "desc",
	}
}

// API для построения фильтров \\

func (f *Filter) WithQuery(query string) *Filter {
	f.Query = query
	return f
}

func (f *Filter) WithCategories(categoryIDs ...int64) *Filter {
	f.CategoryIDs = categoryIDs
	return f
}

func (f *Filter) WithPriceRange(min, max *float32) *Filter {
	f.MinPrice = min
	f.MaxPrice = max
	return f
}

func (f *Filter) WithDateRange(from, to *time.Time) *Filter {
	f.DateFrom = from
	f.DateTo = to
	return f
}

func (f *Filter) WithLocation(location string) *Filter {
	f.Location = &location
	return f
}

func (f *Filter) WithSource(source string) *Filter {
	f.Source = &source
	return f
}

func (f *Filter) WithPagination(from, size int) *Filter {
	f.From = from
	f.Size = size
	return f
}

func (f *Filter) WithSort(sortBy, sortOrder string) *Filter {
	f.SortBy = sortBy
	f.SortOrder = sortOrder
	return f
}

func (f *Filter) IsEmpty() bool {
	return f.Query == "" &&
		len(f.CategoryIDs) == 0 &&
		f.MinPrice == nil &&
		f.MaxPrice == nil &&
		f.DateFrom == nil &&
		f.DateTo == nil &&
		f.Location == nil &&
		f.Source == nil
}
