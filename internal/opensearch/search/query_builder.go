package search

import (
	"time"
)

type QueryBuilder struct{}

func NewQueryBuilder() *QueryBuilder {
	return &QueryBuilder{}
}

func (qb *QueryBuilder) BuildSearchQuery(filter *Filter) map[string]any {
	query := map[string]any{
		"from": filter.From,
		"size": filter.Size,
	}

	// Основной запрос
	boolQuery := map[string]any{
		"bool": map[string]any{},
	}

	var mustQueries []any
	var filterQueries []any

	// Полнотекстовый поиск
	if filter.Query != "" {
		mustQueries = append(mustQueries, qb.buildTextSearchQuery(filter.Query))
	}

	// Фильтры
	if len(filter.CategoryIDs) > 0 {
		filterQueries = append(filterQueries, qb.buildCategoriesFilter(filter.CategoryIDs))
	}

	if filter.MinPrice != nil || filter.MaxPrice != nil {
		filterQueries = append(filterQueries, qb.buildPriceRangeFilter(filter.MinPrice, filter.MaxPrice))
	}

	if filter.DateFrom != nil || filter.DateTo != nil {
		filterQueries = append(filterQueries, qb.buildDateRangeFilter(filter.DateFrom, filter.DateTo))
	}

	if filter.Location != nil {
		filterQueries = append(filterQueries, qb.buildLocationFilter(*filter.Location))
	}

	if filter.Source != nil {
		filterQueries = append(filterQueries, qb.buildSourceFilter(*filter.Source))
	}

	if len(mustQueries) == 0 {
		mustQueries = append(mustQueries, map[string]any{
			"match_all": map[string]any{},
		})
	}

	boolQuery["bool"].(map[string]any)["must"] = mustQueries
	if len(filterQueries) > 0 {
		boolQuery["bool"].(map[string]any)["filter"] = filterQueries
	}

	query["query"] = boolQuery

	// Сортировка
	if filter.SortBy != "" {
		query["sort"] = qb.buildSortQuery(filter.SortBy, filter.SortOrder)
	}

	return query
}

func (qb *QueryBuilder) buildTextSearchQuery(searchText string) map[string]any {
	return map[string]any{
		"multi_match": map[string]any{
			"query":     searchText,
			"fields":    []string{"name^3", "description^2", "location^1"},
			"type":      "best_fields",
			"fuzziness": "AUTO",
		},
	}
}

func (qb *QueryBuilder) buildCategoriesFilter(categoryIDs []int64) map[string]any {
	return map[string]any{
		"terms": map[string]any{
			"category_id": categoryIDs,
		},
	}
}

func (qb *QueryBuilder) buildPriceRangeFilter(minPrice, maxPrice *float32) map[string]any {
	rangeQuery := map[string]any{}

	if minPrice != nil {
		rangeQuery["gte"] = *minPrice
	}
	if maxPrice != nil {
		rangeQuery["lte"] = *maxPrice
	}

	return map[string]any{
		"range": map[string]any{
			"price": rangeQuery,
		},
	}
}

func (qb *QueryBuilder) buildDateRangeFilter(dateFrom, dateTo *time.Time) map[string]any {
	rangeQuery := map[string]any{}

	if dateFrom != nil {
		rangeQuery["gte"] = dateFrom.Format("2006-01-02")
	}
	if dateTo != nil {
		rangeQuery["lte"] = dateTo.Format("2006-01-02")
	}

	return map[string]any{
		"range": map[string]any{
			"date": rangeQuery,
		},
	}
}

func (qb *QueryBuilder) buildLocationFilter(location string) map[string]any {
	return map[string]any{
		"term": map[string]any{
			"location.keyword": location,
		},
	}
}

func (qb *QueryBuilder) buildSourceFilter(source string) map[string]any {
	return map[string]any{
		"term": map[string]any{
			"source": source,
		},
	}
}

func (qb *QueryBuilder) buildSortQuery(sortBy, sortOrder string) []any {
	sortField := sortBy
	if sortOrder == "" {
		sortOrder = "desc"
	}

	// Для текстовых полей используем keyword версию
	if sortField == "name" || sortField == "location" {
		sortField += ".keyword"
	}

	return []any{
		map[string]any{
			sortField: map[string]any{
				"order": sortOrder,
			},
		},
	}
}
