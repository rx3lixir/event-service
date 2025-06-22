package search

import (
	"strings"
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
		mustQueries = append(mustQueries, qb.buildAdvancedTextSearchQuery(filter.Query))
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

// buildAdvancedTextSearchQuery - улучшенный поисковый запрос
func (qb *QueryBuilder) buildAdvancedTextSearchQuery(searchText string) map[string]any {
	// Очищаем поисковый запрос
	cleanQuery := strings.TrimSpace(searchText)

	// Создаем составной запрос с разными стратегиями поиска
	return map[string]any{
		"bool": map[string]any{
			"should": []any{
				// 1. Точное совпадение в названии (максимальный boost)
				map[string]any{
					"match_phrase": map[string]any{
						"name": map[string]any{
							"query": cleanQuery,
							"boost": 10.0,
						},
					},
				},
				// 2. Точное совпадение в описании
				map[string]any{
					"match_phrase": map[string]any{
						"description": map[string]any{
							"query": cleanQuery,
							"boost": 5.0,
						},
					},
				},
				// 3. Точное совпадение в локации
				map[string]any{
					"match_phrase": map[string]any{
						"location": map[string]any{
							"query": cleanQuery,
							"boost": 3.0,
						},
					},
				},
				// 4. Частичное совпадение с AND оператором (все слова должны быть)
				map[string]any{
					"multi_match": map[string]any{
						"query":    cleanQuery,
						"fields":   []string{"name^3", "description^2", "location^1"},
						"type":     "cross_fields",
						"operator": "and",
						"boost":    2.0,
					},
				},
				// 5. Частичное совпадение с OR оператором (хотя бы одно слово)
				map[string]any{
					"multi_match": map[string]any{
						"query":                cleanQuery,
						"fields":               []string{"name^2", "description^1.5", "location^1"},
						"type":                 "best_fields",
						"operator":             "or",
						"fuzziness":            "AUTO",
						"boost":                0.5,
						"minimum_should_match": "75%", // 75% слов должно совпадать
					},
				},
				// 6. Prefix поиск для автодополнения
				qb.buildPrefixSearch(cleanQuery),
			},
			"minimum_should_match": 1,
		},
	}
}

// buildPrefixSearch - поиск по началу слов
func (qb *QueryBuilder) buildPrefixSearch(query string) map[string]any {
	words := strings.Fields(query)
	if len(words) == 0 {
		return map[string]any{}
	}

	// Берем последнее слово для prefix поиска
	lastWord := words[len(words)-1]
	if len(lastWord) < 2 {
		return map[string]any{}
	}

	return map[string]any{
		"multi_match": map[string]any{
			"query":  lastWord,
			"fields": []string{"name.suggest^2", "location.suggest^1"},
			"type":   "phrase_prefix",
			"boost":  1.0,
		},
	}
}

// Остальные методы остаются без изменений
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

	// Если есть поисковый запрос, сначала сортируем по релевантности
	sort := []any{
		map[string]any{
			sortField: map[string]any{
				"order": sortOrder,
			},
		},
	}

	return sort
}

// Добавляем специальный метод для сортировки с учетом релевантности
func (qb *QueryBuilder) BuildSearchQueryWithRelevanceSort(filter *Filter) map[string]any {
	query := qb.BuildSearchQuery(filter)

	// Если есть поисковый запрос, сначала сортируем по score, потом по дате
	if filter.Query != "" {
		query["sort"] = []any{
			map[string]any{
				"_score": map[string]any{
					"order": "desc",
				},
			},
			map[string]any{
				"created_at": map[string]any{
					"order": "desc",
				},
			},
		}
	}

	return query
}
