package suggestions

type QueryBuilder struct{}

func NewQueryBuilder() *QueryBuilder {
	return &QueryBuilder{}
}

func (qb *QueryBuilder) BuildSuggestionQuery(req *Request) map[string]any {
	suggest := make(map[string]any)

	// Используем completion suggester для каждого поля
	for _, field := range req.Fields {
		suggest[field+"_suggestion"] = map[string]any{
			"prefix": req.Query,
			"completion": map[string]any{
				"field":           field + ".completion",
				"size":            req.MaxResults,
				"skip_duplicates": true,
				"fuzzy": map[string]any{
					"fuzziness": "AUTO",
				},
			},
		}
	}

	query := map[string]any{
		"size":    0, // Не нужны документы, только suggestions
		"suggest": suggest,
		"_source": false,
	}

	// Добавляем fallback поиск через edge n-gram если completion не даст результатов
	query["query"] = qb.buildFallbackQuery(req)

	// Агрегация для получения уникальных значений как fallback
	if len(req.Fields) > 0 {
		query["aggs"] = qb.buildFallbackAggregation(req)
	}

	return query
}

func (qb *QueryBuilder) buildFallbackQuery(req *Request) map[string]any {
	fields := make([]string, len(req.Fields))
	for i, field := range req.Fields {
		fields[i] = field + ".suggest"
	}

	return map[string]any{
		"multi_match": map[string]any{
			"query":  req.Query,
			"fields": fields,
			"type":   "phrase_prefix",
		},
	}
}

func (qb *QueryBuilder) buildFallbackAggregation(req *Request) map[string]any {
	// Создаем агрегации для каждого поля
	aggs := make(map[string]any)

	for _, field := range req.Fields {
		aggs[field+"_terms"] = map[string]any{
			"terms": map[string]any{
				"field": field + ".keyword",
				"size":  req.MaxResults,
				"order": map[string]any{
					"_count": "desc",
				},
			},
		}
	}

	return aggs
}
