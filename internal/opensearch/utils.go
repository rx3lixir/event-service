package opensearch

import (
	"sort"
	"strings"
)

func (s *Service) buildCompletionSuggesionQuery(req *SuggestionRequest) map[string]any {
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

	// Также добавляем поиск по edge n-gram для более гибких результатов
	query := map[string]any{
		"size":    0,
		"suggest": suggest,
		"_source": false,
	}

	// Если completion не даст результатов, используем fallback
	if len(req.Fields) > 0 {
		query["query"] = map[string]any{
			"multi_match": map[string]any{
				"query": req.Query,
				"fields": func() []string {
					fields := make([]string, len(req.Fields))
					for i, f := range req.Fields {
						fields[i] = f + ".suggest"
					}
					return fields
				}(),
				"type": "phrase_prefix",
			},
		}

		// Агрегация для получения уникальных значений
		query["aggs"] = map[string]any{
			"suggestions": map[string]any{
				"terms": map[string]any{
					"field": req.Fields[0] + ".keyword",
					"size":  req.MaxResults,
					"order": map[string]any{
						"_count": "desc",
					},
				},
			},
		}
	}

	return query
}

// extractCompletionSuggestions извлекает suggestions из ответа
func (s *Service) extractCompletionSuggestions(response map[string]interface{}, req *SuggestionRequest) []Suggestion {
	suggestions := make([]Suggestion, 0)
	seen := make(map[string]bool)

	// Извлекаем completion suggestions
	if suggest, ok := response["suggest"].(map[string]interface{}); ok {
		for fieldKey, fieldSuggestions := range suggest {
			field := strings.TrimSuffix(fieldKey, "_suggestion")

			if suggestionData, ok := fieldSuggestions.([]interface{}); ok && len(suggestionData) > 0 {
				if firstSuggestion, ok := suggestionData[0].(map[string]interface{}); ok {
					if options, ok := firstSuggestion["options"].([]interface{}); ok {
						for _, option := range options {
							if opt, ok := option.(map[string]interface{}); ok {
								text := opt["text"].(string)
								key := field + ":" + text

								if !seen[key] {
									seen[key] = true

									suggestion := Suggestion{
										Text:  text,
										Score: 1.0, // Completion suggestions имеют высокий score
										Type:  s.determineSuggestionType(field),
									}

									// Добавляем дополнительную информацию
									if field == "name" {
										if source, ok := opt["_source"].(map[string]interface{}); ok {
											if id, ok := source["id"].(float64); ok {
												eventID := int64(id)
												suggestion.EventID = &eventID
											}
											if catID, ok := source["category_id"].(float64); ok {
												suggestion.Category = getCategoryName(int64(catID))
											}
										}
									}

									suggestions = append(suggestions, suggestion)
								}
							}
						}
					}
				}
			}
		}
	}

	// Если completion не дал результатов, используем агрегации
	if len(suggestions) == 0 {
		if aggs, ok := response["aggregations"].(map[string]interface{}); ok {
			if suggAgg, ok := aggs["suggestions"].(map[string]interface{}); ok {
				if buckets, ok := suggAgg["buckets"].([]interface{}); ok {
					for _, bucket := range buckets {
						if b, ok := bucket.(map[string]interface{}); ok {
							if key, ok := b["key"].(string); ok {
								if strings.Contains(strings.ToLower(key), strings.ToLower(req.Query)) {
									suggestions = append(suggestions, Suggestion{
										Text:  key,
										Score: 0.5, // Меньший score для fallback
										Type:  "general",
									})
								}
							}
						}
					}
				}
			}
		}
	}

	// Сортируем по релевантности
	sort.Slice(suggestions, func(i, j int) bool {
		return suggestions[i].Score > suggestions[j].Score
	})

	// Ограничиваем количество
	if len(suggestions) > req.MaxResults {
		suggestions = suggestions[:req.MaxResults]
	}

	return suggestions
}

// determineSuggestionType определяет тип suggestion на основе поля
func (s *Service) determineSuggestionType(field string) string {
	switch field {
	case "name":
		return "event"
	case "location":
		return "location"
	case "category_name":
		return "category"
	default:
		return "general"
	}
}
