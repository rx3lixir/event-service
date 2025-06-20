package suggestions

import (
	"encoding/json"
	"io"
	"sort"
	"strings"

	"github.com/rx3lixir/event-service/pkg/logger"
)

type ResponseParser struct {
	logger logger.Logger
}

func NewResponseParser(logger logger.Logger) *ResponseParser {
	return &ResponseParser{
		logger: logger,
	}
}

func (p *ResponseParser) ParseSuggestionResponse(body io.Reader, req *Request) ([]Suggestion, error) {
	var response map[string]interface{}
	if err := json.NewDecoder(body).Decode(&response); err != nil {
		return nil, err
	}

	suggestions := make([]Suggestion, 0)
	seen := make(map[string]bool)

	// Сначала пытаемся извлечь completion suggestions
	completionSuggestions := p.extractCompletionSuggestions(response, req, seen)
	suggestions = append(suggestions, completionSuggestions...)

	// Если completion не дал достаточно результатов, используем fallback
	if len(suggestions) < req.MaxResults {
		fallbackSuggestions := p.extractFallbackSuggestions(response, req, seen, req.MaxResults-len(suggestions))
		suggestions = append(suggestions, fallbackSuggestions...)
	}

	// Сортируем по релевантности
	sort.Slice(suggestions, func(i, j int) bool {
		return suggestions[i].Score > suggestions[j].Score
	})

	// Ограничиваем количество
	if len(suggestions) > req.MaxResults {
		suggestions = suggestions[:req.MaxResults]
	}

	p.logger.Debug("Parsed suggestions",
		"completion_suggestions", len(completionSuggestions),
		"total_suggestions", len(suggestions),
		"query", req.Query)

	return suggestions, nil
}

func (p *ResponseParser) extractCompletionSuggestions(response map[string]interface{}, req *Request, seen map[string]bool) []Suggestion {
	suggestions := make([]Suggestion, 0)

	suggest, ok := response["suggest"].(map[string]interface{})
	if !ok {
		return suggestions
	}

	for fieldKey, fieldSuggestions := range suggest {
		field := strings.TrimSuffix(fieldKey, "_suggestion")

		if !p.isValidField(field, req.Fields) {
			continue
		}

		suggestionData, ok := fieldSuggestions.([]interface{})
		if !ok || len(suggestionData) == 0 {
			continue
		}

		firstSuggestion, ok := suggestionData[0].(map[string]interface{})
		if !ok {
			continue
		}

		options, ok := firstSuggestion["options"].([]interface{})
		if !ok {
			continue
		}

		for _, option := range options {
			if suggestion := p.parseCompletionOption(option, field, seen); suggestion != nil {
				suggestions = append(suggestions, *suggestion)
			}
		}
	}

	return suggestions
}

func (p *ResponseParser) parseCompletionOption(option interface{}, field string, seen map[string]bool) *Suggestion {
	opt, ok := option.(map[string]interface{})
	if !ok {
		return nil
	}

	textVal, ok := opt["text"]
	if !ok {
		return nil
	}

	text, ok := textVal.(string)
	if !ok || text == "" {
		return nil
	}

	key := field + ":" + text
	if seen[key] {
		return nil
	}
	seen[key] = true

	suggestion := Suggestion{
		Text:  text,
		Score: 1.0, // Completion suggestions имеют высокий score
	}
	suggestion.SetType(field)

	// Добавляем дополнительную информацию если есть
	if source, ok := opt["_source"].(map[string]interface{}); ok {
		p.enrichSuggestionFromSource(&suggestion, source)
	}

	return &suggestion
}

func (p *ResponseParser) extractFallbackSuggestions(response map[string]interface{}, req *Request, seen map[string]bool, maxResults int) []Suggestion {
	suggestions := make([]Suggestion, 0)

	aggs, ok := response["aggregations"].(map[string]interface{})
	if !ok {
		return suggestions
	}

	for _, field := range req.Fields {
		aggKey := field + "_terms"

		fieldAgg, ok := aggs[aggKey].(map[string]interface{})
		if !ok {
			continue
		}

		buckets, ok := fieldAgg["buckets"].([]interface{})
		if !ok {
			continue
		}

		for _, bucket := range buckets {
			if len(suggestions) >= maxResults {
				break
			}

			if suggestion := p.parseFallbackBucket(bucket, field, req.Query, seen); suggestion != nil {
				suggestions = append(suggestions, *suggestion)
			}
		}
	}

	return suggestions
}

func (p *ResponseParser) parseFallbackBucket(bucket interface{}, field, query string, seen map[string]bool) *Suggestion {
	b, ok := bucket.(map[string]interface{})
	if !ok {
		return nil
	}

	keyVal, ok := b["key"]
	if !ok {
		return nil
	}

	key, ok := keyVal.(string)
	if !ok || key == "" {
		return nil
	}

	// Проверяем, что ключ содержит искомый запрос (case-insensitive)
	if !strings.Contains(strings.ToLower(key), strings.ToLower(query)) {
		return nil
	}

	mapKey := field + ":" + key
	if seen[mapKey] {
		return nil
	}
	seen[mapKey] = true

	suggestion := Suggestion{
		Text:  key,
		Score: 0.5, // Меньший score для fallback
	}
	suggestion.SetType(field)

	return &suggestion
}

func (p *ResponseParser) enrichSuggestionFromSource(suggestion *Suggestion, source map[string]interface{}) {
	// Добавляем ID события если это suggestion по имени
	if suggestion.Type == string(SuggestionTypeEvent) {
		if idVal, ok := source["id"]; ok {
			if id, ok := idVal.(float64); ok {
				eventID := int64(id)
				suggestion.EventID = &eventID
			}
		}

		// Добавляем категорию
		if catIDVal, ok := source["category_id"]; ok {
			if catID, ok := catIDVal.(float64); ok {
				suggestion.Category = p.getCategoryName(int64(catID))
			}
		}
	}
}

func (p *ResponseParser) isValidField(field string, validFields []string) bool {
	for _, validField := range validFields {
		if field == validField {
			return true
		}
	}
	return false
}

func (p *ResponseParser) getCategoryName(categoryID int64) string {
	// Здесь можно добавить кэш категорий или запрос к БД
	// Для простоты используем статичную мапу
	categoryMap := map[int64]string{
		1:  "Концерты",
		2:  "Театр",
		3:  "Кино",
		4:  "Выставки",
		5:  "Фестивали",
		6:  "Спорт",
		7:  "Детям",
		8:  "Экскурсии",
		9:  "Вечеринки",
		10: "Клубы",
	}

	if name, exists := categoryMap[categoryID]; exists {
		return name
	}
	return "Прочее"
}
