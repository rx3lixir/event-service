package opensearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/rx3lixir/event-service/internal/db"
	"github.com/rx3lixir/event-service/pkg/logger"
)

// Service представляет сервис для работы с событиями в OpenSearch
type Service struct {
	client *Client
	log    logger.Logger
}

// NewService создает новый сервис OpenSearch
func NewService(client *Client, log logger.Logger) *Service {
	return &Service{
		client: client,
		log:    log,
	}
}

// IndexEvent индексирует событие в OpenSearch
func (s *Service) IndexEvent(ctx context.Context, event *db.Event) error {
	// Конвертируем в документ OS
	doc := NewEventDocumentFromDB(event)

	// Сериализуем в JSON
	body, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("failed to marshal event document: %w", err)
	}

	// Индексируем документ используя низкоуровневый API
	res, err := s.client.GetClient().Index(
		s.client.GetIndex(),
		bytes.NewReader(body),
		s.client.GetClient().Index.WithDocumentID(strconv.FormatInt(event.Id, 10)), // Добавлено
		s.client.GetClient().Index.WithContext(ctx),
		s.client.GetClient().Index.WithRefresh("true"),
	)
	if err != nil {
		return fmt.Errorf("failed to index event: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("opensearch indexing failed: %s", res.Status())
	}

	s.log.Debug("Event indexed successfully",
		"event_id", event.Id,
		"index", s.client.GetIndex(),
		"status", res.Status(),
	)

	return nil
}

// UpdateEvent обновляет событие в OpenSearch
func (s *Service) UpdateEvent(ctx context.Context, event *db.Event) error {
	// Для простоты используем полное переиндексирование
	// В продакшене можно использовать partial update
	return s.IndexEvent(ctx, event)
}

// DeleteEvent удаляет событие из OpenSearch
func (s *Service) DeleteEvent(ctx context.Context, eventID int64) error {
	res, err := s.client.GetClient().Delete(
		s.client.GetIndex(),
		strconv.FormatInt(eventID, 10),
		s.client.GetClient().Delete.WithContext(ctx),
		s.client.GetClient().Delete.WithRefresh("true"),
	)
	if err != nil {
		return fmt.Errorf("failed to delete event: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() && res.StatusCode != 404 {
		return fmt.Errorf("opensearch deletion failed: %s", res.Status())
	}

	s.log.Debug("Event deleted from opensearch",
		"event_id", eventID,
		"status", res.Status(),
	)

	return nil
}

// SearchEvents ищет события в OpenSearch
func (s *Service) SearchEvents(ctx context.Context, filter *SearchFilter) (*SearchResult, error) {
	// Строим поисковый запрос
	query := s.buildSearchQuery(filter)

	// Сериализуем запрос
	queryBody, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal search query: %w", err)
	}

	s.log.Debug("OpenSearch search query", "query", string(queryBody))

	// Выполняем поиск
	start := time.Now()
	res, err := s.client.GetClient().Search(
		s.client.GetClient().Search.WithContext(ctx),
		s.client.GetClient().Search.WithIndex(s.client.GetIndex()),
		s.client.GetClient().Search.WithBody(bytes.NewReader(queryBody)),
	)
	searchTime := time.Since(start)

	if err != nil {
		return nil, fmt.Errorf("failed to execute search: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("failed to search via opensearch: %s", res.Status())
	}

	// Парсим ответ
	var response struct {
		Hits struct {
			Total struct {
				Value int64 `json:"value"`
			} `json:"total"`
			MaxScore *float64 `json:"max_score"`
			Hits     []struct {
				Source EventDocument `json:"_source"`
				Score  *float64      `json:"_score"`
			} `json:"hits"`
		} `json:"hits"`
	}

	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to decode search response: %w", err)
	}

	// Формируем результат
	events := make([]*EventDocument, 0, len(response.Hits.Hits))
	for _, hit := range response.Hits.Hits {
		events = append(events, &hit.Source)
	}

	result := &SearchResult{
		Events:     events,
		Total:      response.Hits.Total.Value,
		MaxScore:   response.Hits.MaxScore,
		SearchTime: searchTime.String(),
	}

	s.log.Info("Search completed",
		"total_found", result.Total,
		"returned", len(events),
		"search_time", searchTime,
		"max_score", result.MaxScore,
	)

	return result, nil
}

// buildSearchQuery строит запрос для OpenSearch (аналогично Elasticsearch)
func (s *Service) buildSearchQuery(filter *SearchFilter) map[string]any {
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

	// Фильтр по категориям
	if len(filter.CategoryIDs) > 0 {
		filterQueries = append(filterQueries, map[string]any{
			"terms": map[string]any{
				"category_id": filter.CategoryIDs,
			},
		})
	}

	// Полнотекстовый поиск
	if filter.Query != "" {
		mustQueries = append(mustQueries, map[string]any{
			"multi_match": map[string]any{
				"query":     filter.Query,
				"fields":    []string{"name^3", "description^2", "location^1"},
				"type":      "best_fields",
				"fuzziness": "AUTO",
			},
		})
	}

	// Фильтр по цене
	if filter.MinPrice != nil || filter.MaxPrice != nil {
		rangeQuery := map[string]any{}

		if filter.MinPrice != nil {
			rangeQuery["gte"] = *filter.MinPrice
		}
		if filter.MaxPrice != nil {
			rangeQuery["lte"] = *filter.MaxPrice
		}

		filterQueries = append(filterQueries, map[string]any{
			"range": map[string]any{
				"price": rangeQuery,
			},
		})
	}

	// Фильтр по датам
	if filter.DateFrom != nil || filter.DateTo != nil {
		rangeQuery := map[string]any{}

		if filter.DateFrom != nil {
			rangeQuery["gte"] = filter.DateFrom.Format("2006-01-02")
		}
		if filter.DateTo != nil {
			rangeQuery["lte"] = filter.DateTo.Format("2006-01-02")
		}

		filterQueries = append(filterQueries, map[string]any{
			"range": map[string]any{
				"date": rangeQuery,
			},
		})
	}

	// Фильтр по локации (точное совпадение)
	if filter.Location != nil {
		filterQueries = append(filterQueries, map[string]any{
			"term": map[string]any{
				"location.keyword": *filter.Location,
			},
		})
	}

	// Фильтр по источнику
	if filter.Source != nil {
		filterQueries = append(filterQueries, map[string]any{
			"term": map[string]any{
				"source": *filter.Source,
			},
		})
	}

	// Если нет поискового запроса, используем match_all
	if len(mustQueries) == 0 {
		mustQueries = append(mustQueries, map[string]any{
			"match_all": map[string]any{},
		})
	}

	// Собираем bool запрос
	boolQuery["bool"].(map[string]any)["must"] = mustQueries
	if len(filterQueries) > 0 {
		boolQuery["bool"].(map[string]any)["filter"] = filterQueries
	}

	query["query"] = boolQuery

	// Добавляем сортировку
	if filter.SortBy != "" {
		sortField := filter.SortBy
		sortOrder := filter.SortOrder

		if sortOrder == "" {
			sortOrder = "desc"
		}

		// Для текстовых полей используем keyword версию
		if sortField == "name" || sortField == "location" {
			sortField += ".keyword"
		}

		query["sort"] = []any{
			map[string]any{
				sortField: map[string]any{
					"order": sortOrder,
				},
			},
		}
	}

	return query
}

// BulkIndexEvents массово индексирует события
func (s *Service) BulkIndexEvents(ctx context.Context, events []*db.Event) error {
	if len(events) == 0 {
		return nil
	}

	var buf bytes.Buffer

	// Формируем bulk запрос
	for _, event := range events {
		// Action line
		actionLine := map[string]any{
			"index": map[string]any{
				"_index": s.client.GetIndex(),
				"_id":    strconv.FormatInt(event.Id, 10),
			},
		}

		actionBytes, err := json.Marshal(actionLine)
		if err != nil {
			return fmt.Errorf("failed to marshal action line: %w", err)
		}

		buf.Write(actionBytes)
		buf.WriteByte('\n')

		// Document line
		doc := NewEventDocumentFromDB(event)
		docBytes, err := json.Marshal(doc)
		if err != nil {
			return fmt.Errorf("failed to marshal document: %w", err)
		}

		buf.Write(docBytes)
		buf.WriteByte('\n')
	}

	// Выполняем bulk запрос
	res, err := s.client.GetClient().Bulk(
		strings.NewReader(buf.String()),
		s.client.GetClient().Bulk.WithContext(ctx),
		s.client.GetClient().Bulk.WithRefresh("true"),
	)
	if err != nil {
		return fmt.Errorf("failed to execute bulk request: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("bulk indexing failed: %s", res.Status())
	}

	s.log.Info("Bulk indexing completed",
		"events_count", len(events),
		"status", res.Status(),
	)

	return nil
}

// Health проверяет состояние OpenSearch
func (s *Service) Health(ctx context.Context) error {
	return s.client.Ping(ctx)
}
