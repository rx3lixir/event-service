package search

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/rx3lixir/event-service/internal/opensearch/client"
	"github.com/rx3lixir/event-service/internal/opensearch/models"
	"github.com/rx3lixir/event-service/pkg/logger"
)

type Searcher struct {
	client       *client.Client
	queryBuilder *QueryBuilder
	logger       logger.Logger
}

func NewSearcher(client *client.Client, logger logger.Logger) *Searcher {
	return &Searcher{
		client:       client,
		queryBuilder: NewQueryBuilder(),
		logger:       logger,
	}
}

func (s *Searcher) SearchEvents(ctx context.Context, filter *Filter) (*models.SearchResult, error) {
	if filter == nil {
		filter = NewFilter()
	}

	// Используем улучшенный запрос с релевантностью для поисковых запросов
	var query map[string]any
	if filter.Query != "" {
		query = s.queryBuilder.BuildSearchQueryWithRelevanceSort(filter)
		s.logger.Debug("Using relevance-based search for query", "query", filter.Query)
	} else {
		query = s.queryBuilder.BuildSearchQuery(filter)
		s.logger.Debug("Using standard search without query")
	}

	// Сериализуем запрос
	queryBody, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal search query: %w", err)
	}

	s.logger.Debug("Executing OpenSearch query",
		"query_text", filter.Query,
		"index", s.client.GetIndexName(),
		"from", filter.From,
		"size", filter.Size,
	)

	// Выполняем поиск
	start := time.Now()
	res, err := s.client.GetNativeClient().Search(
		s.client.GetNativeClient().Search.WithContext(ctx),
		s.client.GetNativeClient().Search.WithIndex(s.client.GetIndexName()),
		s.client.GetNativeClient().Search.WithBody(bytes.NewReader(queryBody)),
		s.client.GetNativeClient().Search.WithTrackTotalHits(true), // Важно для точного подсчета
	)
	searchTime := time.Since(start)

	if err != nil {
		return nil, fmt.Errorf("failed to execute search: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		// Читаем тело ошибки для диагностики
		body, _ := io.ReadAll(res.Body)
		s.logger.Error("OpenSearch query failed",
			"status", res.Status(),
			"error_body", string(body),
			"query", string(queryBody),
		)
		return nil, fmt.Errorf("search failed with status: %s", res.Status())
	}

	// Парсим ответ
	searchResult, err := s.parseSearchResponse(res.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to parse search response: %w", err)
	}

	searchResult.SearchTime = searchTime.String()

	// Логируем результаты с деталями
	s.logger.Info("Search completed",
		"query", filter.Query,
		"total_found", searchResult.Total,
		"returned", len(searchResult.Events),
		"search_time", searchTime,
		"max_score", searchResult.MaxScore,
		"has_query", filter.Query != "",
		"categories", filter.CategoryIDs,
	)

	// Добавим дебаг логи для первых результатов
	if filter.Query != "" && len(searchResult.Events) > 0 {
		s.logger.Debug("Top search results",
			"query", filter.Query,
			"first_result", searchResult.Events[0].Name,
			"first_score", s.getEventScore(searchResult.Events[0]),
		)
	}

	return searchResult, nil
}

func (s *Searcher) parseSearchResponse(body io.Reader) (*models.SearchResult, error) {
	var response struct {
		Hits struct {
			Total struct {
				Value int64 `json:"value"`
			} `json:"total"`
			MaxScore *float64 `json:"max_score"`
			Hits     []struct {
				Source models.EventDocument `json:"_source"`
				Score  *float64             `json:"_score"`
			} `json:"hits"`
		} `json:"hits"`
	}

	// Сохраняем тело ответа для возможной диагностики
	bodyBytes, err := io.ReadAll(body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if err := json.Unmarshal(bodyBytes, &response); err != nil {
		s.logger.Error("Failed to parse OpenSearch response",
			"error", err,
			"response_body", string(bodyBytes),
		)
		return nil, fmt.Errorf("failed to decode search response: %w", err)
	}

	// Формируем результат
	events := make([]*models.EventDocument, 0, len(response.Hits.Hits))
	for _, hit := range response.Hits.Hits {
		// Сохраняем score в событии для отладки
		event := hit.Source
		events = append(events, &event)

		// Логируем score для отладки
		if hit.Score != nil {
			s.logger.Debug("Event found",
				"name", event.Name,
				"score", *hit.Score,
				"id", event.ID,
			)
		}
	}

	return &models.SearchResult{
		Events:   events,
		Total:    response.Hits.Total.Value,
		MaxScore: response.Hits.MaxScore,
	}, nil
}

// Вспомогательный метод для получения score события (для отладки)
func (s *Searcher) getEventScore(event *models.EventDocument) string {
	// В реальности score не сохраняется в EventDocument
	// Это только для логирования, можно убрать в продакшене
	return "unknown"
}

// CountEvents возвращает только количество найденных документов
func (s *Searcher) CountEvents(ctx context.Context, filter *Filter) (int64, error) {
	if filter == nil {
		filter = NewFilter()
	}

	// Создаем count запрос (без пагинации)
	countFilter := *filter
	countFilter.From = 0
	countFilter.Size = 0

	query := s.queryBuilder.BuildSearchQuery(&countFilter)

	queryBody, err := json.Marshal(query)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal count query: %w", err)
	}

	res, err := s.client.GetNativeClient().Count(
		s.client.GetNativeClient().Count.WithContext(ctx),
		s.client.GetNativeClient().Count.WithIndex(s.client.GetIndexName()),
		s.client.GetNativeClient().Count.WithBody(bytes.NewReader(queryBody)),
	)
	if err != nil {
		return 0, fmt.Errorf("failed to execute count: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return 0, fmt.Errorf("count failed with status: %s", res.Status())
	}

	var countResponse struct {
		Count int64 `json:"count"`
	}

	if err := json.NewDecoder(res.Body).Decode(&countResponse); err != nil {
		return 0, fmt.Errorf("failed to decode count response: %w", err)
	}

	return countResponse.Count, nil
}
