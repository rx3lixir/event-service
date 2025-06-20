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

	// Строим поисковый запрос
	query := s.queryBuilder.BuildSearchQuery(filter)

	// Сериализуем запрос
	queryBody, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal search query: %w", err)
	}

	s.logger.Debug("Executing OpenSearch query",
		"query", string(queryBody),
		"index", s.client.GetIndexName(),
	)

	// Выполняем поиск
	start := time.Now()
	res, err := s.client.GetNativeClient().Search(
		s.client.GetNativeClient().Search.WithContext(ctx),
		s.client.GetNativeClient().Search.WithIndex(s.client.GetIndexName()),
		s.client.GetNativeClient().Search.WithBody(bytes.NewReader(queryBody)),
	)
	searchTime := time.Since(start)

	if err != nil {
		return nil, fmt.Errorf("failed to execute search: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("search failed with status: %s", res.Status())
	}

	// Парсим ответ
	searchResult, err := s.parseSearchResponse(res.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to parse search response: %w", err)
	}

	searchResult.SearchTime = searchTime.String()

	s.logger.Info("Search completed",
		"total_found", searchResult.Total,
		"returned", len(searchResult.Events),
		"search_time", searchTime,
		"max_score", searchResult.MaxScore,
		"has_query", filter.Query != "",
	)

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

	if err := json.NewDecoder(body).Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to decode search response: %w", err)
	}

	// Формируем результат
	events := make([]*models.EventDocument, 0, len(response.Hits.Hits))
	for _, hit := range response.Hits.Hits {
		events = append(events, &hit.Source)
	}

	return &models.SearchResult{
		Events:   events,
		Total:    response.Hits.Total.Value,
		MaxScore: response.Hits.MaxScore,
	}, nil
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
