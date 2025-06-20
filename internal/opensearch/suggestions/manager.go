package suggestions

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/rx3lixir/event-service/internal/opensearch/client"
	"github.com/rx3lixir/event-service/pkg/logger"
)

type Manager struct {
	client       *client.Client
	queryBuilder *QueryBuilder
	parser       *ResponseParser
	logger       logger.Logger
}

func NewManager(client *client.Client, logger logger.Logger) *Manager {
	return &Manager{
		client:       client,
		queryBuilder: NewQueryBuilder(),
		parser:       NewResponseParser(logger),
		logger:       logger,
	}
}

func (m *Manager) GetSuggestions(ctx context.Context, req *Request) (*Response, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	// Строим запрос для suggestions
	query := m.queryBuilder.BuildSuggestionQuery(req)

	queryBody, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal suggestion query: %w", err)
	}

	m.logger.Debug("Executing suggestion query",
		"query", req.Query,
		"fields", req.Fields,
		"max_results", req.MaxResults)

	// Выполняем поиск
	res, err := m.client.GetNativeClient().Search(
		m.client.GetNativeClient().Search.WithContext(ctx),
		m.client.GetNativeClient().Search.WithIndex(m.client.GetIndexName()),
		m.client.GetNativeClient().Search.WithBody(bytes.NewReader(queryBody)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to execute suggestion search: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("suggestion search failed with status: %s", res.Status())
	}

	// Парсим ответ
	suggestions, err := m.parser.ParseSuggestionResponse(res.Body, req)
	if err != nil {
		return nil, fmt.Errorf("failed to parse suggestion response: %w", err)
	}

	m.logger.Info("Suggestions retrieved",
		"query", req.Query,
		"suggestions_count", len(suggestions))

	return &Response{
		Suggestions: suggestions,
		Query:       req.Query,
		Total:       len(suggestions),
	}, nil
}
