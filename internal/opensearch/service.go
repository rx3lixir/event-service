package opensearch

import (
	"context"
	"fmt"
	"time"

	"github.com/rx3lixir/event-service/internal/db"
	"github.com/rx3lixir/event-service/internal/opensearch/client"
	"github.com/rx3lixir/event-service/internal/opensearch/indexing"
	"github.com/rx3lixir/event-service/internal/opensearch/mapping"
	"github.com/rx3lixir/event-service/internal/opensearch/models"
	"github.com/rx3lixir/event-service/internal/opensearch/search"
	"github.com/rx3lixir/event-service/internal/opensearch/suggestions"
	"github.com/rx3lixir/event-service/pkg/logger"
)

type Service struct {
	client    *client.Client
	mapper    *mapping.Manager
	searcher  *search.Searcher
	indexer   *indexing.Manager
	suggester *suggestions.Manager
	logger    logger.Logger
}

func NewService(cfg *client.Config, logger logger.Logger) (*Service, error) {
	osClient, err := client.New(cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create opensearch client: %w", err)
	}

	return &Service{
		client:    osClient,
		mapper:    mapping.NewManager(osClient, logger),
		searcher:  search.NewSearcher(osClient, logger),
		indexer:   indexing.NewManager(osClient, logger),
		suggester: suggestions.NewManager(osClient, logger),
		logger:    logger,
	}, nil
}

func (s *Service) Initialize(ctx context.Context) error {
	// Проверяем подключение
	if err := s.Health(ctx); err != nil {
		return fmt.Errorf("opensearch health check failed: %w", err)
	}

	// Создаем индекс если нужно
	if err := s.mapper.EnsureIndex(ctx); err != nil {
		return fmt.Errorf("failed to ensure index: %w", err)
	}

	s.logger.Info("OpenSearch service initialized successfully")
	return nil
}

// RecreateIndex пересоздает индекс (удаляет и создает заново)
func (s *Service) RecreateIndex(ctx context.Context) error {
	s.logger.Info("Recreating OpenSearch index", "index", s.client.GetIndexName())

	// Удаляем существующий индекс
	if err := s.client.GetNativeClient().Indices.Delete.WithContext(ctx); err != nil {
		// Игнорируем ошибку если индекс не существует
		s.logger.Debug("Index deletion result", "error", err)
	}

	// Создаем новый индекс
	if err := s.client.GetNativeClient().Indices.Create.WithContext(ctx); err != nil {
		return fmt.Errorf("failed to create index after recreation: %w", err)
	}

	s.logger.Info("Index recreated successfully", "index", s.client.GetIndexName())
	return nil
}

// Поисковые операции
func (s *Service) SearchEvents(ctx context.Context, filter *search.Filter) (*models.SearchResult, error) {
	return s.searcher.SearchEvents(ctx, filter)
}

// Операции индексации
func (s *Service) IndexEvent(ctx context.Context, event *db.Event) error {
	return s.indexer.IndexEvent(ctx, event)
}

func (s *Service) UpdateEvent(ctx context.Context, event *db.Event) error {
	return s.indexer.UpdateEvent(ctx, event)
}

func (s *Service) DeleteEvent(ctx context.Context, eventID int64) error {
	return s.indexer.DeleteEvent(ctx, eventID)
}

func (s *Service) BulkIndexEvents(ctx context.Context, events []*db.Event) error {
	return s.indexer.BulkIndexEvents(ctx, events)
}

// Suggestions
func (s *Service) GetSuggestions(ctx context.Context, req *suggestions.Request) (*suggestions.Response, error) {
	return s.suggester.GetSuggestions(ctx, req)
}

// Health check
func (s *Service) Health(ctx context.Context) error {
	res, err := s.client.GetNativeClient().Ping(
		s.client.GetNativeClient().Ping.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("failed to ping opensearch: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("opensearch ping failed with status: %s", res.Status())
	}

	return nil
}

func (s *Service) WaitForHealthy(ctx context.Context, maxRetries int, retryInterval time.Duration) error {
	for i := 0; i < maxRetries; i++ {
		if err := s.Health(ctx); err == nil {
			return nil
		}

		if i < maxRetries-1 {
			time.Sleep(retryInterval)
		}
	}

	return fmt.Errorf("opensearch not healthy after %d retries", maxRetries)
}
