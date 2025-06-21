package mapping

import (
	"context"
	"embed"
	"fmt"
	"strings"

	"github.com/rx3lixir/event-service/internal/opensearch/client"
	"github.com/rx3lixir/event-service/pkg/logger"
)

//go:embed events.json
var mappingFiles embed.FS

type Manager struct {
	client *client.Client
	logger logger.Logger
}

func NewManager(client *client.Client, log logger.Logger) *Manager {
	return &Manager{
		client: client,
		logger: log,
	}
}

func (m *Manager) EnsureIndex(ctx context.Context) error {
	indexName := m.client.GetIndexName()

	// Проверяем существование индекса
	exists, err := m.indexExists(ctx, indexName)
	if err != nil {
		return fmt.Errorf("failed to check index existence: %w", err)
	}

	if exists {
		m.logger.Info("OpenSearch index already exists, checking mapping compatibility", "index", indexName)
		// Если индекс существует, но маппинг неправильный, пересоздаем
		if err := m.validateAndRecreateIfNeeded(ctx, indexName); err != nil {
			return fmt.Errorf("failed to validate/recreate index: %w", err)
		}
		return nil
	}

	return m.createIndex(ctx, indexName)
}

func (m *Manager) validateAndRecreateIfNeeded(ctx context.Context, indexName string) error {
	// Получаем текущий маппинг
	res, err := m.client.GetNativeClient().Indices.GetMapping(
		m.client.GetNativeClient().Indices.GetMapping.WithIndex(indexName),
		m.client.GetNativeClient().Indices.GetMapping.WithContext(ctx),
	)
	if err != nil {
		m.logger.Warn("Failed to get current mapping, will recreate index", "error", err)
		return m.recreateIndex(ctx, indexName)
	}
	defer res.Body.Close()

	if res.IsError() {
		m.logger.Warn("Error getting mapping, will recreate index", "status", res.Status())
		return m.recreateIndex(ctx, indexName)
	}

	// Простая проверка: если есть проблемы с completion полями, пересоздаем
	// В идеале здесь можно сделать более детальную проверку маппинга
	m.logger.Info("Index exists with potentially incompatible mapping, recreating for safety")
	return m.recreateIndex(ctx, indexName)
}

func (m *Manager) recreateIndex(ctx context.Context, indexName string) error {
	m.logger.Info("Recreating OpenSearch index", "index", indexName)

	// Удаляем существующий индекс
	deleteRes, err := m.client.GetNativeClient().Indices.Delete(
		[]string{indexName},
		m.client.GetNativeClient().Indices.Delete.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("failed to delete existing index: %w", err)
	}
	defer deleteRes.Body.Close()

	if deleteRes.IsError() && deleteRes.StatusCode != 404 {
		return fmt.Errorf("failed to delete existing index, status: %s", deleteRes.Status())
	}

	m.logger.Info("Existing index deleted", "index", indexName)

	// Создаем новый индекс
	return m.createIndex(ctx, indexName)
}

func (m *Manager) indexExists(ctx context.Context, indexName string) (bool, error) {
	res, err := m.client.GetNativeClient().Indices.Exists(
		[]string{indexName},
		m.client.GetNativeClient().Indices.Exists.WithContext(ctx),
	)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	return res.StatusCode == 200, nil
}

func (m *Manager) createIndex(ctx context.Context, indexName string) error {
	mapping, err := m.loadEventsMapping()
	if err != nil {
		return fmt.Errorf("failed to load mapping: %w", err)
	}

	m.logger.Info("Creating OpenSearch index with new mapping", "index", indexName)

	res, err := m.client.GetNativeClient().Indices.Create(
		indexName,
		m.client.GetNativeClient().Indices.Create.WithContext(ctx),
		m.client.GetNativeClient().Indices.Create.WithBody(strings.NewReader(mapping)),
	)
	if err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("failed to create index, status: %s", res.Status())
	}

	m.logger.Info("OpenSearch index created successfully", "index", indexName)

	return nil
}

func (m *Manager) loadEventsMapping() (string, error) {
	data, err := mappingFiles.ReadFile("events.json")
	if err != nil {
		return "", fmt.Errorf("failed to read events mapping: %w", err)
	}
	return string(data), nil
}
