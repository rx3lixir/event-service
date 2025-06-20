package mapping

import (
	"context"
	"embed"
	"fmt"
	"strings"

	"github.com/rx3lixir/event-service/internal/opensearch/client"
	"github.com/rx3lixir/event-service/pkg/logger"
)

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
		m.logger.Info("OpenSearch index already exists", "index", indexName)
		return nil
	}

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

	res, err := m.client.GetNativeClient().Indices.Create(
		indexName,
		m.client.GetNativeClient().Indices.Create.WithContext(ctx),
		m.client.GetNativeClient().Indices.Create.WithBody(strings.NewReader(mapping)),
	)
	if err != nil {
		return fmt.Errorf("failed to crate index: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("failed to create index, status: %s", res.Status())
	}

	m.logger.Info("OpenSearch index created successfully", "index", "indexName")

	return nil
}

func (m *Manager) loadEventsMapping() (string, error) {
	data, err := mappingFiles.ReadFile("events.json")
	if err != nil {
		return "", fmt.Errorf("failed to read events mapping: %w", err)
	}
	return string(data), nil
}
