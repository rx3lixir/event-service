package indexing

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/rx3lixir/event-service/internal/db"
	"github.com/rx3lixir/event-service/internal/opensearch/client"
	"github.com/rx3lixir/event-service/internal/opensearch/models"
	"github.com/rx3lixir/event-service/pkg/logger"
)

type Manager struct {
	client     *client.Client
	bulkOps    *BulkOperations
	retryLogic *RetryLogic
	logger     logger.Logger
}

func NewManager(client *client.Client, logger logger.Logger) *Manager {
	retryLogic := NewRetryLogic(logger)

	return &Manager{
		client:     client,
		bulkOps:    NewBulkOperations(client, retryLogic, logger),
		retryLogic: retryLogic,
		logger:     logger,
	}
}

func (m *Manager) IndexEvent(ctx context.Context, event *db.Event) error {
	doc := models.FromDBEvent(event)
	if err := doc.ValidateForIndexing(); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	return m.retryLogic.ExecuteWithRetry(ctx, func(context.Context) error {
		return m.indexSingleEvent(ctx, doc)
	})
}

func (m *Manager) UpdateEvent(ctx context.Context, event *db.Event) error {
	// Пока что полное индексирование
	return m.IndexEvent(ctx, event)
}

func (m *Manager) DeleteEvent(ctx context.Context, eventID int64) error {
	return m.retryLogic.ExecuteWithRetry(ctx, func(ctx context.Context) error {
		return m.deleteSingleEvent(ctx, eventID)
	})
}

func (m *Manager) BulkIndexEvents(ctx context.Context, events []*db.Event) error {
	if len(events) == 0 {
		return nil
	}

	docs := models.FromDBEvents(events)

	// Валидируем все документы
	for i, doc := range docs {
		if err := doc.ValidateForIndexing(); err != nil {
			return fmt.Errorf("validation failed for event %d: %w", i, err)
		}
	}

	return m.bulkOps.BulkIndex(ctx, docs)
}

func (m *Manager) indexSingleEvent(ctx context.Context, doc *models.EventDocument) error {
	docData := doc.PrepareForIndex()

	body, err := json.Marshal(docData)
	if err != nil {
		return fmt.Errorf("failed to marshal document: %w", err)
	}

	res, err := m.client.GetNativeClient().Index(
		m.client.GetIndexName(),
		bytes.NewReader(body),
		m.client.GetNativeClient().Index.WithDocumentID(strconv.FormatInt(doc.ID, 10)),
		m.client.GetNativeClient().Index.WithContext(ctx),
		m.client.GetNativeClient().Index.WithRefresh("true"),
	)
	if err != nil {
		return fmt.Errorf("failed to index document: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("indexing failed with status: %s", res.Status())
	}

	m.logger.Debug("Document indexed successfully",
		"event_id", doc.ID,
		"index", m.client.GetIndexName(),
	)

	return nil
}

func (m *Manager) deleteSingleEvent(ctx context.Context, eventID int64) error {
	res, err := m.client.GetNativeClient().Delete(
		m.client.GetIndexName(),
		strconv.FormatInt(eventID, 10),
		m.client.GetNativeClient().Delete.WithContext(ctx),
		m.client.GetNativeClient().Delete.WithRefresh("true"),
	)
	if err != nil {
		return fmt.Errorf("failed to delete document: %w", err)
	}
	defer res.Body.Close()

	// 404 не считается ошибкой при удалении
	if res.IsError() && res.StatusCode != 404 {
		return fmt.Errorf("deletion failed with status: %s", res.Status())
	}

	m.logger.Debug("Document deleted from opensearch",
		"event_id", eventID,
		"status", res.Status(),
	)

	return nil
}
