package indexing

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/rx3lixir/event-service/internal/opensearch/client"
	"github.com/rx3lixir/event-service/internal/opensearch/models"
	"github.com/rx3lixir/event-service/pkg/logger"
)

type BulkOperations struct {
	client     *client.Client
	retryLogic *RetryLogic
	logger     logger.Logger
}

func NewBulkOperations(client *client.Client, retryLogic *RetryLogic, logger logger.Logger) *BulkOperations {
	return &BulkOperations{
		client:     client,
		retryLogic: retryLogic,
		logger:     logger,
	}
}

func (b *BulkOperations) BulkIndex(ctx context.Context, docs []*models.EventDocument) error {
	const maxBatchSize = 100

	// Разбиваем на батчи если необходимо
	for i := 0; i < len(docs); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(docs) {
			end = len(docs)
		}

		batch := docs[i:end]
		if err := b.processBatch(ctx, batch); err != nil {
			return fmt.Errorf("failed to process batch %d-%d: %w", i, end-1, err)
		}

		b.logger.Debug("Batch processed successfully",
			"batch_start", i,
			"batch_end", end-1,
			"batch_size", len(batch))
	}

	return nil
}

func (b *BulkOperations) processBatch(ctx context.Context, docs []*models.EventDocument) error {
	return b.retryLogic.ExecuteWithRetry(ctx, func(ctx context.Context) error {
		return b.executeBulkRequest(ctx, docs)
	})
}

func (b *BulkOperations) executeBulkRequest(ctx context.Context, docs []*models.EventDocument) error {
	body, err := b.buildBulkBody(docs)
	if err != nil {
		return fmt.Errorf("failed to build bulk body: %w", err)
	}

	res, err := b.client.GetNativeClient().Bulk(
		strings.NewReader(body),
		b.client.GetNativeClient().Bulk.WithContext(ctx),
		b.client.GetNativeClient().Bulk.WithRefresh("true"),
	)
	if err != nil {
		return fmt.Errorf("failed to execute bulk request: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("bulk request failed with status: %s", res.Status())
	}

	// Проверяем ответ на ошибки в отдельных операциях
	if err := b.checkBulkResponse(res.Body, len(docs)); err != nil {
		return fmt.Errorf("bulk response contains errors: %w", err)
	}

	return nil
}

func (b *BulkOperations) buildBulkBody(docs []*models.EventDocument) (string, error) {
	var buf bytes.Buffer

	for _, doc := range docs {
		// Action line
		actionLine := map[string]any{
			"index": map[string]any{
				"_index": b.client.GetIndexName(),
				"_id":    strconv.FormatInt(doc.ID, 10),
			},
		}

		actionBytes, err := json.Marshal(actionLine)
		if err != nil {
			return "", fmt.Errorf("failed to marshal action line: %w", err)
		}

		buf.Write(actionBytes)
		buf.WriteByte('\n')

		// Document line
		docData := doc.PrepareForIndex()
		docBytes, err := json.Marshal(docData)
		if err != nil {
			return "", fmt.Errorf("failed to marshal document: %w", err)
		}

		buf.Write(docBytes)
		buf.WriteByte('\n')
	}

	return buf.String(), nil
}

func (b *BulkOperations) checkBulkResponse(body io.Reader, expectedCount int) error {
	var response struct {
		Errors bool `json:"errors"`
		Items  []struct {
			Index struct {
				Status int `json:"status"`
				Error  *struct {
					Type   string `json:"type"`
					Reason string `json:"reason"`
				} `json:"error,omitempty"`
			} `json:"index"`
		} `json:"items"`
	}

	if err := json.NewDecoder(body).Decode(&response); err != nil {
		return fmt.Errorf("failed to decode bulk response: %w", err)
	}

	if !response.Errors {
		b.logger.Debug("Bulk operation completed successfully",
			"operations", len(response.Items))
		return nil
	}

	// Собираем ошибки
	var errors []string
	successCount := 0

	for i, item := range response.Items {
		if item.Index.Error != nil {
			errors = append(errors, fmt.Sprintf("item %d: %s - %s",
				i, item.Index.Error.Type, item.Index.Error.Reason))
		} else if item.Index.Status >= 200 && item.Index.Status < 300 {
			successCount++
		}
	}

	b.logger.Warn("Bulk operation completed with errors",
		"total_operations", len(response.Items),
		"successful", successCount,
		"failed", len(errors))

	if len(errors) == len(response.Items) {
		return fmt.Errorf("all bulk operations failed: %v", errors)
	}

	// Частичный успех - логируем предупреждение, но не возвращаем ошибку
	if len(errors) > 0 {
		b.logger.Warn("Some bulk operations failed", "errors", errors[:min(5, len(errors))])
	}

	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
