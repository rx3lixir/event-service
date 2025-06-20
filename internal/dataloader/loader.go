package dataloader

import (
	"context"
	"fmt"
	"time"

	"github.com/rx3lixir/event-service/internal/db"
	"github.com/rx3lixir/event-service/internal/opensearch"
	"github.com/rx3lixir/event-service/internal/opensearch/search"
	"github.com/rx3lixir/event-service/pkg/logger"
)

type Loader struct {
	storer    *db.PostgresStore
	osService *opensearch.Service
	logger    logger.Logger
}

func NewLoader(storer *db.PostgresStore, osService *opensearch.Service, logger logger.Logger) *Loader {
	return &Loader{
		storer:    storer,
		osService: osService,
		logger:    logger,
	}
}

// InitializeOpenSearchData синхронизирует данные между PostgreSQL и OpenSearch
func (l *Loader) InitializeOpenSearchData(ctx context.Context) error {
	l.logger.Info("Initializing OpenSearch data from PostgreSQL...")

	// Получаем все события из PostgreSQL
	events, err := l.storer.GetEvents(ctx)
	if err != nil {
		return fmt.Errorf("failed to get events from PostgreSQL: %w", err)
	}

	if len(events) == 0 {
		l.logger.Info("No events found in PostgreSQL, skipping OpenSearch initialization")
		return nil
	}

	// Проверяем, есть ли уже данные в OpenSearch
	filter := search.NewFilter().SetPagination(0, 1)
	existingResult, err := l.osService.SearchEvents(ctx, filter)
	if err != nil {
		l.logger.Warn("Failed to check existing OpenSearch data, proceeding with initialization", "error", err)
	} else if existingResult.Total > 0 {
		l.logger.Info("OpenSearch already contains data, skipping bulk initialization",
			"existing_count", existingResult.Total)
		return nil
	}

	// Разбиваем на батчи для более надежной загрузки
	const batchSize = 100
	totalEvents := len(events)
	successCount := 0
	errorCount := 0

	for i := 0; i < totalEvents; i += batchSize {
		end := i + batchSize
		if end > totalEvents {
			end = totalEvents
		}

		batch := events[i:end]
		batchNum := (i / batchSize) + 1
		totalBatches := (totalEvents + batchSize - 1) / batchSize

		l.logger.Info("Processing batch",
			"batch", batchNum,
			"total_batches", totalBatches,
			"batch_size", len(batch))

		// Пытаемся загрузить батч с retry логикой
		if err := l.indexBatchWithRetry(ctx, batch, 3); err != nil {
			l.logger.Error("Failed to index batch after retries",
				"batch", batchNum,
				"batch_size", len(batch),
				"error", err)
			errorCount += len(batch)

			// Продолжаем с следующим батчем вместо полного прекращения
			continue
		}

		successCount += len(batch)
		l.logger.Debug("Batch indexed successfully",
			"batch", batchNum,
			"events_in_batch", len(batch))
	}

	// Проверяем результаты
	if errorCount > 0 {
		l.logger.Warn("OpenSearch initialization completed with errors",
			"total_events", totalEvents,
			"successfully_indexed", successCount,
			"failed_to_index", errorCount,
			"success_rate", fmt.Sprintf("%.1f%%", float64(successCount)/float64(totalEvents)*100))

		// Возвращаем ошибку только если все батчи провалились
		if successCount == 0 {
			return fmt.Errorf("failed to index any events: %d total failures", errorCount)
		}

		// Частичный успех - не возвращаем ошибку, но логируем предупреждение
		return nil
	}

	l.logger.Info("OpenSearch initialization completed successfully",
		"events_indexed", successCount,
		"total_batches", (totalEvents+batchSize-1)/batchSize)

	return nil
}

// indexBatchWithRetry пытается загрузить батч с повторными попытками
func (l *Loader) indexBatchWithRetry(ctx context.Context, events []*db.Event, maxRetries int) error {
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		// Создаем контекст с таймаутом для каждой попытки
		retryCtx, cancel := context.WithTimeout(ctx, time.Second*30)

		err := l.osService.BulkIndexEvents(retryCtx, events)
		cancel()

		if err == nil {
			if attempt > 1 {
				l.logger.Info("Batch indexed successfully after retry",
					"attempt", attempt,
					"events_count", len(events))
			}
			return nil
		}

		lastErr = err
		l.logger.Warn("Failed to index batch",
			"attempt", attempt,
			"max_retries", maxRetries,
			"events_count", len(events),
			"error", err)

		// Экспоненциальная задержка между попытками
		if attempt < maxRetries {
			backoffDuration := time.Duration(attempt*attempt) * time.Second
			l.logger.Debug("Retrying after backoff",
				"backoff_duration", backoffDuration,
				"next_attempt", attempt+1)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoffDuration):
				// Продолжаем к следующей попытке
			}
		}
	}

	return fmt.Errorf("failed after %d attempts, last error: %w", maxRetries, lastErr)
}

// CheckSyncStatus проверяет состояние синхронизации данных
func (l *Loader) CheckSyncStatus(ctx context.Context) (*SyncStatus, error) {
	// Получаем количество событий в PostgreSQL
	pgEvents, err := l.storer.GetEvents(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get PostgreSQL events count: %w", err)
	}
	pgCount := len(pgEvents)

	// Получаем количество событий в OpenSearch
	filter := search.NewFilter().SetPagination(0, 0) // Только count
	osResult, err := l.osService.SearchEvents(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to get OpenSearch events count: %w", err)
	}
	osCount := int(osResult.Total)

	status := &SyncStatus{
		PostgreSQLCount: pgCount,
		OpenSearchCount: osCount,
		InSync:          pgCount == osCount,
		Difference:      pgCount - osCount,
	}

	return status, nil
}

// ForceSyncData принудительно синхронизирует данные (пересоздает индекс)
func (l *Loader) ForceSyncData(ctx context.Context) error {
	l.logger.Info("Starting forced data synchronization...")

	// Получаем все события из PostgreSQL
	events, err := l.storer.GetEvents(ctx)
	if err != nil {
		return fmt.Errorf("failed to get events from PostgreSQL: %w", err)
	}

	l.logger.Info("Retrieved events from PostgreSQL", "count", len(events))

	// Пересоздаем индекс (это очистит старые данные)
	if err := l.osService.RecreateIndex(ctx); err != nil {
		return fmt.Errorf("failed to recreate OpenSearch index: %w", err)
	}

	if len(events) == 0 {
		l.logger.Info("No events to sync")
		return nil
	}

	// Загружаем все события
	const batchSize = 100
	for i := 0; i < len(events); i += batchSize {
		end := i + batchSize
		if end > len(events) {
			end = len(events)
		}

		batch := events[i:end]
		if err := l.indexBatchWithRetry(ctx, batch, 3); err != nil {
			return fmt.Errorf("failed to index batch %d-%d: %w", i, end-1, err)
		}

		l.logger.Debug("Synced batch", "from", i, "to", end-1)
	}

	l.logger.Info("Forced synchronization completed", "events_synced", len(events))
	return nil
}
