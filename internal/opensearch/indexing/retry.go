package indexing

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/rx3lixir/event-service/pkg/logger"
)

type RetryLogic struct {
	maxRetries    int
	baseDelay     time.Duration
	maxDelay      time.Duration
	backoffFactor float64
	logger        logger.Logger
}

func NewRetryLogic(logger logger.Logger) *RetryLogic {
	return &RetryLogic{
		maxRetries:    3,
		baseDelay:     time.Second,
		maxDelay:      time.Minute,
		backoffFactor: 2.0,
		logger:        logger,
	}
}

func (r *RetryLogic) ExecuteWithRetry(ctx context.Context, operation func(context.Context) error) error {
	var lastErr error

	for attempt := 1; attempt <= r.maxRetries; attempt++ {
		opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)

		err := operation(opCtx)
		cancel()

		if err == nil {
			if attempt > 1 {
				r.logger.Info("Operation succeded after retry",
					"attempt", attempt,
					"total_attempts", r.maxRetries,
				)
				return nil
			}
		}

		lastErr = err

		r.logger.Warn("Operation failed",
			"attempt", attempt,
			"max_retries", r.maxRetries,
			"error", err,
		)

		// Не делаем задержку после последней попытки
		if attempt < r.maxRetries {
			delay := r.calculateBackoffDelay(attempt)
			r.logger.Debug("Retrying after backoff",
				"delay", delay,
				"next_attempt", attempt+1,
			)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				// Продолжаем к след попытке
			}
		}
	}
	return fmt.Errorf("operation failed after %d attempts, last error: %w", r.maxRetries, lastErr)
}

func (r *RetryLogic) calculateBackoffDelay(attempt int) time.Duration {
	// Экспоненциальная задержка с джиттером
	delay := float64(r.baseDelay) * math.Pow(r.backoffFactor, float64(attempt-1))

	// Добавляем случайный джиттер ±25%
	jitter := 0.25
	jitterRange := delay * jitter
	jitteredDelay := delay + (2*jitterRange*math.Mod(float64(time.Now().UnixNano()), 1.0) - jitterRange)

	finalDelay := time.Duration(jitteredDelay)

	// Ограничиваем максимальную задержку
	if finalDelay > r.maxDelay {
		finalDelay = r.maxDelay
	}

	return finalDelay
}

// WithMaxRetries позволяет настроить количество повторов
func (r *RetryLogic) WithMaxRetries(maxRetries int) *RetryLogic {
	r.maxRetries = maxRetries
	return r
}

// WithBaseDelay позволяет настроить базовую задержку
func (r *RetryLogic) WithBaseDelay(delay time.Duration) *RetryLogic {
	r.baseDelay = delay
	return r
}
