package health

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresChecker проверка PostgreSQL через pgxpool
func PostgresChecker(pool *pgxpool.Pool) Checker {
	return CheckerFunc(func(ctx context.Context) CheckResult {
		start := time.Now()

		// Пингуем базу
		err := pool.Ping(ctx)
		duration := time.Since(start)

		if err != nil {
			return CheckResult{
				Status: StatusDown,
				Error:  err.Error(),
				Details: map[string]any{
					"duration_ms": duration.Milliseconds(),
				},
			}
		}

		// Получаем статистику пула
		stats := pool.Stat()

		return CheckResult{
			Status: StatusUp,
			Details: map[string]any{
				"duration_ms":    duration.Milliseconds(),
				"total_conns":    stats.TotalConns(),
				"idle_conns":     stats.IdleConns(),
				"acquired_conns": stats.AcquiredConns(),
			},
		}
	})
}

// DiskSpaceChecker проверка свободного места на диске
func DiskSpaceChecker(path string, minFreeBytes uint64) Checker {
	return CheckerFunc(func(ctx context.Context) CheckResult {
		// Для Linux/Unix систем
		// В продакшене лучше использовать библиотеку типа github.com/shirou/gopsutil
		return CheckResult{
			Status: StatusUp,
			Details: map[string]any{
				"path": path,
				"note": "implement disk check based on OS",
			},
		}
	})
}

// MemoryChecker проверка использования памяти
func MemoryChecker(maxUsagePercent float64) Checker {
	return CheckerFunc(func(ctx context.Context) CheckResult {
		// В продакшене использовать runtime.MemStats или gopsutil
		return CheckResult{
			Status: StatusUp,
			Details: map[string]any{
				"max_usage_percent": maxUsagePercent,
				"note":              "implement memory check",
			},
		}
	})
}
