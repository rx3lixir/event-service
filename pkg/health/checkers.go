package health

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rx3lixir/event-service/pkg/consistency"
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

// MigrationChecker проверяет состояние миграций
func MigrationChecker(pool *pgxpool.Pool, expectedVersion int) Checker {
	return CheckerFunc(func(ctx context.Context) CheckResult {
		start := time.Now()

		// Проверяем, существует ли таблица schema_migrations
		var tableExists bool
		err := pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT FROM information_schema.tables 
				WHERE table_schema = 'public' 
				AND table_name = 'schema_migrations'
			)
		`).Scan(&tableExists)

		if err != nil {
			return CheckResult{
				Status: StatusDown,
				Error:  fmt.Sprintf("Failed to check migration table: %v", err),
				Details: map[string]any{
					"duration_ms": time.Since(start).Milliseconds(),
				},
			}
		}

		if !tableExists {
			return CheckResult{
				Status: StatusDown,
				Error:  "Migration table does not exist",
				Details: map[string]any{
					"duration_ms": time.Since(start).Milliseconds(),
					"note":        "Run migrations first",
				},
			}
		}

		// Получаем текущую версию миграций
		var currentVersion int
		var dirty bool
		err = pool.QueryRow(ctx, `
			SELECT version, dirty 
			FROM schema_migrations 
			ORDER BY version DESC 
			LIMIT 1
		`).Scan(&currentVersion, &dirty)

		if err != nil {
			return CheckResult{
				Status: StatusDown,
				Error:  fmt.Sprintf("Failed to get migration version: %v", err),
				Details: map[string]any{
					"duration_ms": time.Since(start).Milliseconds(),
				},
			}
		}

		// Проверяем, не в dirty состоянии ли миграции
		if dirty {
			return CheckResult{
				Status: StatusDown,
				Error:  "Database is in dirty migration state",
				Details: map[string]any{
					"duration_ms":     time.Since(start).Milliseconds(),
					"current_version": currentVersion,
					"dirty":           dirty,
					"note":            "Fix migration state before proceeding",
				},
			}
		}

		// Проверяем версию (опционально)
		status := StatusUp
		details := map[string]any{
			"duration_ms":     time.Since(start).Milliseconds(),
			"current_version": currentVersion,
			"dirty":           dirty,
		}

		if expectedVersion > 0 && currentVersion < expectedVersion {
			status = StatusDown
			details["error"] = fmt.Sprintf("Migration version too old. Expected: %d, Current: %d", expectedVersion, currentVersion)
			details["expected_version"] = expectedVersion
		} else if expectedVersion > 0 {
			details["expected_version"] = expectedVersion
			details["version_match"] = currentVersion >= expectedVersion
		}

		return CheckResult{
			Status:  status,
			Details: details,
		}
	})
}

// SimpleTableChecker проверяет существование критически важных таблиц
func SimpleTableChecker(pool *pgxpool.Pool, tables []string) Checker {
	return CheckerFunc(func(ctx context.Context) CheckResult {
		start := time.Now()

		missingTables := []string{}
		existingTables := []string{}

		for _, table := range tables {
			var exists bool
			err := pool.QueryRow(ctx, `
			SELECT EXISTS(
				SELECT * FROM information_schema.tables 
				WHERE table_schema = 'public'
				AND table_name = $1
			);
			`, table).Scan(&exists)

			if err != nil {
				return CheckResult{
					Status: StatusDown,
					Error:  fmt.Sprintf("Failed to check table %s: %v", table, err),
					Details: map[string]any{
						"duration_ms": time.Since(start).Milliseconds(),
						"table":       table,
					},
				}
			}

			if exists {
				existingTables = append(existingTables, table)
			} else {
				missingTables = append(missingTables, table)
			}
		}

		status := StatusUp
		details := map[string]any{
			"duration_ms":     time.Since(start).Milliseconds(),
			"existing_tables": existingTables,
			"total_checked":   len(tables),
		}

		if len(missingTables) > 0 {
			status = StatusDown
			details["missing_tables"] = missingTables
			details["error"] = fmt.Sprintf("Missing tables: %v", missingTables)
		}

		return CheckResult{
			Status:  status,
			Details: details,
		}
	})
}

func ConsistencyChecker(consManager *consistency.Manager, maxIncon int, timeout time.Duration) Checker {
	return CheckerFunc(func(ctx context.Context) CheckResult {
		checkctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		start := time.Now()

		// Выполняем проверку консистентности
		result, err := consManager.CheckConsistency(checkctx)
		if err != nil {
			return CheckResult{
				Status: StatusDown,
				Error:  err.Error(),
				Details: map[string]any{
					"duration_ms": time.Since(start).Milliseconds(),
				},
			}
		}

		// Определяем статус на основе результатов
		status := StatusUp
		details := map[string]any{
			"duration":        time.Since(start).Milliseconds(),
			"is_consistent":   result.IsConsistent,
			"total_events_db": result.TotalEventsDB,
			"total_events_es": result.TotalEventsOS,
			"check_timestamp": result.Timestamp,
		}

		// Подсчитываем общее кол-во проблем
		totalProblems := len(result.MissingInOS) + len(result.MissingInDB) + len(result.Mismatches)

		if totalProblems > 0 {
			details["total_problems"] = totalProblems
			details["problems"] = map[string]any{
				"missing_in_es": len(result.MissingInOS),
				"missing_in_db": len(result.MissingInDB),
				"mismatches":    len(result.Mismatches),
			}
		}

		if totalProblems > maxIncon {
			status = StatusDown
			details["error"] = "too many consistency issues"
			details["max_allowed"] = maxIncon
		}

		// Добавляем детали о первых нескольких проблемах
		if len(result.MissingInOS) > 0 {
			maxShow := 5
			if len(result.MissingInOS) < maxShow {
				maxShow = len(result.MissingInOS)
			}
			details["sample_missing_in_es"] = result.MissingInOS[:maxShow]
		}

		if len(result.MissingInDB) > 0 {
			maxShow := 5
			if len(result.MissingInDB) < maxShow {
				maxShow = len(result.MissingInDB)
			}
			details["sample_missing_in_db"] = result.MissingInDB[:maxShow]
		}

		return CheckResult{
			Status:  status,
			Details: details,
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
