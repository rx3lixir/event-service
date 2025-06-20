package dataloader

import "time"

// SyncStatus представляет состояние синхронизации между PostgreSQL и OpenSearch
type SyncStatus struct {
	PostgreSQLCount int       `json:"postgresql_count"`
	OpenSearchCount int       `json:"opensearch_count"`
	InSync          bool      `json:"in_sync"`
	Difference      int       `json:"difference"`
	LastChecked     time.Time `json:"last_checked"`
}

// SyncConfig содержит настройки для синхронизации
type SyncConfig struct {
	BatchSize  int           `json:"batch_size"`
	MaxRetries int           `json:"max_retries"`
	RetryDelay time.Duration `json:"retry_delay"`
	ForceSync  bool          `json:"force_sync"`
	CheckOnly  bool          `json:"check_only"`
}

// DefaultSyncConfig возвращает конфигурацию по умолчанию
func DefaultSyncConfig() *SyncConfig {
	return &SyncConfig{
		BatchSize:  100,
		MaxRetries: 3,
		RetryDelay: time.Second,
		ForceSync:  false,
		CheckOnly:  false,
	}
}

// SyncResult содержит результаты операции синхронизации
type SyncResult struct {
	Success         bool          `json:"success"`
	EventsProcessed int           `json:"events_processed"`
	EventsSucceeded int           `json:"events_succeeded"`
	EventsFailed    int           `json:"events_failed"`
	Duration        time.Duration `json:"duration"`
	Error           string        `json:"error,omitempty"`
	StartedAt       time.Time     `json:"started_at"`
	CompletedAt     time.Time     `json:"completed_at"`
}
