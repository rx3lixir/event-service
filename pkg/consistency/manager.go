package consistency

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rx3lixir/event-service/internal/db"
	"github.com/rx3lixir/event-service/internal/elasticsearch"
	"github.com/rx3lixir/event-service/pkg/logger"
)

// Manager отвечает за проверку консистентности данных
// между PostgreSQL и Elasticsearch
type Manager struct {
	store         *db.PostgresStore
	esService     *elasticsearch.Service
	log           logger.Logger
	mu            sync.RWMutex
	lastCheck     *CheckResult
	lastCheckTime time.Time
	checkCacheTTL time.Duration
}

// CheckResult результат проверки консистентности
type CheckResult struct {
	IsConsistent  bool            `json:"is_consistent"`
	TotalEventsDB int             `json:"total_events_db"`
	TotalEventsES int             `json:"total_events_es"`
	MissingInES   []int64         `json:"missing_in_es,omitempty"`
	MissingInDB   []int64         `json:"missing_in_db,omitempty"`
	Mismatches    []EventMismatch `json:"mismatches,omitempty"`
	CheckDuration time.Duration   `json:"check_duration"`
	Timestamp     time.Time       `json:"timestamp"`
}

// EventMismatch описывает несоответствия между данными
type EventMismatch struct {
	EventID int64  `json:"event_id"`
	Field   string `json:"field"`
	DBValue string `json:"db_value"`
	ESValue string `json:"es_value"`
}

// New создает новый менеджер консистентности
func New(store *db.PostgresStore, esService *elasticsearch.Service, log logger.Logger) *Manager {
	return &Manager{
		store:         store,
		esService:     esService,
		log:           log,
		checkCacheTTL: 1 * time.Minute,
	}
}

// CheckConsistency выполняет полную проверку консистентности данных
func (m *Manager) CheckConsistency(ctx context.Context) (*CheckResult, error) {
	if result := m.getCachedResult(); result != nil {
		m.log.Debug("returning cached consistency check result")
		return result, nil
	}

	m.log.Info("starting consistency check")
	start := time.Now()

	result := &CheckResult{
		Timestamp:    start,
		IsConsistent: true,
	}

	// Получаем все события из PostgreSQL
	dbEvents, err := m.store.GetEvents(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get events from database: %w", err)
	}
	result.TotalEventsDB = len(dbEvents)

	// Получаем все события из PostgreSQL
	filter := elasticsearch.NewSearchFilter().SetPagination(0, 10000)
	esResult, err := m.esService.SearchEvents(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to get events from elasticsearch: %w", err)
	}
	result.TotalEventsES = int(esResult.Total)

	// Создаем мапы для быстрого поиска
	dbEventsMap := make(map[int64]*db.Event)
	for _, event := range dbEvents {
		dbEventsMap[event.Id] = event
	}

	// Создаем мапы для быстрого поиска
	esEventsMap := make(map[int64]*elasticsearch.EventDocument)
	for _, doc := range esResult.Events {
		esEventsMap[doc.ID] = doc
	}

	// Проверяем события, которые есть в БД, но нет в ES
	for id := range dbEventsMap {
		if _, exists := esEventsMap[id]; !exists {
			result.MissingInES = append(result.MissingInES, id)
			result.IsConsistent = false
		}
	}

	// Проверяем события, которые есть в ES, но нет в БД
	for id := range esEventsMap {
		if _, exists := dbEventsMap[id]; !exists {
			result.MissingInDB = append(result.MissingInDB, id)
			result.IsConsistent = false
		}
	}

	// Проверяем соответствие данных для событий, которые есть в обоих хранилищах
	for id, dbEvent := range dbEventsMap {
		if esDoc, exists := esEventsMap[id]; exists {
			mismatches := m.compareEvent(dbEvent, esDoc)
			if len(mismatches) > 0 {
				result.Mismatches = append(result.Mismatches, mismatches...)
				result.IsConsistent = false
			}
		}
	}

	result.CheckDuration = time.Since(start)

	// Кэшируем результат
	m.setCachedResult(result)

	m.log.Info("consistency check completed",
		"is_consistent", result.IsConsistent,
		"total_db", result.TotalEventsDB,
		"total_es", result.TotalEventsES,
		"missing_in_es", len(result.MissingInES),
		"missing_in_db", len(result.MissingInDB),
		"mismatches", len(result.Mismatches),
		"duration", result.CheckDuration,
	)

	return result, nil
}

// CheckEventConsistency проверяет консистентность конкретного события
func (m *Manager) CheckEventConsistency(ctx context.Context, eventID int64) (*CheckResult, error) {
	m.log.Debug("checking consistency for single event", "event_id", eventID)
	start := time.Now()

	result := &CheckResult{
		Timestamp:    start,
		IsConsistent: true,
	}

	// Получаем событие из БД
	dbEvent, err := m.store.GetEventByID(ctx, eventID)
	if err != nil {
		// Если события нет в БД, проверяем ES
		filter := elasticsearch.NewSearchFilter().
			SetQuery(fmt.Sprintf("id:%d", eventID)).
			SetPagination(0, 1)

		esResult, esErr := m.esService.SearchEvents(ctx, filter)
		if esErr == nil && esResult.Total > 0 {
			result.MissingInDB = []int64{eventID}
			result.IsConsistent = false
			result.TotalEventsES = 1
		}

		result.CheckDuration = time.Since(start)
		return result, nil
	}

	result.TotalEventsDB = 1

	// Получаем событие из ES
	filter := elasticsearch.NewSearchFilter().
		SetQuery(fmt.Sprintf("id:%d", eventID)).
		SetPagination(0, 1)

	esResult, err := m.esService.SearchEvents(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to search event in elasticsearch: %w", err)
	}

	if esResult.Total == 0 || len(esResult.Events) == 0 {
		result.MissingInES = []int64{eventID}
		result.IsConsistent = false
	} else {
		result.TotalEventsES = 1
		// Сравниваем данные
		mismatches := m.compareEvent(dbEvent, esResult.Events[0])
		if len(mismatches) > 0 {
			result.Mismatches = mismatches
			result.IsConsistent = false
		}
	}

	result.CheckDuration = time.Since(start)
	return result, nil
}

// RepairInconsistencies пытается исправить найденные несоответствия
func (m *Manager) RepairInconsistencies(ctx context.Context, result *CheckResult) error {
	if result.IsConsistent {
		m.log.Info("data is consistent, no repair needed")
		return nil
	}

	m.log.Info("starting data repair",
		"missing_in_es", len(result.MissingInES),
		"missing_in_db", len(result.MissingInDB),
		"mismatches", len(result.Mismatches),
	)

	var repairErrors []error

	// Синхронизируем события, отсутствующие в ES
	if len(result.MissingInES) > 0 {
		for _, eventID := range result.MissingInES {
			event, err := m.store.GetEventByID(ctx, eventID)
			if err != nil {
				repairErrors = append(repairErrors, fmt.Errorf("failed to get event %d from db: %w", eventID, err))
				continue
			}

			if err := m.esService.IndexEvent(ctx, event); err != nil {
				repairErrors = append(repairErrors, fmt.Errorf("failed to index event %d: %w", eventID, err))
			} else {
				m.log.Info("successfully indexed missing event", "event_id", eventID)
			}
		}
	}

	// Удаляем из ES события, которых нет в БД (осторожно!)
	if len(result.MissingInDB) > 0 {
		for _, eventID := range result.MissingInDB {
			if err := m.esService.DeleteEvent(ctx, eventID); err != nil {
				repairErrors = append(repairErrors, fmt.Errorf("failed to delete orphaned event %d from es: %w", eventID, err))
			} else {
				m.log.Info("successfully removed orphaned event from elasticsearch", "event_id", eventID)
			}
		}
	}

	// Обновляем события с несоответствиями
	processedEvents := make(map[int64]bool)
	for _, mismatch := range result.Mismatches {
		if processedEvents[mismatch.EventID] {
			continue
		}
		processedEvents[mismatch.EventID] = true

		event, err := m.store.GetEventByID(ctx, mismatch.EventID)
		if err != nil {
			repairErrors = append(repairErrors, fmt.Errorf("failed to get event %d for update: %w", mismatch.EventID, err))
			continue
		}

		if err := m.esService.UpdateEvent(ctx, event); err != nil {
			repairErrors = append(repairErrors, fmt.Errorf("failed to update event %d in es: %w", mismatch.EventID, err))
		} else {
			m.log.Info("successfully updated event with mismatches", "event_id", mismatch.EventID)
		}
	}

	if len(repairErrors) > 0 {
		return fmt.Errorf("repair completed with %d errors", len(repairErrors))
	}

	m.log.Info("data repair completed successfully")
	return nil
}

// compareEvent сравнивает данные события из БД и ES
func (m *Manager) compareEvent(dbEvent *db.Event, esDoc *elasticsearch.EventDocument) []EventMismatch {
	var mismatches []EventMismatch

	// Сравниваем основные поля
	if dbEvent.Name != esDoc.Name {
		mismatches = append(mismatches, EventMismatch{
			EventID: dbEvent.Id,
			Field:   "name",
			DBValue: dbEvent.Name,
			ESValue: esDoc.Name,
		})
	}

	if dbEvent.Description != esDoc.Description {
		mismatches = append(mismatches, EventMismatch{
			EventID: dbEvent.Id,
			Field:   "description",
			DBValue: dbEvent.Description,
			ESValue: esDoc.Description,
		})
	}

	if dbEvent.CategoryID != esDoc.CategoryID {
		mismatches = append(mismatches, EventMismatch{
			EventID: dbEvent.Id,
			Field:   "category_id",
			DBValue: fmt.Sprintf("%d", dbEvent.CategoryID),
			ESValue: fmt.Sprintf("%d", esDoc.CategoryID),
		})
	}

	if dbEvent.Date != esDoc.Date {
		mismatches = append(mismatches, EventMismatch{
			EventID: dbEvent.Id,
			Field:   "date",
			DBValue: dbEvent.Date,
			ESValue: esDoc.Date,
		})
	}

	if dbEvent.Time != esDoc.Time {
		mismatches = append(mismatches, EventMismatch{
			EventID: dbEvent.Id,
			Field:   "time",
			DBValue: dbEvent.Time,
			ESValue: esDoc.Time,
		})
	}

	if dbEvent.Location != esDoc.Location {
		mismatches = append(mismatches, EventMismatch{
			EventID: dbEvent.Id,
			Field:   "location",
			DBValue: dbEvent.Location,
			ESValue: esDoc.Location,
		})
	}

	if dbEvent.Price != esDoc.Price {
		mismatches = append(mismatches, EventMismatch{
			EventID: dbEvent.Id,
			Field:   "price",
			DBValue: fmt.Sprintf("%.2f", dbEvent.Price),
			ESValue: fmt.Sprintf("%.2f", esDoc.Price),
		})
	}

	if dbEvent.Source != esDoc.Source {
		mismatches = append(mismatches, EventMismatch{
			EventID: dbEvent.Id,
			Field:   "source",
			DBValue: dbEvent.Source,
			ESValue: esDoc.Source,
		})
	}

	return mismatches
}

// getCachedResult возвращает закэшированный результат, если он еще актуален
func (m *Manager) getCachedResult() *CheckResult {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.lastCheck == nil {
		return nil
	}

	if time.Since(m.lastCheckTime) > m.checkCacheTTL {
		return nil
	}

	return m.lastCheck
}

// setCachedResult сохраняет результат в кэш
func (m *Manager) setCachedResult(result *CheckResult) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.lastCheck = result
	m.lastCheckTime = time.Now()
}

// SetCacheTTL устанавливает время жизни кэша результатов
func (m *Manager) SetCacheTTL(ttl time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.checkCacheTTL = ttl
}
