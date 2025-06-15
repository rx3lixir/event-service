package consistency

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rx3lixir/event-service/internal/db"
	"github.com/rx3lixir/event-service/internal/opensearch"
	"github.com/rx3lixir/event-service/pkg/logger"
)

// Manager отвечает за проверку консистентности данных
// между PostgreSQL и OpenSearch
type Manager struct {
	store         *db.PostgresStore
	osService     *opensearch.Service // Заменили esService на osService
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
	TotalEventsOS int             `json:"total_events_os"`         // Заменили ES на OS
	MissingInOS   []int64         `json:"missing_in_os,omitempty"` // Заменили ES на OS
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
	OSValue string `json:"os_value"` // Заменили ES на OS
}

// New создает новый менеджер консистентности
func New(store *db.PostgresStore, osService *opensearch.Service, log logger.Logger) *Manager {
	return &Manager{
		store:         store,
		osService:     osService, // Заменили esService на osService
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

	// Получаем все события из OpenSearch
	filter := opensearch.NewSearchFilter().SetPagination(0, 10000)
	osResult, err := m.osService.SearchEvents(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to get events from opensearch: %w", err)
	}
	result.TotalEventsOS = int(osResult.Total)

	// Создаем мапы для быстрого поиска
	dbEventsMap := make(map[int64]*db.Event)
	for _, event := range dbEvents {
		dbEventsMap[event.Id] = event
	}

	// Создаем мапы для быстрого поиска
	osEventsMap := make(map[int64]*opensearch.EventDocument)
	for _, doc := range osResult.Events {
		osEventsMap[doc.ID] = doc
	}

	// Проверяем события, которые есть в БД, но нет в OS
	for id := range dbEventsMap {
		if _, exists := osEventsMap[id]; !exists {
			result.MissingInOS = append(result.MissingInOS, id)
			result.IsConsistent = false
		}
	}

	// Проверяем события, которые есть в OS, но нет в БД
	for id := range osEventsMap {
		if _, exists := dbEventsMap[id]; !exists {
			result.MissingInDB = append(result.MissingInDB, id)
			result.IsConsistent = false
		}
	}

	// Проверяем соответствие данных для событий, которые есть в обоих хранилищах
	for id, dbEvent := range dbEventsMap {
		if osDoc, exists := osEventsMap[id]; exists {
			mismatches := m.compareEvent(dbEvent, osDoc)
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
		"total_os", result.TotalEventsOS,
		"missing_in_os", len(result.MissingInOS),
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
		// Если события нет в БД, проверяем OS
		filter := opensearch.NewSearchFilter().
			SetQuery(fmt.Sprintf("id:%d", eventID)).
			SetPagination(0, 1)

		osResult, osErr := m.osService.SearchEvents(ctx, filter)
		if osErr == nil && osResult.Total > 0 {
			result.MissingInDB = []int64{eventID}
			result.IsConsistent = false
			result.TotalEventsOS = 1
		}

		result.CheckDuration = time.Since(start)
		return result, nil
	}

	result.TotalEventsDB = 1

	// Получаем событие из OS
	filter := opensearch.NewSearchFilter().
		SetQuery(fmt.Sprintf("id:%d", eventID)).
		SetPagination(0, 1)

	osResult, err := m.osService.SearchEvents(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to search event in opensearch: %w", err)
	}

	if osResult.Total == 0 || len(osResult.Events) == 0 {
		result.MissingInOS = []int64{eventID}
		result.IsConsistent = false
	} else {
		result.TotalEventsOS = 1
		// Сравниваем данные
		mismatches := m.compareEvent(dbEvent, osResult.Events[0])
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
		"missing_in_os", len(result.MissingInOS),
		"missing_in_db", len(result.MissingInDB),
		"mismatches", len(result.Mismatches),
	)

	var repairErrors []error

	// Синхронизируем события, отсутствующие в OS
	if len(result.MissingInOS) > 0 {
		for _, eventID := range result.MissingInOS {
			event, err := m.store.GetEventByID(ctx, eventID)
			if err != nil {
				repairErrors = append(repairErrors, fmt.Errorf("failed to get event %d from db: %w", eventID, err))
				continue
			}

			if err := m.osService.IndexEvent(ctx, event); err != nil {
				repairErrors = append(repairErrors, fmt.Errorf("failed to index event %d: %w", eventID, err))
			} else {
				m.log.Info("successfully indexed missing event", "event_id", eventID)
			}
		}
	}

	// Удаляем из OS события, которых нет в БД (осторожно!)
	if len(result.MissingInDB) > 0 {
		for _, eventID := range result.MissingInDB {
			if err := m.osService.DeleteEvent(ctx, eventID); err != nil {
				repairErrors = append(repairErrors, fmt.Errorf("failed to delete orphaned event %d from os: %w", eventID, err))
			} else {
				m.log.Info("successfully removed orphaned event from opensearch", "event_id", eventID)
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

		if err := m.osService.UpdateEvent(ctx, event); err != nil {
			repairErrors = append(repairErrors, fmt.Errorf("failed to update event %d in os: %w", mismatch.EventID, err))
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

// compareEvent сравнивает данные события из БД и OS
func (m *Manager) compareEvent(dbEvent *db.Event, osDoc *opensearch.EventDocument) []EventMismatch {
	var mismatches []EventMismatch

	// Сравниваем основные поля
	if dbEvent.Name != osDoc.Name {
		mismatches = append(mismatches, EventMismatch{
			EventID: dbEvent.Id,
			Field:   "name",
			DBValue: dbEvent.Name,
			OSValue: osDoc.Name,
		})
	}

	if dbEvent.Description != osDoc.Description {
		mismatches = append(mismatches, EventMismatch{
			EventID: dbEvent.Id,
			Field:   "description",
			DBValue: dbEvent.Description,
			OSValue: osDoc.Description,
		})
	}

	if dbEvent.CategoryID != osDoc.CategoryID {
		mismatches = append(mismatches, EventMismatch{
			EventID: dbEvent.Id,
			Field:   "category_id",
			DBValue: fmt.Sprintf("%d", dbEvent.CategoryID),
			OSValue: fmt.Sprintf("%d", osDoc.CategoryID),
		})
	}

	if dbEvent.Date != osDoc.Date {
		mismatches = append(mismatches, EventMismatch{
			EventID: dbEvent.Id,
			Field:   "date",
			DBValue: dbEvent.Date,
			OSValue: osDoc.Date,
		})
	}

	if dbEvent.Time != osDoc.Time {
		mismatches = append(mismatches, EventMismatch{
			EventID: dbEvent.Id,
			Field:   "time",
			DBValue: dbEvent.Time,
			OSValue: osDoc.Time,
		})
	}

	if dbEvent.Location != osDoc.Location {
		mismatches = append(mismatches, EventMismatch{
			EventID: dbEvent.Id,
			Field:   "location",
			DBValue: dbEvent.Location,
			OSValue: osDoc.Location,
		})
	}

	if dbEvent.Price != osDoc.Price {
		mismatches = append(mismatches, EventMismatch{
			EventID: dbEvent.Id,
			Field:   "price",
			DBValue: fmt.Sprintf("%.2f", dbEvent.Price),
			OSValue: fmt.Sprintf("%.2f", osDoc.Price),
		})
	}

	if dbEvent.Source != osDoc.Source {
		mismatches = append(mismatches, EventMismatch{
			EventID: dbEvent.Id,
			Field:   "source",
			DBValue: dbEvent.Source,
			OSValue: osDoc.Source,
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
