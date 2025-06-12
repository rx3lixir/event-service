package health

import (
	"context"
	"encoding/json"

	"net/http"
	"sync"
	"time"
)

type Status string

const (
	StatusUp   Status = "UP"
	StatusDown Status = "DOWN"
)

// CheckHealth результат проверки компонента
type CheckResult struct {
	Status  Status         `json:"status"`
	Details map[string]any `json:"details,omitempty"`
	Error   string         `json:"error,omitempty"`
}

// Response ответ healthcheck endpoint
type Response struct {
	Status    Status                 `json:"status"`
	Timestamp time.Time              `json:"timestamp"`
	Service   string                 `json:"service"`
	Version   string                 `json:"version,omitempty"`
	Checks    map[string]CheckResult `json:"checks,omitempty"`
	Duration  string                 `json:"duration"`
}

// Checker интерфейс для проверки здоровья компонента
type Checker interface {
	Check(ctx context.Context) CheckResult
}

// CheckerFunc адаптер для использования функций как Checker
type CheckerFunc func(ctx context.Context) CheckResult

func (f CheckerFunc) Check(ctx context.Context) CheckResult {
	return f(ctx)
}

// Health основная структура для управления healthcheck
type Health struct {
	service  string
	version  string
	checkers map[string]Checker
	mu       sync.RWMutex
	timeout  time.Duration
}

// New создает новый экземпляр Health
func New(service, version string, opts ...Option) *Health {
	h := &Health{
		service:  service,
		version:  version,
		checkers: make(map[string]Checker),
		timeout:  5 * time.Second,
	}

	for _, opt := range opts {
		opt(h)
	}

	return h
}

// Option функция для настройки Health
type Option func(*Health)

// WithTimeout устанавливает таймаут для проверок
func WithTimeout(timeout time.Duration) Option {
	return func(h *Health) {
		h.timeout = timeout
	}
}

// AddCheckFunc добавляет проверку как функцию
func (h *Health) AddCheck(name string, checker Checker) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.checkers[name] = checker
}

// Check выполняет все проверки
func (h *Health) Check(ctx context.Context) Response {
	start := time.Now()

	// Контекст с таймаутом
	checkCtx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()

	h.mu.RLock()

	checkers := make(map[string]Checker, len(h.checkers))

	for name, checker := range h.checkers {
		checkers[name] = checker
	}

	h.mu.RUnlock()

	// Параллельное выполнение всех проверок
	results := make(map[string]CheckResult)

	resChan := make(chan struct {
		name   string
		result CheckResult
	}, len(checkers))

	var wg sync.WaitGroup

	for name, checker := range checkers {
		wg.Add(1)
		go func(n string, c Checker) {
			defer wg.Done()

			// Запускаем проверку в горутине с обработкой паники
			result := h.safeCheck(checkCtx, c)
			resChan <- struct {
				name   string
				result CheckResult
			}{n, result}
		}(name, checker)
	}

	// Ждем завершения всех проверок
	wg.Wait()
	close(resChan)

	// Собираем результаты
	overallStatus := StatusUp

	for r := range resChan {
		results[r.name] = r.result
		if r.result.Status == StatusDown {
			overallStatus = StatusDown
		}
	}

	return Response{
		Status:    overallStatus,
		Timestamp: time.Now(),
		Service:   h.service,
		Version:   h.version,
		Checks:    results,
		Duration:  time.Since(start).String(),
	}
}

// safeCheck безопасно выполняет проверку с обработкой паники
func (h *Health) safeCheck(ctx context.Context, checker Checker) (result CheckResult) {
	defer func() {
		if r := recover(); r != nil {
			result = CheckResult{
				Status: StatusDown,
				Error:  "Panic during health check",
			}
		}
	}()

	return checker.Check(ctx)
}

// Handler возвращает HTTP handler для health эндпоинта
func (h *Health) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := h.Check(r.Context())

		// Устанавливаем статус код
		statusCode := http.StatusOK
		if response.Status == StatusDown {
			statusCode = http.StatusServiceUnavailable
		}

		// Отправляем ответ
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(statusCode)
		json.NewEncoder(w).Encode(response)
	}
}

// ReadyHandler простой handler для проверки готовности
func (h *Health) ReadyHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Быстрая проверка - сервис запущен и готов принимать запросы
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("READY"))
	}
}
