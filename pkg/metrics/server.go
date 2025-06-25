package metrics

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rx3lixir/event-service/pkg/logger"
)

// MetricsServer HTTP сервер для метрик Prometheus
type MetricsServer struct {
	server    *http.Server
	logger    logger.Logger
	startTime time.Time
}

// NewMetricsServer создает новый сервер метрик
func NewMetricsServer(port string, logger logger.Logger) *MetricsServer {
	if port == "" {
		port = ":8091"
	}

	mux := http.NewServeMux()

	// Основной эндпоинт для метрик
	mux.Handle("/metrics", promhttp.Handler())

	// Дополнительные эндпоинты для отладки
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("READY"))
	})

	server := &http.Server{
		Addr:         port,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return &MetricsServer{
		server:    server,
		logger:    logger,
		startTime: time.Now(),
	}
}

// Start запускает сервер метрик
func (ms *MetricsServer) Start() error {
	ms.logger.Info("Starting metrics server",
		"address", ms.server.Addr,
		"endpoints", []string{"/metrics", "/health", "/ready"},
	)

	if err := ms.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("metrics server failed: %w", err)
	}

	return nil
}

// Shutdown грациозно останавливает сервер
func (ms *MetricsServer) Shutdown(ctx context.Context) error {
	ms.logger.Info("Shutting down metrics server")
	return ms.server.Shutdown(ctx)
}

// GetUptime возвращает время работы сервера
func (ms *MetricsServer) GetUptime() time.Duration {
	return time.Since(ms.startTime)
}

// StartUptimeUpdater запускает горутину для обновления метрики uptime
func (ms *MetricsServer) StartUptimeUpdater(serviceName string) {
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				UpdateServiceUptime(serviceName, ms.startTime)
			}
		}
	}()
}
