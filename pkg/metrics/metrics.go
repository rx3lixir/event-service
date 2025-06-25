package metrics

import (
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Метрики для gRPC запросов
var (
	// Счетчик всех gRPC запросов
	GrpcRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "grpc_requests_total",
			Help: "Total number of gRPC requests",
		},
		[]string{"service", "method", "status"},
	)

	// Гистограмма времени выполнения gRPC запросов
	GrpcRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "grpc_request_duration_seconds",
			Help:    "Duration of gRPC requests in seconds",
			Buckets: prometheus.DefBuckets, // 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10
		},
		[]string{"service", "method"},
	)

	// Количество активных подключений
	GrpcActiveConnections = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "grpc_active_connections",
			Help: "Number of active gRPC connections",
		},
		[]string{"service"},
	)
)

// Метрики для базы данных
var (
	// Счетчик database операций
	DatabaseOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "database_operations_total",
			Help: "Total number of database operations",
		},
		[]string{"operation", "table", "status"},
	)

	// Время выполнения database запросов
	DatabaseOperationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "database_operation_duration_seconds",
			Help:    "Duration of database operations in seconds",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		},
		[]string{"operation", "table"},
	)

	// Размер connection pool
	DatabasePoolConnections = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "database_pool_connections",
			Help: "Number of database pool connections",
		},
		[]string{"state"}, // active, idle, total
	)
)

// Метрики для OpenSearch
var (
	// Счетчик OpenSearch операций
	OpenSearchOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "opensearch_operations_total",
			Help: "Total number of OpenSearch operations",
		},
		[]string{"operation", "index", "status"},
	)

	// Время выполнения OpenSearch операций
	OpenSearchOperationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "opensearch_operation_duration_seconds",
			Help:    "Duration of OpenSearch operations in seconds",
			Buckets: []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		},
		[]string{"operation", "index"},
	)

	// Количество документов в индексе
	OpenSearchDocumentsTotal = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "opensearch_documents_total",
			Help: "Total number of documents in OpenSearch index",
		},
		[]string{"index"},
	)
)

// Бизнес метрики
var (
	// Общее количество событий
	EventsTotal = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "events_total",
			Help: "Total number of events in the system",
		},
		[]string{"category"},
	)

	// Счетчик созданных событий
	EventsCreatedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "events_created_total",
			Help: "Total number of events created",
		},
		[]string{"category", "source"},
	)

	// Счетчик поисковых запросов
	SearchRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "search_requests_total",
			Help: "Total number of search requests",
		},
		[]string{"type"}, // text_search, filter, suggestions
	)

	// Время выполнения поиска
	SearchDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "search_duration_seconds",
			Help:    "Duration of search operations",
			Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
		},
		[]string{"type"},
	)
)

// Системные метрики
var (
	// Информация о сервисе
	ServiceInfo = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "service_info",
			Help: "Information about the service",
		},
		[]string{"version", "service", "environment"},
	)

	// Время работы сервиса
	ServiceUptime = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "service_uptime_seconds",
			Help: "Service uptime in seconds",
		},
		[]string{"service"},
	)
)

// Хелперы для удобного использования метрик

// RecordGrpcRequest записывает метрику gRPC запроса
func RecordGrpcRequest(service, method, status string, duration time.Duration) {
	GrpcRequestsTotal.WithLabelValues(service, method, status).Inc()
	GrpcRequestDuration.WithLabelValues(service, method).Observe(duration.Seconds())
}

// RecordDatabaseOperation записывает метрику database операции
func RecordDatabaseOperation(operation, table, status string, duration time.Duration) {
	DatabaseOperationsTotal.WithLabelValues(operation, table, status).Inc()
	DatabaseOperationDuration.WithLabelValues(operation, table).Observe(duration.Seconds())
}

// RecordOpenSearchOperation записывает метрику OpenSearch операции
func RecordOpenSearchOperation(operation, index, status string, duration time.Duration) {
	OpenSearchOperationsTotal.WithLabelValues(operation, index, status).Inc()
	OpenSearchOperationDuration.WithLabelValues(operation, index).Observe(duration.Seconds())
}

// RecordSearchRequest записывает метрику поискового запроса
func RecordSearchRequest(searchType string, duration time.Duration) {
	SearchRequestsTotal.WithLabelValues(searchType).Inc()
	SearchDuration.WithLabelValues(searchType).Observe(duration.Seconds())
}

// SetServiceInfo устанавливает информацию о сервисе
func SetServiceInfo(version, service, environment string) {
	ServiceInfo.WithLabelValues(version, service, environment).Set(1)
}

// UpdateServiceUptime обновляет время работы сервиса
func UpdateServiceUptime(service string, startTime time.Time) {
	ServiceUptime.WithLabelValues(service).Set(time.Since(startTime).Seconds())
}

// UpdateDatabasePoolMetrics обновляет метрики connection pool
func UpdateDatabasePoolMetrics(active, idle, total int32) {
	DatabasePoolConnections.WithLabelValues("active").Set(float64(active))
	DatabasePoolConnections.WithLabelValues("idle").Set(float64(idle))
	DatabasePoolConnections.WithLabelValues("total").Set(float64(total))
}

// UpdateOpenSearchDocuments обновляет количество документов в индексе
func UpdateOpenSearchDocuments(index string, count int64) {
	OpenSearchDocumentsTotal.WithLabelValues(index).Set(float64(count))
}

// UpdateEventsTotal обновляет общее количество событий
func UpdateEventsTotal(category string, count int) {
	EventsTotal.WithLabelValues(category).Set(float64(count))
}

// RecordEventCreated записывает создание нового события
func RecordEventCreated(category, source string) {
	EventsCreatedTotal.WithLabelValues(category, source).Inc()
}

// StatusFromError возвращает статус на основе ошибки
func StatusFromError(err error) string {
	if err != nil {
		return "error"
	}
	return "success"
}

// StatusFromGrpcCode возвращает статус на основе gRPC кода
func StatusFromGrpcCode(code int) string {
	if code == 0 {
		return "ok"
	}
	return "error_" + strconv.Itoa(code)
}

// GetMethodName извлекает короткое имя метода из полного пути
func GetMethodName(fullMethod string) string {
	// Например: /event.EventService/CreateEvent -> CreateEvent
	if len(fullMethod) > 0 && fullMethod[0] == '/' {
		parts := strings.Split(fullMethod[1:], "/")
		if len(parts) >= 2 {
			return parts[1]
		}
	}
	return fullMethod
}
