package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "github.com/rx3lixir/event-service/event-grpc/gen/go"

	"github.com/rx3lixir/event-service/event-grpc/server"
	"github.com/rx3lixir/event-service/internal/config"
	"github.com/rx3lixir/event-service/internal/dataloader"
	"github.com/rx3lixir/event-service/internal/db"
	"github.com/rx3lixir/event-service/internal/opensearch"
	"github.com/rx3lixir/event-service/internal/opensearch/client"
	"github.com/rx3lixir/event-service/pkg/consistency"
	"github.com/rx3lixir/event-service/pkg/health"
	"github.com/rx3lixir/event-service/pkg/logger"
	"github.com/rx3lixir/event-service/pkg/metrics"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	// Загрузка конфигурации
	c, err := config.New()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Ошибка загрузки конфигурации: %v\n", err)
		os.Exit(1)
	}

	// Инициализация логгера
	logger.Init(c.Service.Env)
	defer logger.Close()

	log := logger.NewLogger()

	// Создаем контекст, который можно отменить при получении сигнала остановки
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Настраиваем обработку сигналов для грациозного завершения
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)

	// Логируем конфигурацию для отладки
	log.Info("Configuration loaded",
		"env", c.Service.Env,
		"db_host", c.DB.Host,
		"db_port", c.DB.Port,
		"db_name", c.DB.DBName,
		"server_address", c.Server.Address,
		"opensearch_url", c.OpenSearch.URL,
		"opensearch_index", c.OpenSearch.Index,
	)

	// Инициализируем метрики
	metrics.SetServiceInfo("1.0.0", "event-service", c.Service.Env)
	log.Info("Metrics initialized", "service", "event-service", "version", "1.0.0")

	// Создаем пул соединений с базой данных
	pool, err := db.CreatePostgresPool(ctx, c.DB.DSN())
	if err != nil {
		log.Error("Failed to create postgres pool", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	log.Info("Connected to PostgreSQL database")

	// Создаем конфигурацию OpenSearch
	osConfig := client.DefaultConfig()
	osConfig.URL = c.OpenSearch.URL
	osConfig.IndexName = c.OpenSearch.Index
	osConfig.Timeout = c.OpenSearch.Timeout

	// Создаем OpenSearch сервис
	osService, err := opensearch.NewService(osConfig, log)
	if err != nil {
		log.Error("Failed to create OpenSearch service", "error", err)
		os.Exit(1)
	}

	// Инициализация OpenSearch
	if err := osService.Initialize(ctx); err != nil {
		log.Error("Failed to initialize OpenSearch", "error", err)
		os.Exit(1)
	}

	log.Info("OpenSearch initialized successfully")

	// Создаем сервисы
	storer := db.NewPosgresStore(pool)

	// Создаем dataloader для синхронизации данных
	loader := dataloader.NewLoader(storer, osService, log)

	// Синхронизация данных при старте
	if err := loader.InitializeOpenSearchData(ctx); err != nil {
		log.Error("Failed to init OpenSearch data", "error", err)
		os.Exit(1)
	}

	// Проверяем статус синхронизации
	syncStatus, err := loader.CheckSyncStatus(ctx)
	if err != nil {
		log.Warn("Failed to check sync status", "error", err)
	} else {
		log.Info("Data synchronization status",
			"postgresql_count", syncStatus.PostgreSQLCount,
			"opensearch_count", syncStatus.OpenSearchCount,
			"in_sync", syncStatus.InSync,
			"difference", syncStatus.Difference)
	}

	// Создаем менеджера консистентности
	consistencyManager := consistency.New(storer, osService, log)

	// Создаем gRPC сервер с interceptors для метрик
	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(metrics.UnaryServerInterceptor("event-service")),
		grpc.StreamInterceptor(metrics.StreamServerInterceptor("event-service")),
	)

	// Создаем gRPC сервер
	srv := server.NewServer(storer, osService, log)
	pb.RegisterEventServiceServer(grpcServer, srv)

	// Включаем reflection API для gRPC (полезно для отладки)
	reflection.Register(grpcServer)

	// Запускаем gRPC сервер
	listener, err := net.Listen("tcp", c.Server.Address)
	if err != nil {
		log.Error("Failed to start listener", "error", err)
		os.Exit(1)
	}

	log.Info("Server is listening", "address", c.Server.Address)

	// Создаем HealthCheck сервер с проверкой OpenSearch
	healthServer := health.NewServer(pool, consistencyManager, log,
		health.WithServiceName("event-service"),
		health.WithVersion("1.0.0"),
		health.WithPort(":8081"),
		health.WithTimeout(5*time.Second),
		health.WithRequiredTables("events", "categories"),
	)

	// Добавляем проверку OpenSearch в health checker
	healthServer.AddOpenSearchCheck(osService)

	// Создаем сервер метрик
	metricsServer := metrics.NewMetricsServer(":8080", log)

	// Запускаем фоновые задачи для обновления метрик
	startMetricsCollectors(ctx, storer, osService, log)

	// Запускаем серверы
	errCh := make(chan error, 3)

	// Health check сервер
	go func() {
		errCh <- healthServer.Start()
	}()

	// Metrics сервер
	go func() {
		errCh <- metricsServer.Start()
	}()

	// gRPC сервер
	go func() {
		errCh <- grpcServer.Serve(listener)
	}()

	// Ждем завершения
	select {
	case <-signalCh:
		log.Info("Shutting down gracefully...")

		// Останавливаем серверы
		grpcServer.GracefulStop()
		if err := healthServer.Shutdown(context.Background()); err != nil {
			log.Error("Health server shutdown error", "error", err)
		}
		if err := metricsServer.Shutdown(context.Background()); err != nil {
			log.Error("Metrics server shutdown error", "error", err)
		}

	case err := <-errCh:
		log.Error("Server error", "error", err)

		grpcServer.GracefulStop()
		if err := healthServer.Shutdown(context.Background()); err != nil {
			log.Error("Health server shutdown error", "error", err)
		}
		if err := metricsServer.Shutdown(context.Background()); err != nil {
			log.Error("Metrics server shutdown error", "error", err)
		}
	}

	log.Info("Server stopped gracefully")
}

// startMetricsCollectors запускает фоновые коллекторы метрик
func startMetricsCollectors(ctx context.Context, pool *db.PostgresStore, osService *opensearch.Service, log logger.Logger) {
	// Обновляем метрики connection pool каждые 30 секунд
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Здесь нужно получить статистику из pgxpool
				// Для этого нужно немного изменить структуру PostgresStore
				// чтобы она хранила ссылку на pool
				log.Debug("Updating database pool metrics")
			}
		}
	}()

	// Обновляем количество документов в OpenSearch каждые 60 секунд
	go func() {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Получаем количество документов в OpenSearch
				// Можно добавить метод в osService для этого
				log.Debug("Updating OpenSearch documents metrics")
			}
		}
	}()
}
