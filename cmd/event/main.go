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
	"github.com/rx3lixir/event-service/internal/db"
	"github.com/rx3lixir/event-service/internal/opensearch"
	"github.com/rx3lixir/event-service/pkg/consistency"
	"github.com/rx3lixir/event-service/pkg/health"
	"github.com/rx3lixir/event-service/pkg/logger"

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

	// Создаем пул соединений с базой данных
	pool, err := db.CreatePostgresPool(ctx, c.DB.DSN())
	if err != nil {
		log.Error("Failed to create postgres pool", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	log.Info("Connected to PostgreSQL database")

	// Создаем клиент OpenSearch
	osClient, err := opensearch.NewClient(c.OpenSearch, log)
	if err != nil {
		log.Error("Failed to create OpenSearch client", "error", err)
		os.Exit(1)
	}

	// Проверяем подключение к OpenSearch
	if err := osClient.Ping(ctx); err != nil {
		log.Error("Failed to ping OpenSearch", "error", err)
		os.Exit(1)
	}
	log.Info("Connected to OpenSearch")

	// Создаем индекс событий, если его нет
	if err := osClient.CreateIndex(ctx); err != nil {
		log.Error("Failed to create OpenSearch index", "error", err)
		os.Exit(1)
	}

	// Создаем сервисы
	storer := db.NewPosgresStore(pool)
	osService := opensearch.NewService(osClient, log)

	// Создаем менеджера консистентности
	consistencyManager := consistency.New(storer, osService, log)

	// Инициализируем данные в OpenSearch (синхронизация с PostgreSQL)
	if err := initializeOpenSearchData(ctx, storer, osService, log); err != nil {
		log.Error("Failed to initialize OpenSearch data", "error", err)
		// Не завершаем работу, так как это не критическая ошибка
	}

	// Запускаем проверку консистентности после инициализации
	go func() {
		time.Sleep(10 * time.Second)
		if result, err := consistencyManager.CheckConsistency(ctx); err == nil {
			if !result.IsConsistent {
				log.Warn("Consistency check found issues",
					"missing_in_os", len(result.MissingInOS),
					"missing_in_db", len(result.MissingInDB),
					"mismatches", len(result.Mismatches),
				)
			}
		}
	}()

	// Создаем gRPC сервер
	srv := server.NewServer(storer, osService, log)

	// Настраиваем gRPC сервер
	grpcServer := grpc.NewServer(
	// Здесь можно добавить перехватчики (interceptors) для логирования, трассировки и т.д.
	)
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

	// Запускаем серверы
	errCh := make(chan error, 2)

	// Health check сервер
	go func() {
		errCh <- healthServer.Start()
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

	case err := <-errCh:
		log.Error("Server error", "error", err)

		grpcServer.GracefulStop()
		if err := healthServer.Shutdown(context.Background()); err != nil {
			log.Error("gRPC server shutdown error", "error", err)
		}
	}

	log.Info("Server stopped gracefully")
}

// initializeOpenSearchData синхронизирует данные между PostgreSQL и OpenSearch
func initializeOpenSearchData(ctx context.Context, storer *db.PostgresStore, osService *opensearch.Service, log logger.Logger) error {
	log.Info("Initializing OpenSearch data from PostgreSQL...")

	// Получаем все события из PostgreSQL
	events, err := storer.GetEvents(ctx)
	if err != nil {
		return fmt.Errorf("failed to get events from PostgreSQL: %w", err)
	}

	if len(events) == 0 {
		log.Info("No events found in PostgreSQL, skipping OpenSearch initialization")
		return nil
	}

	// Массово индексируем события в OpenSearch
	if err := osService.BulkIndexEvents(ctx, events); err != nil {
		return fmt.Errorf("failed to bulk index events: %w", err)
	}

	log.Info("OpenSearch initialization completed",
		"events_indexed", len(events),
	)

	return nil
}
