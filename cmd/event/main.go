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
	"github.com/rx3lixir/event-service/internal/elasticsearch"
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
		"elasticsearch_url", c.Elasticsearch.URL,
		"elasticsearch_index", c.Elasticsearch.Index,
	)

	// Создаем пул соединений с базой данных
	pool, err := db.CreatePostgresPool(ctx, c.DB.DSN())
	if err != nil {
		log.Error("Failed to create postgres pool", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	log.Info("Connected to PostgreSQL database")

	// Создаем клиент Elasticsearch
	esClient, err := elasticsearch.NewClient(c.Elasticsearch, log)
	if err != nil {
		log.Error("Failed to create Elasticsearch client", "error", err)
		os.Exit(1)
	}

	// Проверяем подключение к Elasticsearch
	if err := esClient.Ping(ctx); err != nil {
		log.Error("Failed to ping Elasticsearch", "error", err)
		os.Exit(1)
	}
	log.Info("Connected to Elasticsearch")

	// Создаем индекс событий, если его нет
	if err := esClient.CreateIndex(ctx); err != nil {
		log.Error("Failed to create Elasticsearch index", "error", err)
		os.Exit(1)
	}

	// Создаем сервисы
	storer := db.NewPosgresStore(pool)
	esService := elasticsearch.NewService(esClient, log)

	// Создаем менеджера консистентности
	consistencyManager := consistency.New(storer, esService, log)

	// Инициализируем данные в Elasticsearch (синхронизация с PostgreSQL)
	if err := initializeElasticsearchData(ctx, storer, esService, log); err != nil {
		log.Error("Failed to initialize Elasticsearch data", "error", err)
		// Не завершаем работу, так как это не критическая ошибка
	}

	// Запускаем проверку консистентности после инициализации
	go func() {
		time.Sleep(10 * time.Second)
		if result, err := consistencyManager.CheckConsistency(ctx); err == nil {
			if !result.IsConsistent {
				log.Warn("Consistency check found issues",
					"missing_in_es", len(result.MissingInES),
					"missing_in_db", len(result.MissingInDB),
					"mismatches", len(result.Mismatches),
				)
			}
		}
	}()

	// Создаем gRPC сервер
	srv := server.NewServer(storer, esService, log)

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

	// Создаем HealthCheck сервер с проверкой Elasticsearch
	healthServer := health.NewServer(pool, consistencyManager, log,
		health.WithServiceName("event-service"),
		health.WithVersion("1.0.0"),
		health.WithPort(":8081"),
		health.WithTimeout(5*time.Second),
		health.WithRequiredTables("events", "categories"),
	)

	// Добавляем проверку Elasticsearch в health checker
	healthServer.AddElasticsearchCheck(esService)

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

// initializeElasticsearchData синхронизирует данные между PostgreSQL и Elasticsearch
func initializeElasticsearchData(ctx context.Context, storer *db.PostgresStore, esService *elasticsearch.Service, log logger.Logger) error {
	log.Info("Initializing Elasticsearch data from PostgreSQL...")

	// Получаем все события из PostgreSQL
	events, err := storer.GetEvents(ctx)
	if err != nil {
		return fmt.Errorf("failed to get events from PostgreSQL: %w", err)
	}

	if len(events) == 0 {
		log.Info("No events found in PostgreSQL, skipping Elasticsearch initialization")
		return nil
	}

	// Массово индексируем события в Elasticsearch
	if err := esService.BulkIndexEvents(ctx, events); err != nil {
		return fmt.Errorf("failed to bulk index events: %w", err)
	}

	log.Info("Elasticsearch initialization completed",
		"events_indexed", len(events),
	)

	return nil
}
