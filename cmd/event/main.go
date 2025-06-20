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
