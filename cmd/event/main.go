package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	pb "github.com/rx3lixir/event-service/event-grpc/gen/go"
	"github.com/rx3lixir/event-service/event-grpc/server"
	"github.com/rx3lixir/event-service/internal/config"
	"github.com/rx3lixir/event-service/internal/db"
	"github.com/rx3lixir/event-service/internal/logger"
	"github.com/rx3lixir/event-service/pkg/health"
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

	// Создаем экземпляр логгера для передачи компонентам
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
	)

	// Создаем пул соединений с базой данных
	pool, err := db.CreatePostgresPool(ctx, c.DB.DSN())
	if err != nil {
		log.Error("Failed to create postgres pool", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	log.Info("Connected to database")

	// Создаем хранилище и gRPC сервер
	storer := db.NewPosgresStore(pool)
	srv := server.NewServer(storer, log)

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

	// Настраиваем health checks
	healthChecker := health.New("event-service", "1.0.0", health.WithTimeout(3*time.Second))

	// Добавляем проверку базы данных
	healthChecker.AddCheck("database", health.PostgresChecker(pool))

	// Запускаем HTTP сервер для health checks
	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/health", healthChecker.Handler())
	healthMux.HandleFunc("/ready", healthChecker.ReadyHandler())

	// Добавляем liveness probe (просто отвечает 200 OK)
	healthMux.HandleFunc("/live", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ALIVE"))
	})

	// HTTP сервер на порту 8081
	healthServer := &http.Server{
		Addr:    ":8081",
		Handler: healthMux,
	}

	var wg sync.WaitGroup

	// Запускаем HTTP сервер в горутине
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Info("Starting health check server", "address", healthServer.Addr)
		if err := healthServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("Health check server error", "error", err)
		}
	}()

	// Запускаем gRPC сервер в горутине
	wg.Add(1)
	serverError := make(chan error, 1)
	go func() {
		defer wg.Done()
		serverError <- grpcServer.Serve(listener)
	}()

	// Ждем либо завершения контекста (по сигналу), либо ошибки сервера
	select {
	case <-signalCh:
		log.Info("Shutting down gracefully...")
		cancel()

		// Останавливаем серверы
		grpcServer.GracefulStop()
		if err := healthServer.Shutdown(context.Background()); err != nil {
			log.Error("HTTP server shutdown error", "error", err)
		}

	case err := <-serverError:
		log.Error("Server error", "error", err)
		if err := healthServer.Shutdown(context.Background()); err != nil {
			log.Error("HTTP server shutdown error", "error", err)
		}
	}

	// Ждем завершения всех горутин
	wg.Wait()
	log.Info("Server stopped gracefully")
}
