package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	pb "github.com/rx3lixir/event-service/event-grpc/gen/go"
	"github.com/rx3lixir/event-service/event-grpc/server"
	"github.com/rx3lixir/event-service/internal/config"
	"github.com/rx3lixir/event-service/internal/db"
	"github.com/rx3lixir/event-service/internal/logger"
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
	go func() {
		<-signalCh
		log.Info("Shutting down gracefully...")
		cancel()
	}()

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

	// Запускаем сервер в горутине
	serverError := make(chan error, 1)
	go func() {
		serverError <- grpcServer.Serve(listener)
	}()

	// Ждем либо завершения контекста (по сигналу), либо ошибки сервера
	select {
	case <-ctx.Done():
		grpcServer.GracefulStop()
		log.Info("Server stopped gracefully")
	case err := <-serverError:
		log.Error("Server error", "error", err)
	}
}
