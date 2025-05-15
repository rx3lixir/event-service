package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/ianschenck/envflag"
	pb "github.com/rx3lixir/event-service/event-grpc/gen/go"
	"github.com/rx3lixir/event-service/event-grpc/server"
	"github.com/rx3lixir/event-service/internal/db"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	var (
		dbURL       = envflag.String("DB_URL", "postgres://rx3lixir:password@localhost:5432/aggregator?sslmode=disable", "Database connection URL")
		grpcSvcAddr = envflag.String("GRPC_PORT", "0.0.0.0:9091", "gRPC server address")
	)
	envflag.Parse()

	// Настраиваем логирование
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	// Создаем контекст, который можно отменить при получении сигнала остановки
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Настраиваем обработку сигналов для грациозного завершения
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signalCh
		slog.Info("Shutting down gracefully...")
		cancel()
	}()

	// Создаем пул соединений с базой данных
	pool, err := db.CreatePostgresPool(ctx, *dbURL)
	if err != nil {
		slog.Error("Failed to create postgres pool", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	slog.Info("Connected to database")

	// Создаем хранилище и gRPC сервер
	storer := db.NewPosgresStore(pool)
	srv := server.NewServer(storer)

	// Настраиваем gRPC сервер
	grpcServer := grpc.NewServer(
	// Здесь можно добавить перехватчики (interceptors) для логирования, трассировки и т.д.
	)
	pb.RegisterEventServiceServer(grpcServer, srv)

	// Включаем reflection API для gRPC (полезно для отладки)
	reflection.Register(grpcServer)

	// Запускаем gRPC сервер
	listener, err := net.Listen("tcp", *grpcSvcAddr)
	if err != nil {
		slog.Error("Failed to start listener", "error", err)
		os.Exit(1)
	}

	slog.Info("Server is listening", "address", *grpcSvcAddr)

	// Запускаем сервер в горутине
	serverError := make(chan error, 1)
	go func() {
		serverError <- grpcServer.Serve(listener)
	}()

	// Ждем либо завершения контекста (по сигналу), либо ошибки сервера
	select {
	case <-ctx.Done():
		grpcServer.GracefulStop()
		slog.Info("Server stopped gracefully")
	case err := <-serverError:
		slog.Error("Server error", "error", err)
	}
}
