package main

import (
	"context"
	"log/slog"
	"net"
	"os"

	"github.com/ianschenck/envflag"
	"github.com/jackc/pgx/v5/pgxpool"
	pb "github.com/rx3lixir/event-service/event-grpc/gen/go"
	"github.com/rx3lixir/event-service/event-grpc/server"
	"github.com/rx3lixir/event-service/internal/db"
	"google.golang.org/grpc"
)

type eventServer struct {
	pb.UnimplementedEventServiceServer
	db *pgxpool.Pool
}

func main() {
	var (
		db_url_env    = envflag.String("DB_URL", "postgres://", "!")
		grpc_svc_addr = envflag.String("GRPC_PORT", "0.0.0.0:9091", "!")
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := db.CreatePostgresPool(ctx, *db_url_env)
	if err != nil {
		slog.Error("Failed to create postgres pool", "error", err)
		os.Exit(1)
	}

	// instantiate server
	storer := db.NewPosgresStore(pool)
	srv := server.NewServer(storer)

	// register server with the gRPC server
	grpcSrv := grpc.NewServer()
	pb.RegisterEventServiceServer(grpcSrv, srv)

	listener, err := net.Listen("tcp", *grpc_svc_addr)
	if err != nil {
		slog.Error("Failed to start listener", "error", err)
		os.Exit(1)
	}

	slog.Info("Server is listening on", "address", *grpc_svc_addr)

	grpcSrv.Serve(listener)
	if err != nil {
		slog.Error("failed to serve gRPC", "error", err)
		os.Exit(1)
	}
}
