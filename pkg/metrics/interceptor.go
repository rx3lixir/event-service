package metrics

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// UnaryServerInterceptor создает interceptor для unary gRPC методов
func UnaryServerInterceptor(serviceName string) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		start := time.Now()

		// Получаем имя метода
		method := GetMethodName(info.FullMethod)

		// Выполняем запрос
		resp, err := handler(ctx, req)

		// Записываем метрики
		duration := time.Since(start)
		grpcCode := codes.OK
		if err != nil {
			grpcCode = status.Code(err)
		}

		statusStr := StatusFromGrpcCode(int(grpcCode))
		RecordGrpcRequest(serviceName, method, statusStr, duration)

		return resp, err
	}
}

// StreamServerInterceptor создает interceptor для streaming gRPC методов
func StreamServerInterceptor(serviceName string) grpc.StreamServerInterceptor {
	return func(
		srv any,
		stream grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		start := time.Now()

		// Получаем имя метода
		method := GetMethodName(info.FullMethod)

		// Выполняем stream
		err := handler(srv, stream)

		// Записываем метрики
		duration := time.Since(start)
		grpcCode := codes.OK
		if err != nil {
			grpcCode = status.Code(err)
		}

		statusStr := StatusFromGrpcCode(int(grpcCode))
		RecordGrpcRequest(serviceName, method, statusStr, duration)

		return err
	}
}

// DatabaseInterceptor обертка для database операций
func DatabaseInterceptor(operation, table string, fn func() error) error {
	start := time.Now()
	err := fn()
	duration := time.Since(start)
	status := StatusFromError(err)

	RecordDatabaseOperation(operation, table, status, duration)
	return err
}

// OpenSearchInterceptor обертка для OpenSearch операций
func OpenSearchInterceptor(operation, index string, fn func() error) error {
	start := time.Now()
	err := fn()
	duration := time.Since(start)
	status := StatusFromError(err)

	RecordOpenSearchOperation(operation, index, status, duration)
	return err
}
