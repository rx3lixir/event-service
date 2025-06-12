# Стандартный Dockerfile для всех Go микросервисов
# Build stage
FROM golang:1.24-alpine AS builder

# Устанавливаем необходимые пакеты для сборки
RUN apk add --no-cache git ca-certificates tzdata

# Создаём непривилегированного пользователя
RUN adduser -D -g '' appuser

# Рабочая директория
WORKDIR /build

# Копируем go mod файлы и скачиваем зависимости
COPY go.mod go.sum ./
RUN go mod download && go mod verify

# Копируем исходный код
COPY . .

# Аргументы для имени сервиса и пути (передаём при сборке)
ARG SERVICE_NAME
ARG SERVICE_PATH

# Собираем бинарник
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-w -s" \
    -o ${SERVICE_NAME} \
    ${SERVICE_PATH}

# Final stage - используем alpine вместо scratch
FROM alpine:latest

# Устанавливаем ca-certificates (если нужны HTTPS запросы)
RUN apk --no-cache add ca-certificates

# Аргумент для имени сервиса
ARG SERVICE_NAME

# Создаём непривилегированного пользователя
RUN addgroup -g 1000 -S appuser && \
    adduser -u 1000 -S appuser -G appuser

# Создаём директорию для приложения
RUN mkdir -p /app && chown -R appuser:appuser /app

# Копируем конфиг
COPY --from=builder --chown=appuser:appuser /build/internal/config/config.yaml /app/internal/config/config.yaml

# Копируем бинарник и переименовываем его в app для простоты
COPY --from=builder --chown=appuser:appuser /build/${SERVICE_NAME} /app/app

# Используем непривилегированного пользователя
USER appuser

# Рабочая директория
WORKDIR /app

# Запускаем приложение
ENTRYPOINT ["./app"]
