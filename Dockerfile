# Стандартный Dockerfile для всех Go микросервисов
# Build stage
FROM golang:1.24-alpine AS builder

# Устанавливаем необходимые пакеты для сборки
RUN apk add --no-cache git ca-certificates tzdata

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

# Final stage - используем конкретную версию alpine и обновляем пакеты
FROM alpine:3.19

# Обновляем пакетный менеджер и устанавливаем необходимые пакеты
RUN apk update && apk upgrade && apk add --no-cache \
    ca-certificates \
    wget \
    curl \
    tzdata \
    && rm -rf /var/cache/apk/*

# Аргумент для имени сервиса
ARG SERVICE_NAME

# Создаём непривилегированного пользователя
RUN addgroup -g 1000 -S appuser && \
    adduser -u 1000 -S appuser -G appuser

# Создаём директорию для приложения со всей нужной структурой
RUN mkdir -p /app/internal/config && chown -R appuser:appuser /app

# Копируем конфиг в правильное место
COPY --from=builder --chown=appuser:appuser /build/internal/config/config.yaml /app/internal/config/config.yaml

# Копируем бинарник и переименовываем его в app для простоты
COPY --from=builder --chown=appuser:appuser /build/${SERVICE_NAME} /app/app

# Делаем бинарник исполняемым
RUN chmod +x /app/app

# Используем непривилегированного пользователя
USER appuser

# Рабочая директория
WORKDIR /app

# Экспонируем порты (документационно)
EXPOSE 9091
EXPOSE 8081

# Health check на уровне Docker с ПРАВИЛЬНЫМ портом для event-service
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8081/health || exit 1

# Запускаем приложение
ENTRYPOINT ["./app"]
