package opensearch

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/opensearch-project/opensearch-go"
	"github.com/rx3lixir/event-service/internal/config"
	"github.com/rx3lixir/event-service/pkg/logger"
)

// Client представляет клиент для работы с OpenSearch
type Client struct {
	os    *opensearch.Client
	index string
	log   logger.Logger
}

// NewClient создает новый клиент OpenSearch
func NewClient(cfg config.OpenSearchParams, log logger.Logger) (*Client, error) {
	// Конфигурация OpenSearch клиента
	osConfig := opensearch.Config{
		Addresses: []string{cfg.URL},
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 10,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true, // Для разработки, в продакшене лучше настроить SSL
			},
		},
		RetryOnStatus: []int{502, 503, 504, 429},
		MaxRetries:    cfg.MaxRetries,
	}

	// Создаем клиент
	os, err := opensearch.NewClient(osConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create opensearch client: %w", err)
	}

	client := &Client{
		os:    os,
		index: cfg.Index,
		log:   log,
	}

	return client, nil
}

// Ping проверяет подключение к OpenSearch
func (c *Client) Ping(ctx context.Context) error {
	res, err := c.os.Ping(
		c.os.Ping.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("failed to ping opensearch: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("opensearch ping failed with status: %s", res.Status())
	}

	c.log.Debug("OpenSearch ping success", "status", res.Status())
	return nil
}

// Исправленная версия CreateIndex в internal/opensearch/client.go
func (c *Client) CreateIndex(ctx context.Context) error {
	// Проверяем, существует ли индекс
	res, err := c.os.Indices.Exists(
		[]string{c.index},
		c.os.Indices.Exists.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("failed to check if index exists: %w", err)
	}
	defer res.Body.Close()

	// Если индекс уже существует, ничего не делаем
	if res.StatusCode == 200 {
		c.log.Info("OpenSearch index already exists", "index", c.index)
		return nil
	}

	// Получаем рабочую директорию
	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("не удалось получить рабочую директорию: %s", err)
	}

	// Получаем маппинг из mapping.json
	mappingBytes, err := os.ReadFile(filepath.Join(cwd, "mapping.json"))
	mapping := string(mappingBytes)

	res, err = c.os.Indices.Create(
		c.index,
		c.os.Indices.Create.WithContext(ctx),
		c.os.Indices.Create.WithBody(strings.NewReader(mapping)),
	)
	if err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		// Читаем тело ответа для получения детальной информации об ошибке
		body := make([]byte, 1024)
		if n, readErr := res.Body.Read(body); readErr == nil && n > 0 {
			c.log.Error("OpenSearch index creation failed", "status", res.Status(), "response", string(body[:n]))
			return fmt.Errorf("failed to create index, status: %s, response: %s", res.Status(), string(body[:n]))
		}
		return fmt.Errorf("failed to create index, status: %s", res.Status())
	}

	c.log.Info("OpenSearch index created successfully", "index", c.index)
	return nil
}

// GetClient возвращает оригинальный клиент OpenSearch для низкоуровневых операций
func (c *Client) GetClient() *opensearch.Client {
	return c.os
}

// GetIndex возвращает имя индекса
func (c *Client) GetIndex() string {
	return c.index
}
