package elasticsearch

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/rx3lixir/event-service/internal/config"
	"github.com/rx3lixir/event-service/pkg/logger"
)

// Client представляет клиент для работы с Elasticsearch
type Client struct {
	es    *elasticsearch.Client
	index string
	log   logger.Logger
}

// NewClient создает новый клиент Elasticsearch
func NewClient(cfg config.ElasticsearchParams, log logger.Logger) (*Client, error) {
	// Конфигурация Elasticsearch клиента
	esConfig := elasticsearch.Config{
		Addresses: []string{cfg.URL},
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   10,
			ResponseHeaderTimeout: cfg.Timeout,
		},
		MaxRetries: cfg.MaxRetries,
	}

	// Создаем клиент
	es, err := elasticsearch.NewClient(esConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create elasticsearch client: %w", err)
	}

	client := &Client{
		es:    es,
		index: cfg.Index,
		log:   log,
	}

	return client, nil
}

// Ping проверяет подключение к Elasticsearch
func (c *Client) Ping(ctx context.Context) error {
	res, err := c.es.Ping(
		c.es.Ping.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("failed to ping elasticsearch: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("elasticsearch ping failed with status: %s", res.Status())
	}

	c.log.Debug("Elasticsearch ping success", "status", res.Status())
	return nil
}

// CreateIndex создает индекс для событий, если его не существует
func (c *Client) CreateIndex(ctx context.Context) error {
	// Проверяем, существует ли индекс
	res, err := c.es.Indices.Exists([]string{c.index})
	if err != nil {
		return fmt.Errorf("failed to check if index exists: %w", err)
	}
	defer res.Body.Close()

	// Если индекс уже существует, ничего не делаем
	if res.StatusCode == 200 {
		c.log.Info("Elasticsearch index already exists", "index", c.index)
		return nil
	}

	// Создаем индекс с маппингом
	mapping := `{
		"settings": {
			"number_of_shards": 1,
			"number_of_replicas": 0,
			"analysis": {
				"analyzer": {
					"russian_analyzer": {
						"type": "custom",
						"tokenizer": "standard",
						"filter": [
							"lowercase",
							"russian_morphology",
							"english_morphology"
						]
					}
				}
			}
		},
		"mappings": {
			"properties": {
				"id": {
					"type": "long"
				},
				"name": {
					"type": "text",
					"analyzer": "russian_analyzer",
					"fields": {
						"keyword": {
							"type": "keyword"
						}
					}
				},
				"description": {
					"type": "text",
					"analyzer": "russian_analyzer"
				},
				"category_id": {
					"type": "long"
				},
				"category_name": {
					"type": "keyword"
				},
				"date": {
					"type": "date",
					"format": "yyyy-MM-dd||strict_date_optional_time||epoch_millis"
				},
				"time": {
					"type": "keyword"
				},
				"location": {
					"type": "text",
					"analyzer": "russian_analyzer",
					"fields": {
						"keyword": {
							"type": "keyword"
						}
					}
				},
				"price": {
					"type": "float"
				},
				"image": {
					"type": "keyword",
					"index": false
				},
				"source": {
					"type": "keyword"
				},
				"created_at": {
					"type": "date"
				},
				"updated_at": {
					"type": "date"
				}
			}
		}
	}`

	res, err = c.es.Indices.Create(
		c.index,
		c.es.Indices.Create.WithContext(ctx),
		c.es.Indices.Create.WithBody(strings.NewReader(mapping)),
	)
	if err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("failed to create index, status: %s", res.Status())
	}

	c.log.Info("Elasticsearch index created successfully", "index", c.index)
	return nil
}

// GetClient возвращает оригинальный клиент Elasticsearch для низкоуровневых операций
func (c *Client) GetClient() *elasticsearch.Client {
	return c.es
}

// GetIndex возвращает имя индекса
func (c *Client) GetIndex() string {
	return c.index
}

