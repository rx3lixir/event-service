package client

import (
	"crypto/tls"
	"fmt"
	"net/http"

	"github.com/opensearch-project/opensearch-go"
	"github.com/rx3lixir/event-service/pkg/logger"
)

type Client struct {
	client *opensearch.Client
	config *Config
	logger logger.Logger
}

func New(cfg *Config, log logger.Logger) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	osConfig := opensearch.Config{
		Addresses: []string{cfg.URL},
		Transport: &http.Transport{
			MaxIdleConnsPerHost: cfg.MaxIdleConns,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: cfg.InsecureSkipVerify,
			},
		},
		RetryOnStatus: cfg.RetryOnStatus,
		MaxRetries:    cfg.MaxRetries,
	}

	osClient, err := opensearch.NewClient(osConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create opensearch client: %w", err)
	}

	return &Client{
		client: osClient,
		config: cfg,
		logger: log,
	}, nil
}

func (c *Client) GetNativeClient() *opensearch.Client {
	return c.client
}

func (c *Client) GetIndexName() string {
	return c.config.IndexName
}
