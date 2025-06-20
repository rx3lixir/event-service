package client

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type HealthChecker struct {
	client *Client
}

func NewHealthChecker(client *Client) *HealthChecker {
	return &HealthChecker{
		client: client,
	}
}

func (h *HealthChecker) Check(ctx context.Context) error {
	res, err := h.client.client.Ping(
		h.client.client.Ping.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("failed to ping opensearch: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("opensearch ping failed with status: %s", res.Status())
	}

	return nil
}

func (h *HealthChecker) WaitForHealthy(ctx context.Context, maxRetries int, retryInterval time.Duration) error {
	for i := 0; i < maxRetries; i++ {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled while waiting for OpenSearch: %w", ctx.Err())
		default:
		}

		if err := h.Check(ctx); err == nil {
			return nil
		}

		if i < maxRetries-1 {
			time.Sleep(retryInterval)
		}
	}

	return fmt.Errorf("opensearch not healthy after %d retries", maxRetries)
}

func (h *HealthChecker) GetClusterHealth(ctx context.Context) (*ClusterHealth, error) {
	res, err := h.client.client.Cluster.Health(
		h.client.client.Cluster.Health.WithContext(ctx),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster health: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("cluster health request failed: %s", res.Status())
	}

	var health ClusterHealth
	if err := json.NewDecoder(res.Body).Decode(&health); err != nil {
		return nil, fmt.Errorf("failed to decode cluster health response: %w", err)
	}

	return &health, nil
}

type ClusterHealth struct {
	ClusterName         string `json:"cluster_name"`
	Status              string `json:"status"`
	NumberOfNodes       int    `json:"number_of_nodes"`
	NumberOfDataNodes   int    `json:"number_of_data_nodes"`
	ActivePrimaryShards int    `json:"active_primary_shards"`
	ActiveShards        int    `json:"active_shards"`
	RelocatingShards    int    `json:"relocating_shards"`
	InitializingShards  int    `json:"initializing_shards"`
	UnassignedShards    int    `json:"unassigned_shards"`
	TimedOut            bool   `json:"timed_out"`
}

func (ch *ClusterHealth) IsHealthy() bool {
	return ch.Status == "green" || ch.Status == "yellow"
}
