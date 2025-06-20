package client

import (
	"fmt"
	"time"
)

type Config struct {
	URL                string        `mapstructure:"url" validate:"required"`
	IndexName          string        `mapstructure:"index_name" validate:"required"`
	Timeout            time.Duration `mapstructure:"timeout" validate:"required,min=1s"`
	MaxRetries         int           `mapstructure:"max_retries" validate:"min=0,max=5"`
	MaxIdleConns       int           `mapstructure:"max_idle_conns"`
	InsecureSkipVerify bool          `mapstructure:"insecure_skip_verify"`
	RetryOnStatus      []int         `mapstructure:"retry_on_status"`
}

func DefaultConfig() *Config {
	return &Config{
		IndexName:          "events",
		Timeout:            5 * time.Second,
		MaxRetries:         3,
		MaxIdleConns:       10,
		InsecureSkipVerify: true, // Только в дев режиме
		RetryOnStatus:      []int{502, 503, 504, 429},
	}
}

func (c *Config) Validate() error {
	if c.URL == "" {
		return fmt.Errorf("URL is required")
	}
	if c.IndexName == "" {
		return fmt.Errorf("index name is required")
	}
	if c.Timeout < time.Second {
		return fmt.Errorf("timeout must be at least 1 second")
	}
	return nil
}
