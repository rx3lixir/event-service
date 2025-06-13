package health

import (
	"time"
)

// Config конфигурация для health сервера
type Config struct {
	Port             string
	ServiceName      string
	Version          string
	Timeout          time.Duration
	RequiredTables   []string
	MigrationVersion int // 0 = не проверять версию

	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
}

// Option функция для настройки health server
type Option func(*Config)

// defaultConfig возвращает конфигурацию по умолчанию
func defaultConfig() Config {
	return Config{
		Port:             ":8081",
		ServiceName:      "unknown-service",
		Version:          "unknown",
		Timeout:          5 * time.Second,
		RequiredTables:   []string{},
		MigrationVersion: 0, // 0 = не проверять версию
		ReadTimeout:      10 * time.Second,
		WriteTimeout:     10 * time.Second,
		IdleTimeout:      60 * time.Second,
	}
}

// WithPort устанавливает порт для health server
func WithPort(port string) Option {
	return func(c *Config) {
		c.Port = port
	}
}

// WithServiceName устанавливает только имя сервиса
func WithServiceName(name string) Option {
	return func(c *Config) {
		c.ServiceName = name
	}
}

// WithVersion устанавливает только версию сервиса
func WithVersion(version string) Option {
	return func(c *Config) {
		c.Version = version
	}
}

// WithTimeout устанавливает таймаут для health checks
func WithTimeout(timeout time.Duration) Option {
	return func(c *Config) {
		c.Timeout = timeout
	}
}

// WithRequiredTables устанавливает список обязательных таблиц для проверки
func WithRequiredTables(tables ...string) Option {
	return func(c *Config) {
		c.RequiredTables = make([]string, len(tables))
		copy(c.RequiredTables, tables)
	}
}

// WithMigrationVersion устанавливает ожидаемую минимальную версию миграций
// Если version = 0, проверка версии отключена
func WithMigrationVersion(version int) Option {
	return func(c *Config) {
		c.MigrationVersion = version
	}
}

// WithHTTPTimeouts устанавливает все HTTP timeouts одновременно
func WithHTTPTimeouts(read, write, idle time.Duration) Option {
	return func(c *Config) {
		c.ReadTimeout = read
		c.WriteTimeout = write
		c.IdleTimeout = idle
	}
}

// - Предустановленные конфигурации - \\

// EventServiceOptions возвращает специфичные для event-service опции
func EventServiceOptions(version string) []Option {
	return []Option{
		WithServiceName("event-service"),
		WithVersion("1.0.0"),
		WithPort(":8081"),
		WithTimeout(5 * time.Second),
		WithRequiredTables("events", "categories"),
		// Можно добавить конкретную версию миграций если нужно:
		// WithMigrationVersion(20250515143526),
	}
}
