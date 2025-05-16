package config

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/spf13/viper"
)

// Константы для ключей конфигурации
const (
	envKey = "service_params.env"

	usernameKey       = "db_params.username"
	passwordKey       = "db_params.password"
	dbNameKey         = "db_params.db_name"
	hostKey           = "db_params.host"
	portKey           = "db_params.port"
	connectTimeoutKey = "db_params.connect_timeout"

	serviceAddress = "server_params.address"
)

// AppConfig представляет конфигурацию всего приложения
type AppConfig struct {
	Service ServiceParams `mapstructure:"service_params" validate:"required"`
	DB      DBParams      `mapstructure:"db_params" validate:"required"`
	Server  ServerParams  `mapstructure:"server_params" validate:"required"`
}

// ApplicationParams содержит общие параметры приложения
type ServiceParams struct {
	Env string `mapstructure:"env" validate:"required,oneof=dev prod test"`
}

type ServerParams struct {
	Address string `mapstructure:"address" validate:"required"`
}

// DBParams содержит параметры подключения к базе данных
type DBParams struct {
	Username       string        `mapstructure:"username" validate:"required"`
	Password       string        `mapstructure:"password" validate:"required"`
	DBName         string        `mapstructure:"db_name" validate:"required"`
	Host           string        `mapstructure:"host" validate:"required"`
	Port           int           `mapstructure:"port" validate:"required,min=1,max=65535"`
	ConnectTimeout time.Duration `mapstructure:"connect_timeout" validate:"required,min=1"`
}

// DSN собирает строку подключения к базе данных
func (db *DBParams) DSN() string {
	// Если хост не указан, используем localhost по умолчанию
	host := db.Host
	if host == "" {
		host = "localhost"
	}

	// Преобразование timeout в секунды
	timeoutSec := int(db.ConnectTimeout.Seconds())
	if timeoutSec < 1 {
		timeoutSec = 10 // Значение по умолчанию, если timeout некорректный
	}

	return fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?connect_timeout=%d&sslmode=disable",
		db.Username,
		db.Password,
		host,
		db.Port,
		db.DBName,
		timeoutSec,
	)
}

// EnvBindings возвращает мапу ключей конфигурации и соответствующих им переменных окружения
func envBindings() map[string]string {
	return map[string]string{
		envKey:            "SERVICE_KEY",
		usernameKey:       "DB_USERNAME",
		passwordKey:       "DB_PASSWORD",
		dbNameKey:         "DB_NAME",
		hostKey:           "DB_HOST",
		portKey:           "DB_PORT",
		connectTimeoutKey: "DB_CONNECT_TIMEOUT",
		serviceAddress:    "SERVICE_ADDRESS",
	}
}

// New загружает конфигурацию из файла и переменных окружения
func New() (*AppConfig, error) {
	v := viper.New()

	// Получаем рабочую директорию
	cwd, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("не удалось получить рабочую директорию: %w", err)
	}

	v.AddConfigPath(filepath.Join(cwd, "internal", "config"))
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AutomaticEnv()

	// Привязка переменных окружения
	for configKey, envVar := range envBindings() {
		if err := v.BindEnv(configKey, envVar); err != nil {
			return nil, fmt.Errorf("ошибка привязки переменной окружения %s: %w", envVar, err)
		}
	}

	// Чтение конфигурации
	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("ошибка чтения конфигурационного файла: %w", err)
	}

	var config AppConfig

	if err := v.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("ошибка при декодировании конфигурации: %w", err)
	}

	// Установка значений по умолчанию
	if config.DB.Host == "" {
		config.DB.Host = "0.0.0.0"
	}

	// Валидация конфигурации
	validate := validator.New()

	if err := validate.Struct(config); err != nil {
		return nil, fmt.Errorf("ошибка валидации конфигурации: %w", err)
	}

	return &config, nil
}
