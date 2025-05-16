package logger

import (
	"fmt"
	"os"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	// Log - глобальный логгер
	Log *zap.Logger
	// SugaredLog - обертка для удобного форматирования
	SugaredLog *zap.SugaredLogger
	once       sync.Once
)

// Config содержит настройки логгера
type Config struct {
	Level      string `mapstructure:"level" validate:"required,oneof=debug info warn error fatal"`
	Encoding   string `mapstructure:"encoding" validate:"required,oneof=json console"`
	OutputPath string `mapstructure:"output_path"`
}

// Initialize инициализирует логгер с заданными настройками
func Initialize(cfg *Config) error {
	var err error
	once.Do(func() {
		// Преобразовываем строковый уровень логирования в zapcore.Level
		var level zapcore.Level
		err = level.UnmarshalText([]byte(cfg.Level))
		if err != nil {
			err = fmt.Errorf("невозможно разобрать уровень логирования: %w", err)
			return
		}

		// Базовая конфигурация
		encoderConfig := zap.NewProductionEncoderConfig()
		encoderConfig.TimeKey = "timestamp"
		encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

		// Определяем путь для вывода логов
		var outputPaths []string
		if cfg.OutputPath != "" {
			outputPaths = append(outputPaths, cfg.OutputPath)
		}
		outputPaths = append(outputPaths, "stdout")

		// Создаем конфигурацию zap
		zapConfig := zap.Config{
			Level:             zap.NewAtomicLevelAt(level),
			Development:       false,
			Encoding:          cfg.Encoding,
			EncoderConfig:     encoderConfig,
			OutputPaths:       outputPaths,
			ErrorOutputPaths:  []string{"stderr"},
			DisableCaller:     false,
			DisableStacktrace: false,
		}

		// Создаем логгер
		Log, err = zapConfig.Build()
		if err != nil {
			err = fmt.Errorf("ошибка создания логгера: %w", err)
			return
		}

		SugaredLog = Log.Sugar()
	})

	return err
}

// InitDefault инициализирует логгер со стандартными настройками для разработки
func InitDefault() {
	if err := Initialize(&Config{
		Level:    "debug",
		Encoding: "console",
	}); err != nil {
		fmt.Printf("Ошибка инициализации логгера: %v\n", err)
		os.Exit(1)
	}
}

// Debug логирует на уровне DEBUG
func Debug(msg string, fields ...zapcore.Field) {
	if Log != nil {
		Log.Debug(msg, fields...)
	}
}

// Info логирует на уровне INFO
func Info(msg string, fields ...zapcore.Field) {
	if Log != nil {
		Log.Info(msg, fields...)
	}
}

// Warn логирует на уровне WARN
func Warn(msg string, fields ...zapcore.Field) {
	if Log != nil {
		Log.Warn(msg, fields...)
	}
}

// Error логирует на уровне ERROR
func Error(msg string, fields ...zapcore.Field) {
	if Log != nil {
		Log.Error(msg, fields...)
	}
}

// Fatal логирует на уровне FATAL и завершает программу
func Fatal(msg string, fields ...zapcore.Field) {
	if Log != nil {
		Log.Fatal(msg, fields...)
	}
}

// Sync сбрасывает записи из буфера логгера
func Sync() {
	if Log != nil {
		_ = Log.Sync()
	}
}

// WithFields создает новый логгер с дополнительными полями
func WithFields(fields ...zapcore.Field) *zap.Logger {
	if Log != nil {
		return Log.With(fields...)
	}
	return zap.NewNop()
}

// WithField создает новый логгер с дополнительным полем
func WithField(key string, value interface{}) *zap.Logger {
	if Log != nil {
		return Log.With(zap.Any(key, value))
	}
	return zap.NewNop()
}
