package logger

import (
	"context"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	// Log - глобальный экземпляр логгера (SugaredLogger)
	Log *zap.SugaredLogger
	// RawLog - глобальный экземпляр обычного логгера (zap.Logger)
	RawLog *zap.Logger
	// используем ли мы быстрый немаршалированный логгер
	useRawLogger bool
)

// Logger представляет интерфейс для логирования
type Logger interface {
	Debug(msg string, args ...interface{})
	DebugContext(ctx context.Context, msg string, args ...interface{})
	Info(msg string, args ...interface{})
	InfoContext(ctx context.Context, msg string, args ...interface{})
	Warn(msg string, args ...interface{})
	WarnContext(ctx context.Context, msg string, args ...interface{})
	Error(msg string, args ...interface{})
	ErrorContext(ctx context.Context, msg string, args ...interface{})
	Fatal(msg string, args ...interface{})
	FatalContext(ctx context.Context, msg string, args ...interface{})
	Panic(msg string, args ...interface{})
	PanicContext(ctx context.Context, msg string, args ...interface{})
}

// DefaultLogger реализует интерфейс Logger, оборачивая глобальные функции
type DefaultLogger struct{}

// Debug логирует с уровнем Debug
func (l *DefaultLogger) Debug(msg string, args ...interface{}) {
	Debug(msg, args...)
}

// Info логирует с уровнем Info
func (l *DefaultLogger) Info(msg string, args ...interface{}) {
	Info(msg, args...)
}

// Warn логирует с уровнем Warn
func (l *DefaultLogger) Warn(msg string, args ...interface{}) {
	Warn(msg, args...)
}

// Error логирует с уровнем Error
func (l *DefaultLogger) Error(msg string, args ...interface{}) {
	Error(msg, args...)
}

// Fatal логирует с уровнем Fatal
func (l *DefaultLogger) Fatal(msg string, args ...interface{}) {
	Fatal(msg, args...)
}

// Panic логирует с уровнем Panic
func (l *DefaultLogger) Panic(msg string, args ...interface{}) {
	Panic(msg, args...)
}

func (l *DefaultLogger) DebugContext(ctx context.Context, msg string, args ...interface{}) {
	DebugContext(ctx, msg, args...)
}

func (l *DefaultLogger) InfoContext(ctx context.Context, msg string, args ...interface{}) {
	InfoContext(ctx, msg, args...)
}

func (l *DefaultLogger) WarnContext(ctx context.Context, msg string, args ...interface{}) {
	WarnContext(ctx, msg, args...)
}

func (l *DefaultLogger) ErrorContext(ctx context.Context, msg string, args ...interface{}) {
	ErrorContext(ctx, msg, args...)
}

func (l *DefaultLogger) FatalContext(ctx context.Context, msg string, args ...interface{}) {
	FatalContext(ctx, msg, args...)
}

func (l *DefaultLogger) PanicContext(ctx context.Context, msg string, args ...interface{}) {
	PanicContext(ctx, msg, args...)
}

// DebugContext логирует с уровнем Debug с контекстом
func DebugContext(ctx context.Context, msg string, args ...interface{}) {
	logWithContext(ctx, zapcore.DebugLevel, msg, args...)
}

// InfoContext логирует с уровнем Info с контекстом
func InfoContext(ctx context.Context, msg string, args ...interface{}) {
	logWithContext(ctx, zapcore.InfoLevel, msg, args...)
}

// WarnContext логирует с уровнем Warn с контекстом
func WarnContext(ctx context.Context, msg string, args ...interface{}) {
	logWithContext(ctx, zapcore.WarnLevel, msg, args...)
}

// ErrorContext логирует с уровнем Error с контекстом
func ErrorContext(ctx context.Context, msg string, args ...interface{}) {
	logWithContext(ctx, zapcore.ErrorLevel, msg, args...)
}

// FatalContext логирует с уровнем Fatal с контекстом
func FatalContext(ctx context.Context, msg string, args ...interface{}) {
	logWithContext(ctx, zapcore.FatalLevel, msg, args...)
}

// PanicContext логирует с уровнем Panic с контекстом
func PanicContext(ctx context.Context, msg string, args ...interface{}) {
	logWithContext(ctx, zapcore.PanicLevel, msg, args...)
}

// logWithContext - внутренняя функция для логирования с контекстом
func logWithContext(ctx context.Context, level zapcore.Level, msg string, args ...interface{}) {
	// Извлекаем поля из контекста
	contextFields := extractContextFields(ctx)

	// Объединяем поля из контекста с переданными аргументами
	allArgs := append(contextFields, args...)

	// Логируем в зависимости от уровня
	if useRawLogger {
		fields := argsToFields(allArgs)
		switch level {
		case zapcore.DebugLevel:
			RawLog.Debug(msg, fields...)
		case zapcore.InfoLevel:
			RawLog.Info(msg, fields...)
		case zapcore.WarnLevel:
			RawLog.Warn(msg, fields...)
		case zapcore.ErrorLevel:
			RawLog.Error(msg, fields...)
		case zapcore.FatalLevel:
			RawLog.Fatal(msg, fields...)
		case zapcore.PanicLevel:
			RawLog.Panic(msg, fields...)
		}
	} else {
		switch level {
		case zapcore.DebugLevel:
			Log.Debugw(msg, allArgs...)
		case zapcore.InfoLevel:
			Log.Infow(msg, allArgs...)
		case zapcore.WarnLevel:
			Log.Warnw(msg, allArgs...)
		case zapcore.ErrorLevel:
			Log.Errorw(msg, allArgs...)
		case zapcore.FatalLevel:
			Log.Fatalw(msg, allArgs...)
		case zapcore.PanicLevel:
			Log.Panicw(msg, allArgs...)
		}
	}
}

// extractContextFields извлекает поля для логирования из контекста
func extractContextFields(ctx context.Context) []interface{} {
	if ctx == nil {
		return nil
	}

	fields := make([]interface{}, 0)

	if requestID, ok := ctx.Value("request_id").(string); ok {
		fields = append(fields, "request_id", requestID)
	}

	if userID, ok := ctx.Value("user_id").(string); ok {
		fields = append(fields, "user_id", userID)
	}

	return fields
}

// NewLogger создает новый экземпляр логгера, который можно передавать в другие компоненты
func NewLogger() Logger {
	return &DefaultLogger{}
}

// Init инициализирует логгер на основе переданного окружения
func Init(env string) {
	isProd := env == "prod"

	if isProd {
		useRawLogger = true
		initProductionLogger()
	} else {
		useRawLogger = false
		initDevelopmentLogger()
	}
}

// initProductionLogger инициализирует оптимизированный логгер для продакшена
func initProductionLogger() {
	encoderConfig := zapcore.EncoderConfig{
		MessageKey:     "message",
		LevelKey:       "level",
		TimeKey:        "time",
		NameKey:        "logger",
		FunctionKey:    zapcore.OmitKey,
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
	}

	encoder := zapcore.NewJSONEncoder(encoderConfig)
	stdout := zapcore.AddSync(os.Stdout)
	stderr := zapcore.AddSync(os.Stderr)

	core := zapcore.NewCore(
		encoder,
		stdout,
		zap.NewAtomicLevelAt(zapcore.InfoLevel),
	)

	options := []zap.Option{
		zap.AddCaller(),
		zap.AddCallerSkip(1),
		zap.ErrorOutput(stderr),
	}

	RawLog = zap.New(core, options...)
	Log = RawLog.Sugar()
}

// initDevelopmentLogger инициализирует расширенный логгер для разработки
func initDevelopmentLogger() {
	encoderConfig := zapcore.EncoderConfig{
		MessageKey:     "message",
		LevelKey:       "level",
		TimeKey:        "time",
		NameKey:        "logger",
		FunctionKey:    zapcore.OmitKey,
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
	}

	encoder := zapcore.NewJSONEncoder(encoderConfig)
	stdout := zapcore.AddSync(os.Stdout)
	stderr := zapcore.AddSync(os.Stderr)

	core := zapcore.NewCore(
		encoder,
		stdout,
		zap.NewAtomicLevelAt(zapcore.DebugLevel),
	)

	options := []zap.Option{
		zap.AddCaller(),
		zap.AddCallerSkip(1),
		zap.AddStacktrace(zapcore.WarnLevel),
		zap.Development(),
		zap.ErrorOutput(stderr),
	}

	RawLog = zap.New(core, options...)
	Log = RawLog.Sugar()
}

// Close закрывает логгер и освобождает ресурсы
func Close() error {
	if RawLog != nil {
		return RawLog.Sync()
	}
	return nil
}

// Debug логирует с уровнем Debug
func Debug(msg string, args ...interface{}) {
	if useRawLogger {
		if len(args) > 0 && len(args)%2 == 0 {
			fields := argsToFields(args)
			RawLog.Debug(msg, fields...)
		} else {
			RawLog.Debug(msg)
		}
	} else {
		Log.Debugw(msg, args...)
	}
}

// Info логирует с уровнем Info
func Info(msg string, args ...interface{}) {
	if useRawLogger {
		if len(args) > 0 && len(args)%2 == 0 {
			fields := argsToFields(args)
			RawLog.Info(msg, fields...)
		} else {
			RawLog.Info(msg)
		}
	} else {
		Log.Infow(msg, args...)
	}
}

// Warn логирует с уровнем Warn
func Warn(msg string, args ...interface{}) {
	if useRawLogger {
		if len(args) > 0 && len(args)%2 == 0 {
			fields := argsToFields(args)
			RawLog.Warn(msg, fields...)
		} else {
			RawLog.Warn(msg)
		}
	} else {
		Log.Warnw(msg, args...)
	}
}

// Error логирует с уровнем Error
func Error(msg string, args ...interface{}) {
	if useRawLogger {
		if len(args) > 0 && len(args)%2 == 0 {
			fields := argsToFields(args)
			RawLog.Error(msg, fields...)
		} else {
			RawLog.Error(msg)
		}
	} else {
		Log.Errorw(msg, args...)
	}
}

// Fatal логирует с уровнем Fatal и завершает программу с кодом 1
func Fatal(msg string, args ...interface{}) {
	if useRawLogger {
		if len(args) > 0 && len(args)%2 == 0 {
			fields := argsToFields(args)
			RawLog.Fatal(msg, fields...)
		} else {
			RawLog.Fatal(msg)
		}
	} else {
		Log.Fatalw(msg, args...)
	}
}

// Panic логирует с уровнем Panic и вызывает panic()
func Panic(msg string, args ...interface{}) {
	if useRawLogger {
		if len(args) > 0 && len(args)%2 == 0 {
			fields := argsToFields(args)
			RawLog.Panic(msg, fields...)
		} else {
			RawLog.Panic(msg)
		}
	} else {
		Log.Panicw(msg, args...)
	}
}

// WithContext возвращает логгер с добавленными полями из контекста
func WithContext(ctx context.Context) Logger {
	return &contextLogger{ctx: ctx, logger: NewLogger()}
}

type contextLogger struct {
	ctx    context.Context
	logger Logger
}

func (l *contextLogger) Debug(msg string, args ...interface{}) {
	l.logger.DebugContext(l.ctx, msg, args...)
}

func (l *contextLogger) DebugContext(ctx context.Context, msg string, args ...interface{}) {
	l.logger.DebugContext(ctx, msg, args...)
}

func (l *contextLogger) Info(msg string, args ...interface{}) {
	l.logger.InfoContext(l.ctx, msg, args...)
}

func (l *contextLogger) InfoContext(ctx context.Context, msg string, args ...interface{}) {
	l.logger.InfoContext(ctx, msg, args...)
}

func (l *contextLogger) Warn(msg string, args ...interface{}) {
	l.logger.WarnContext(l.ctx, msg, args...)
}

func (l *contextLogger) WarnContext(ctx context.Context, msg string, args ...interface{}) {
	l.logger.WarnContext(ctx, msg, args...)
}

func (l *contextLogger) Error(msg string, args ...interface{}) {
	l.logger.ErrorContext(l.ctx, msg, args...)
}

func (l *contextLogger) ErrorContext(ctx context.Context, msg string, args ...interface{}) {
	l.logger.ErrorContext(ctx, msg, args...)
}

func (l *contextLogger) Fatal(msg string, args ...interface{}) {
	l.logger.FatalContext(l.ctx, msg, args...)
}

func (l *contextLogger) FatalContext(ctx context.Context, msg string, args ...interface{}) {
	l.logger.FatalContext(ctx, msg, args...)
}

func (l *contextLogger) Panic(msg string, args ...interface{}) {
	l.logger.PanicContext(l.ctx, msg, args...)
}

func (l *contextLogger) PanicContext(ctx context.Context, msg string, args ...interface{}) {
	l.logger.PanicContext(ctx, msg, args...)
}

// argsToFields преобразует аргументы вида [key1, val1, key2, val2...] в поля zap.Field
func argsToFields(args []interface{}) []zap.Field {
	fields := make([]zap.Field, 0, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		key, ok := args[i].(string)
		if !ok {
			continue
		}

		if i+1 < len(args) {
			fields = append(fields, zap.Any(key, args[i+1]))
		}
	}
	return fields
}
