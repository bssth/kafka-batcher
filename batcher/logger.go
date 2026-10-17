package batcher

var _ Logger = ZeroLogger{}

// Logger is the logging interface used by KafkaBatch.
type Logger interface {
	Info(args ...any)
	Warn(args ...any)
}

// ZeroLogger discards everything. It is the default logger.
type ZeroLogger struct{}

// Info does nothing.
func (ZeroLogger) Info(_ ...any) {}

// Warn does nothing.
func (ZeroLogger) Warn(_ ...any) {}
