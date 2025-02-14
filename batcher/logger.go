package batcher

var _ Logger = ZeroLogger{}

type Logger interface {
	Info(args ...interface{})
	Warn(args ...interface{})
}

type ZeroLogger struct{}

func (ZeroLogger) Info(args ...interface{}) {}
func (ZeroLogger) Warn(args ...interface{}) {}
