// Package batcher collects events in memory and writes them to Kafka in batches.
package batcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

// DefaultMaxBatchSize is how many messages a batch holds before it is written
// without waiting for the next tick. Change it with WithMaxBatchSize.
const DefaultMaxBatchSize = 1000

var (
	// ErrNotInitialized is returned when the batcher was not created with NewKafkaBatch
	// or was given no producer.
	ErrNotInitialized = errors.New("batcher: producer is not initialized")
	// ErrEventIsNil is returned by Send when the event is nil.
	ErrEventIsNil = errors.New("batcher: event is nil")
	// ErrClosed is returned when sending to a batcher that is closed or closing.
	ErrClosed = errors.New("batcher: batcher is closed")
	// ErrInvalidFrequency is returned by NewKafkaBatch when frequency is not positive.
	ErrInvalidFrequency = errors.New("batcher: frequency must be positive")
	// ErrInvalidBatchSize is returned by NewKafkaBatch when the max batch size is not positive.
	ErrInvalidBatchSize = errors.New("batcher: max batch size must be positive")
)

// Writer is the part of *kafka.Writer that KafkaBatch uses.
// Any other implementation (for example, a fake in tests) works too.
type Writer interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

// ErrorHandler receives a batch that could not be written and the error from the writer.
// It runs on the batching goroutine, so it should return quickly.
type ErrorHandler func(msgs []kafka.Message, err error)

// Option configures a KafkaBatch.
type Option func(b *KafkaBatch)

// WithLogger sets the logger. By default nothing is logged.
func WithLogger(logger Logger) Option {
	return func(b *KafkaBatch) {
		b.SetLogger(logger)
	}
}

// WithMaxBatchSize sets how many messages trigger a write before the next tick.
func WithMaxBatchSize(size int) Option {
	return func(b *KafkaBatch) {
		b.maxBatchSize = size
	}
}

// WithErrorHandler sets the function called when a batch fails to be written.
// By default the failure is logged with Warn and the batch is dropped.
func WithErrorHandler(handler ErrorHandler) Option {
	return func(b *KafkaBatch) {
		b.onError = handler
	}
}

// KafkaBatch queues messages and writes them to Kafka in batches:
// every frequency, or sooner when a batch reaches the max batch size.
//
// All methods are safe for concurrent use.
type KafkaBatch struct {
	producer     Writer
	queue        chan kafka.Message
	frequency    time.Duration
	maxBatchSize int
	onError      ErrorHandler
	logger       atomic.Pointer[loggerBox]

	// mu guards closed. Senders hold the read lock while they enqueue, so once
	// Close holds the write lock no message can be added behind the final drain.
	mu      sync.RWMutex
	closed  bool
	closing chan struct{} // closed when Close starts; wakes blocked senders
	stop    chan struct{} // closed when nothing can be enqueued anymore; run drains the queue and exits
	stopped chan struct{} // closed when run has exited

	closeOnce   sync.Once
	cancelWrite context.CancelFunc // aborts the write in progress when Close gives up
}

type loggerBox struct {
	Logger
}

// NewKafkaBatch starts a batcher that writes to producer every frequency.
// bufferSize is the capacity of the queue between Send and the batching goroutine;
// Send blocks while the queue is full.
//
// Cancelling ctx closes the batcher like Close does: queued messages are still
// written. Call Close to wait until that is done.
func NewKafkaBatch(
	ctx context.Context,
	producer Writer,
	frequency time.Duration,
	bufferSize uint,
	opts ...Option,
) (*KafkaBatch, error) {
	if producer == nil {
		return nil, ErrNotInitialized
	}
	if w, ok := producer.(*kafka.Writer); ok && w == nil {
		return nil, ErrNotInitialized
	}
	if frequency <= 0 {
		return nil, ErrInvalidFrequency
	}

	batch := &KafkaBatch{
		producer:     producer,
		queue:        make(chan kafka.Message, bufferSize),
		frequency:    frequency,
		maxBatchSize: DefaultMaxBatchSize,
		closing:      make(chan struct{}),
		stop:         make(chan struct{}),
		stopped:      make(chan struct{}),
	}
	batch.SetLogger(nil)
	batch.onError = batch.logWriteError

	for _, opt := range opts {
		opt(batch)
	}
	if batch.maxBatchSize <= 0 {
		return nil, ErrInvalidBatchSize
	}
	if batch.onError == nil {
		batch.onError = batch.logWriteError
	}

	// Writes keep ctx values (for tracing etc.) but not its cancellation:
	// queued messages must still be written after ctx is cancelled.
	writeCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	batch.cancelWrite = cancel
	go batch.run(writeCtx)

	if ctx.Done() != nil {
		go func() {
			select {
			case <-ctx.Done():
				_ = batch.Close(context.WithoutCancel(ctx))
			case <-batch.stopped:
			}
		}()
	}

	return batch, nil
}

// SetLogger replaces the logger. A nil logger turns logging off.
func (b *KafkaBatch) SetLogger(logger Logger) {
	if logger == nil {
		logger = ZeroLogger{}
	}
	b.logger.Store(&loggerBox{logger})
}

func (b *KafkaBatch) log() Logger {
	if box := b.logger.Load(); box != nil {
		return box.Logger
	}

	return ZeroLogger{}
}

// Send encodes event as JSON and queues it. It blocks while the queue is full
// and returns ErrClosed once the batcher is closed.
func (b *KafkaBatch) Send(event any) error {
	return b.SendContext(context.Background(), event)
}

// SendContext is like Send but stops waiting for queue space when ctx is done.
func (b *KafkaBatch) SendContext(ctx context.Context, event any) error {
	if event == nil {
		return ErrEventIsNil
	}

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("batcher: marshal event: %w", err)
	}

	return b.SendMessage(ctx, kafka.Message{Value: data})
}

// SendMessage queues a ready-made message, for example one with a Key or Headers.
// A message accepted without error is written before Close returns,
// unless the context given to Close ends first.
func (b *KafkaBatch) SendMessage(ctx context.Context, msg kafka.Message) error {
	if b == nil || b.queue == nil {
		return ErrNotInitialized
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.closed {
		return ErrClosed
	}

	select {
	case b.queue <- msg:
		return nil
	case <-b.closing:
		return ErrClosed
	case <-ctx.Done():
		return fmt.Errorf("batcher: send: %w", ctx.Err())
	}
}

// Close stops accepting messages, writes everything still queued and waits for it.
// If ctx ends first, the write in progress is cancelled, the unsent messages go
// to the error handler and the context error is returned.
// It is safe to call Close more than once.
func (b *KafkaBatch) Close(ctx context.Context) error {
	if b == nil || b.queue == nil {
		return ErrNotInitialized
	}

	b.closeOnce.Do(func() {
		close(b.closing) // senders blocked on a full queue give up now
		b.mu.Lock()      // wait for senders that are still enqueueing
		b.closed = true
		b.mu.Unlock()
		close(b.stop)
	})

	select {
	case <-b.stopped:
		return nil
	case <-ctx.Done():
		b.cancelWrite()

		return fmt.Errorf("batcher: close: %w", ctx.Err())
	}
}

// run is the only goroutine that touches the current batch, so it needs no lock.
func (b *KafkaBatch) run(ctx context.Context) {
	defer close(b.stopped)
	defer b.cancelWrite()

	ticker := time.NewTicker(b.frequency)
	defer ticker.Stop()

	var batch []kafka.Message

	for {
		select {
		case msg := <-b.queue:
			batch = b.add(ctx, batch, msg)
		case <-ticker.C:
			batch = b.flush(ctx, batch)
		case <-b.stop:
			for {
				select {
				case msg := <-b.queue:
					batch = b.add(ctx, batch, msg)
				default:
					b.flush(ctx, batch)
					b.log().Info("Kafka batcher stopped")

					return
				}
			}
		}
	}
}

func (b *KafkaBatch) add(ctx context.Context, batch []kafka.Message, msg kafka.Message) []kafka.Message {
	batch = append(batch, msg)
	if len(batch) >= b.maxBatchSize {
		return b.flush(ctx, batch)
	}

	return batch
}

// flush writes the batch and returns a new empty one. The old slice is never reused,
// because the error handler may keep it.
func (b *KafkaBatch) flush(ctx context.Context, batch []kafka.Message) []kafka.Message {
	if len(batch) == 0 {
		return batch
	}

	if err := b.producer.WriteMessages(ctx, batch...); err != nil {
		b.onError(batch, err)
	}

	return nil
}

func (b *KafkaBatch) logWriteError(msgs []kafka.Message, err error) {
	b.log().Warn("Failed to send messages to kafka, dropped:", len(msgs), err)
}
