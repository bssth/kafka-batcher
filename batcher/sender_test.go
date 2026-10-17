package batcher

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	testTimeout = 5 * time.Second
	neverTick   = time.Hour
)

var errKafkaDown = errors.New("kafka is down")

// fakeWriter records written batches. If release is set, every write waits
// for it (or for ctx) before doing anything.
type fakeWriter struct {
	mu      sync.Mutex
	batches [][]kafka.Message
	err     error
	release chan struct{}
	started chan struct{}
}

func (w *fakeWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	if w.started != nil {
		select {
		case w.started <- struct{}{}:
		default:
		}
	}
	if w.release != nil {
		select {
		case <-w.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	w.batches = append(w.batches, append([]kafka.Message(nil), msgs...))

	return w.err
}

func (w *fakeWriter) sizes() []int {
	w.mu.Lock()
	defer w.mu.Unlock()

	sizes := make([]int, 0, len(w.batches))
	for _, batch := range w.batches {
		sizes = append(sizes, len(batch))
	}

	return sizes
}

func (w *fakeWriter) values() []string {
	w.mu.Lock()
	defer w.mu.Unlock()

	var values []string
	for _, batch := range w.batches {
		for _, msg := range batch {
			values = append(values, string(msg.Value))
		}
	}

	return values
}

func newBatch(t *testing.T, writer Writer, frequency time.Duration, bufferSize uint, opts ...Option) *KafkaBatch {
	t.Helper()

	batch, err := NewKafkaBatch(context.Background(), writer, frequency, bufferSize, opts...)
	if err != nil {
		t.Fatalf("NewKafkaBatch: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		defer cancel()
		_ = batch.Close(ctx)
	})

	return batch
}

func closeBatch(t *testing.T, batch *KafkaBatch) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	if err := batch.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(testTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func equalInts(got, want []int) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}

	return true
}

func TestFlushesOnInterval(t *testing.T) {
	t.Parallel()

	writer := &fakeWriter{}
	batch := newBatch(t, writer, 10*time.Millisecond, 10)

	for i := range 3 {
		if err := batch.Send(map[string]int{"n": i}); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}

	waitFor(t, "3 written messages", func() bool { return len(writer.values()) == 3 })

	want := []string{`{"n":0}`, `{"n":1}`, `{"n":2}`}
	for i, got := range writer.values() {
		if got != want[i] {
			t.Errorf("message %d = %s, want %s", i, got, want[i])
		}
	}
}

func TestFlushesWhenBatchIsFull(t *testing.T) {
	t.Parallel()

	writer := &fakeWriter{}
	batch := newBatch(t, writer, neverTick, 10, WithMaxBatchSize(2))

	for i := range 5 {
		if err := batch.Send(i); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}

	waitFor(t, "two full batches", func() bool { return equalInts(writer.sizes(), []int{2, 2}) })

	closeBatch(t, batch)

	if got := writer.sizes(); !equalInts(got, []int{2, 2, 1}) {
		t.Fatalf("batch sizes = %v, want [2 2 1]", got)
	}
}

func TestCloseWritesQueuedMessages(t *testing.T) {
	t.Parallel()

	writer := &fakeWriter{}
	batch := newBatch(t, writer, neverTick, 100)

	for i := range 50 {
		if err := batch.Send(i); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}
	closeBatch(t, batch)

	values := writer.values()
	if len(values) != 50 {
		t.Fatalf("written %d messages, want 50", len(values))
	}
	for i, value := range values {
		if value != strconv.Itoa(i) {
			t.Fatalf("message %d = %s, want %d", i, value, i)
		}
	}
}

func TestSendAfterCloseFails(t *testing.T) {
	t.Parallel()

	batch := newBatch(t, &fakeWriter{}, neverTick, 1)
	closeBatch(t, batch)

	if err := batch.Send(1); !errors.Is(err, ErrClosed) {
		t.Fatalf("Send after Close = %v, want ErrClosed", err)
	}
	// A second Close is fine.
	closeBatch(t, batch)
}

func TestCloseUnblocksSenderAndGivesUpOnTimeout(t *testing.T) {
	t.Parallel()

	writer := &fakeWriter{release: make(chan struct{}), started: make(chan struct{}, 1)}

	var (
		failedMu sync.Mutex
		failed   int
		lastErr  error
	)
	onError := func(msgs []kafka.Message, err error) {
		failedMu.Lock()
		defer failedMu.Unlock()
		failed += len(msgs)
		lastErr = err
	}
	batch := newBatch(t, writer, neverTick, 0, WithMaxBatchSize(1), WithErrorHandler(onError))

	// The first message is taken by the batching goroutine, which then hangs in the writer.
	if err := batch.Send(1); err != nil {
		t.Fatalf("Send: %v", err)
	}
	<-writer.started

	// The queue is unbuffered and nobody reads it, so this Send blocks.
	sendErr := make(chan error, 1)
	go func() { sendErr <- batch.Send(2) }()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if err := batch.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close = %v, want context.DeadlineExceeded", err)
	}

	select {
	case err := <-sendErr:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("blocked Send = %v, want ErrClosed", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("blocked Send did not return after Close")
	}

	waitFor(t, "error handler", func() bool {
		failedMu.Lock()
		defer failedMu.Unlock()

		return failed == 1 && errors.Is(lastErr, context.Canceled)
	})
}

func TestSendContextStopsWaiting(t *testing.T) {
	t.Parallel()

	writer := &fakeWriter{release: make(chan struct{}), started: make(chan struct{}, 1)}
	batch := newBatch(t, writer, neverTick, 0, WithMaxBatchSize(1))
	t.Cleanup(func() { close(writer.release) })

	if err := batch.Send(1); err != nil {
		t.Fatalf("Send: %v", err)
	}
	<-writer.started

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	if err := batch.SendContext(ctx, 2); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("SendContext = %v, want context.DeadlineExceeded", err)
	}
}

func TestCancelledContextClosesBatcher(t *testing.T) {
	t.Parallel()

	writer := &fakeWriter{}
	ctx, cancel := context.WithCancel(context.Background())

	batch, err := NewKafkaBatch(ctx, writer, neverTick, 10)
	if err != nil {
		t.Fatalf("NewKafkaBatch: %v", err)
	}
	for i := range 3 {
		if err := batch.Send(i); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}
	cancel()

	waitFor(t, "messages written after cancel", func() bool { return len(writer.values()) == 3 })
	waitFor(t, "Send to fail after cancel", func() bool { return errors.Is(batch.Send(4), ErrClosed) })
}

func TestWriteErrorGoesToHandler(t *testing.T) {
	t.Parallel()

	writer := &fakeWriter{err: errKafkaDown}

	got := make(chan error, 1)
	onError := func(msgs []kafka.Message, err error) {
		if len(msgs) == 2 {
			got <- err
		}
	}
	batch := newBatch(t, writer, neverTick, 10, WithMaxBatchSize(2), WithErrorHandler(onError))

	_ = batch.Send(1)
	_ = batch.Send(2)

	select {
	case err := <-got:
		if !errors.Is(err, errKafkaDown) {
			t.Fatalf("handler error = %v, want %v", err, errKafkaDown)
		}
	case <-time.After(testTimeout):
		t.Fatal("error handler was not called")
	}
}

func TestSendMessageKeepsKey(t *testing.T) {
	t.Parallel()

	writer := &fakeWriter{}
	batch := newBatch(t, writer, neverTick, 1)

	msg := kafka.Message{Key: []byte("user-1"), Value: []byte("raw")}
	if err := batch.SendMessage(context.Background(), msg); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	closeBatch(t, batch)

	writer.mu.Lock()
	defer writer.mu.Unlock()

	if len(writer.batches) != 1 || string(writer.batches[0][0].Key) != "user-1" {
		t.Fatalf("written batches = %v, want one message with key user-1", writer.batches)
	}
}

func TestInvalidArguments(t *testing.T) {
	t.Parallel()

	var nilWriter *kafka.Writer

	tests := []struct {
		name      string
		producer  Writer
		frequency time.Duration
		opts      []Option
		want      error
	}{
		{name: "nil producer", producer: nil, frequency: time.Second, want: ErrNotInitialized},
		{name: "typed nil writer", producer: nilWriter, frequency: time.Second, want: ErrNotInitialized},
		{name: "zero frequency", producer: &fakeWriter{}, frequency: 0, want: ErrInvalidFrequency},
		{name: "negative frequency", producer: &fakeWriter{}, frequency: -time.Second, want: ErrInvalidFrequency},
		{
			name:      "zero batch size",
			producer:  &fakeWriter{},
			frequency: time.Second,
			opts:      []Option{WithMaxBatchSize(0)},
			want:      ErrInvalidBatchSize,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewKafkaBatch(context.Background(), testCase.producer, testCase.frequency, 1, testCase.opts...)
			if !errors.Is(err, testCase.want) {
				t.Fatalf("err = %v, want %v", err, testCase.want)
			}
		})
	}
}

func TestBadEvents(t *testing.T) {
	t.Parallel()

	batch := newBatch(t, &fakeWriter{}, neverTick, 1)

	if err := batch.Send(nil); !errors.Is(err, ErrEventIsNil) {
		t.Errorf("Send(nil) = %v, want ErrEventIsNil", err)
	}

	var typeErr *json.UnsupportedTypeError
	if err := batch.Send(make(chan int)); !errors.As(err, &typeErr) {
		t.Errorf("Send(chan) = %v, want *json.UnsupportedTypeError", err)
	}
}

func TestZeroValueIsNotUsable(t *testing.T) {
	t.Parallel()

	var batch KafkaBatch
	if err := batch.Send(1); !errors.Is(err, ErrNotInitialized) {
		t.Errorf("Send = %v, want ErrNotInitialized", err)
	}
	if err := batch.Close(context.Background()); !errors.Is(err, ErrNotInitialized) {
		t.Errorf("Close = %v, want ErrNotInitialized", err)
	}
}

// Every message that Send accepted must be written, even when Close runs
// while senders are still busy. Run with -race.
func TestNoAcceptedMessageIsLost(t *testing.T) {
	t.Parallel()

	const (
		senders   = 8
		perSender = 500
	)

	writer := &fakeWriter{}
	batch := newBatch(t, writer, time.Millisecond, 16, WithMaxBatchSize(7))

	var (
		accepted atomic.Int64
		running  sync.WaitGroup
	)
	for range senders {
		running.Add(1)
		go func() {
			defer running.Done()
			for i := range perSender {
				if i == perSender/2 {
					batch.SetLogger(ZeroLogger{})
				}
				if batch.Send(i) == nil {
					accepted.Add(1)
				}
			}
		}()
	}

	time.Sleep(5 * time.Millisecond)
	closeBatch(t, batch)
	running.Wait()

	if got, want := int64(len(writer.values())), accepted.Load(); got != want {
		t.Fatalf("written %d messages, accepted %d", got, want)
	}
}
