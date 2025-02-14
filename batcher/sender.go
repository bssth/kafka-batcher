package batcher

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaBatch struct {
	sync.Mutex
	producer *kafka.Writer
	// queue is a channel to send logs to kafka. It's used to avoid blocking the main thread when sending logs
	queue chan []byte
	// buffer stores logs before sending them to kafka. It's used to batch logs and send them in bulk
	buffer []kafka.Message
	// frequency is how often to commit local data batch to Kafka
	frequency time.Duration
	logger    Logger
}

func (b *KafkaBatch) SetLogger(logger Logger) {
	b.logger = logger
}

func (b *KafkaBatch) RunBatch(ctx context.Context) {
	batchTicker := time.After(b.frequency)

	for {
		select {
		case <-batchTicker:
			if len(b.buffer) > 0 {
				b.Lock()
				err := b.producer.WriteMessages(
					ctx,
					b.buffer...,
				)
				if err != nil {
					b.logger.Warn("Failed to send messages to kafka", err)
				}
				b.buffer = nil
				b.Unlock()
			}
			batchTicker = time.After(b.frequency)
		case <-ctx.Done():
			b.logger.Info("Logs buffer commit stopped")
			return
		}
	}
}

func (b *KafkaBatch) RunQueueReader(ctx context.Context) {
	for {
		select {
		case data := <-b.queue:
			b.Lock()
			b.buffer = append(b.buffer, kafka.Message{
				Value: data,
			})
			b.Unlock()

		case <-ctx.Done():
			b.logger.Info("Logs producer stopped")
			return
		}
	}
}

var errLogsNotInitialized = errors.New("logs producer is not initialized")
var errEventIsNil = errors.New("event is nil")

// Send is a wrapper around kafka producer to send logs
func (b *KafkaBatch) Send(event any) error {
	if b.producer == nil {
		b.logger.Warn("Producer is not init")
		return errLogsNotInitialized
	}
	if event == nil {
		b.logger.Warn("Empty event")
		return errEventIsNil
	}

	data, err := json.Marshal(event)
	if err != nil {
		b.logger.Warn("Failed to marshal event", err)
		return err
	}

	b.queue <- data
	return err
}

func NewKafkaBatch(ctx context.Context, producer *kafka.Writer, frequency time.Duration, bufferSize uint) (*KafkaBatch, error) {
	batch := &KafkaBatch{}
	batch.queue = make(chan []byte, bufferSize)
	batch.producer = producer
	batch.frequency = frequency
	batch.logger = &ZeroLogger{}

	go batch.RunBatch(ctx)
	go batch.RunQueueReader(ctx)

	return batch, nil
}
