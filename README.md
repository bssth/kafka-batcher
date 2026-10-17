# Kafka Batcher

[![Go Reference](https://pkg.go.dev/badge/github.com/bssth/kafka-batcher.svg)](https://pkg.go.dev/github.com/bssth/kafka-batcher)
[![CI](https://github.com/bssth/kafka-batcher/actions/workflows/ci.yml/badge.svg)](https://github.com/bssth/kafka-batcher/actions/workflows/ci.yml)

A small Go library that collects events in memory and writes them to Kafka in batches.
It uses [segmentio/kafka-go](https://github.com/segmentio/kafka-go) as the Kafka client.

- `Send` encodes an event as JSON and returns as soon as it is queued.
- A batch is written every `frequency`, or sooner when it reaches the max batch size.
- `Close` writes everything that is still queued, so nothing is lost on shutdown.

## Install

```bash
go get github.com/bssth/kafka-batcher
```

## Usage

```go
writer := &kafka.Writer{
	Addr:         kafka.TCP("localhost:9092"),
	Topic:        "events",
	BatchTimeout: 10 * time.Millisecond, // see "Tuning the writer" below
}
defer writer.Close()

batch, err := batcher.NewKafkaBatch(ctx, writer, time.Second, 1000,
	batcher.WithMaxBatchSize(500),
	batcher.WithErrorHandler(func(msgs []kafka.Message, err error) {
		log.Printf("dropped %d messages: %v", len(msgs), err)
	}),
)
if err != nil {
	return err
}

// Somewhere in your code:
_ = batch.Send(map[string]any{"event": "signup", "user": 42})

// Messages with a key or headers:
_ = batch.SendMessage(ctx, kafka.Message{Key: []byte("user-42"), Value: payload})

// On shutdown, write what is left. The timeout bounds how long this can take.
shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
if err := batch.Close(shutdownCtx); err != nil {
	log.Printf("not everything was written: %v", err)
}
```

## How it behaves

| Situation | What happens |
| --- | --- |
| Queue is full | `Send` waits. Use `SendContext` to limit the wait. |
| `Close` is called | New sends fail with `ErrClosed`, blocked sends return `ErrClosed`, queued messages are written. |
| `ctx` passed to `NewKafkaBatch` is cancelled | Same as `Close`. Call `Close` if you need to wait for the final write. |
| Kafka write fails | The error handler gets the batch. By default it is logged with `Warn` and dropped. Retries are done by `kafka.Writer` itself (`MaxAttempts`). |

## Tuning the writer

In its default (synchronous) mode, `kafka.Writer` waits up to `BatchTimeout` (1 second by default)
before sending a batch that is smaller than `BatchSize` (100 by default).
Since this library already does the batching, set a small `BatchTimeout` (for example 10ms)
and a `BatchSize` at least as large as `WithMaxBatchSize`, or every flush will take about a second.

### Why not `kafka.Writer{Async: true}`?

The async writer also batches in the background, reports errors through `Completion`
and flushes on `Close`. But its internal queue has no limit: if Kafka is slow, messages pile up in memory.
This library keeps a bounded queue, so `Send` waits (or `SendContext` gives up) instead,
and it encodes events as JSON for you. If you don't need those, the async writer alone is enough.

## Development

```bash
go test -race ./...
golangci-lint run
```
