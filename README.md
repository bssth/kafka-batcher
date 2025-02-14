# Kafka Batcher

[![Go Reference](https://pkg.go.dev/badge/github.com/bssth/kafka-batcher.svg)](https://pkg.go.dev/github.com/bssth/kafka-batcher)

This is a simple Kafka producer proxy that batches messages before sending them to the broker. It is written in Go and uses [segmentio/kafka-go](github.com/segmentio/kafka-go) as the Kafka client.
