// Copyright (c) 2016 Twitch Interactive

package kinsumer

import "time"

// A StatReceiver will have its methods called as operations
// happen inside a running kinsumer, and is useful for tracking
// the operation of the consumer.
//
// The methods will get called from multiple go routines and it is
// the implementors responsibility to handle thread synchronization
type StatReceiver interface {
	// Dynamo operations

	// Checkpoint is called every time a checkpoint is written to dynamodb
	Checkpoint()

	// EventToClient is called every time a record is returned to the client
	// `inserted` is the approximate time the record was inserted into kinesis
	// `retrieved` is the time when kinsumer retrieved the record from kinesis
	EventToClient(inserted, retrieved time.Time)

	// EventsFromKinesis is called every time a bunch of records is retrieved from
	// a kinesis shard.
	// `num` Number of records retrieved.
	// `shardID` ID of the shard that the records were retrieved from
	// `lag` How far the records are from the tip of the stream.
	EventsFromKinesis(num int, shardID string, lag time.Duration)

	// RecordsInMemory is called periodically to report the current number of records
	// that have been pulled from Kinesis and are buffered in memory, waiting to be
	// delivered to the client.
	// `count` Current number of records in the internal buffer
	RecordsInMemory(count int64)

	// RecordsInMemoryBytes is called periodically to report the current total bytes
	// of record payloads that have been pulled from Kinesis and are buffered in memory,
	// waiting to be delivered to the client.
	// `bytes` Current total payload bytes in the internal buffer
	RecordsInMemoryBytes(bytes int64)
}

// filteredStatReceiver wraps a StatReceiver and filters method calls based on MetricsConfig
// This allows selective enabling/disabling of metrics at the configuration level
type filteredStatReceiver struct {
	underlying StatReceiver
	config     MetricsConfig
}

// newFilteredStatReceiver creates a new filteredStatReceiver that wraps the underlying StatReceiver
func newFilteredStatReceiver(underlying StatReceiver, config MetricsConfig) StatReceiver {
	return &filteredStatReceiver{
		underlying: underlying,
		config:     config,
	}
}

func (f *filteredStatReceiver) Checkpoint() {
	if f.config.EnableCheckpoint {
		f.underlying.Checkpoint()
	}
}

func (f *filteredStatReceiver) EventToClient(inserted, retrieved time.Time) {
	if f.config.EnableEventToClient {
		f.underlying.EventToClient(inserted, retrieved)
	}
}

func (f *filteredStatReceiver) EventsFromKinesis(num int, shardID string, lag time.Duration) {
	if f.config.EnableEventsFromKinesis {
		f.underlying.EventsFromKinesis(num, shardID, lag)
	}
}

func (f *filteredStatReceiver) RecordsInMemory(count int64) {
	if f.config.EnableRecordsInMemory {
		f.underlying.RecordsInMemory(count)
	}
}

func (f *filteredStatReceiver) RecordsInMemoryBytes(bytes int64) {
	if f.config.EnableRecordsInMemoryBytes {
		f.underlying.RecordsInMemoryBytes(bytes)
	}
}
