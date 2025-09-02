// Copyright (c) 2016 Twitch Interactive

package kinsumer

import (
	"time"

	ktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

//TODO: Update documentation to include the defaults
//TODO: Update the 'with' methods' comments to be less ridiculous

// Config holds all configuration values for a single Kinsumer instance
type Config struct {
	stats               StatReceiver
	logger              Logger
	manualCheckpointing bool

	// ---------- [ Per Shard Worker ] ----------
	// Time to sleep if no records are found
	throttleDelay time.Duration

	// Delay between commits to the checkpoint database
	commitFrequency time.Duration

	// Delay between tests for the client or shard numbers changing
	shardCheckFrequency time.Duration

	// Max age for client record before we consider it stale
	clientRecordMaxAge *time.Duration

	// Starting timestamp of the shard iterator, if "AT_TIMESTAMP" is the desired iterator type
	iteratorStartTimestamp *time.Time

	// Iterator type to use when starting from an empty checkpoint (no previous sequence number)
	// Valid values:
	//   ktypes.ShardIteratorTypeTrimHorizon (default): Start reading from the oldest available record
	//   ktypes.ShardIteratorTypeLatest: Start reading from the newest record (skip existing data)
	//   ktypes.ShardIteratorTypeAtTimestamp: Use iteratorStartTimestamp to specify start position
	iteratorType ktypes.ShardIteratorType

	// ---------- [ For the leader (first client alphabetically) ] ----------
	// Time between leader actions
	leaderActionFrequency time.Duration

	// ---------- [ For the entire Kinsumer ] ----------
	// Size of the buffer for the combined records channel. When the channel fills up
	// the workers will stop adding new elements to the queue, so a slow client will
	// potentially fall behind the kinesis stream.
	bufferSize int

	// ---------- [ For the Dynamo DB tables ] ----------
	// Read and write capacity for the Dynamo DB tables when created
	// with CreateRequiredTables() call. If tables already exist because they were
	// created on a prevoius run or created manually, these parameters will not be used.
	dynamoReadCapacity  int64
	dynamoWriteCapacity int64
	// Time to wait between attempts to verify tables were created/deleted completely
	dynamoWaiterDelay time.Duration

	// use ListShards to avoid LimitExceedException from DescribeStream
	useListShardsForKinesisStreamReady bool

	// Maximum number of records to fetch per GetRecords request
	// AWS Kinesis allows up to 10,000 records per request (default)
	// Reducing this value helps control memory usage at the cost of increased API calls
	getRecordsLimit int
}

// NewConfig returns a default Config struct
func NewConfig() Config {
	return Config{
		throttleDelay:         250 * time.Millisecond,
		commitFrequency:       1000 * time.Millisecond,
		shardCheckFrequency:   1 * time.Minute,
		leaderActionFrequency: 1 * time.Minute,
		bufferSize:            100,
		stats:                 &NoopStatReceiver{},
		dynamoReadCapacity:    10,
		dynamoWriteCapacity:   10,
		dynamoWaiterDelay:     3 * time.Second,
		logger:                &DefaultLogger{},
		iteratorType:          ktypes.ShardIteratorTypeLatest,
		getRecordsLimit:       10000,
	}
}

// WithManualCheckpointing returns a Config with a modified manual checkpointing flag
// If set to false, records will be automatically checkpointed upon calls to Next()
// If set to true, NextWithCheckpointer() must be used and the returned checkpointer function
// must be called when the record is fully processed.
func (c Config) WithManualCheckpointing(v bool) Config {
	c.manualCheckpointing = v
	return c
}

// WithThrottleDelay returns a Config with a modified throttle delay
func (c Config) WithThrottleDelay(delay time.Duration) Config {
	c.throttleDelay = delay
	return c
}

// WithCommitFrequency returns a Config with a modified commit frequency
func (c Config) WithCommitFrequency(commitFrequency time.Duration) Config {
	c.commitFrequency = commitFrequency
	return c
}

// WithShardCheckFrequency returns a Config with a modified shard check frequency
func (c Config) WithShardCheckFrequency(shardCheckFrequency time.Duration) Config {
	c.shardCheckFrequency = shardCheckFrequency
	return c
}

// WithClientRecordMaxAge returns a config with a modified client record max age
func (c Config) WithClientRecordMaxAge(clientRecordMaxAge *time.Duration) Config {
	c.clientRecordMaxAge = clientRecordMaxAge
	return c
}

// WithLeaderActionFrequency returns a Config with a modified leader action frequency
func (c Config) WithLeaderActionFrequency(leaderActionFrequency time.Duration) Config {
	c.leaderActionFrequency = leaderActionFrequency
	return c
}

// WithBufferSize returns a Config with a modified buffer size
func (c Config) WithBufferSize(bufferSize int) Config {
	c.bufferSize = bufferSize
	return c
}

// WithStats returns a Config with a modified stats
func (c Config) WithStats(stats StatReceiver) Config {
	c.stats = stats
	return c
}

// WithIteratorStartTimestamp returns a Config with a modified iteratorStartTimestamp
func (c Config) WithIteratorStartTimestamp(timestamp *time.Time) Config {
	c.iteratorStartTimestamp = timestamp
	return c
}

// WithIteratorType returns a Config with a modified iterator type for new checkpoints
// This determines where to start reading when no previous checkpoint exists.
// Valid values:
//
//	ktypes.ShardIteratorTypeTrimHorizon (default): Start from the oldest available record
//	ktypes.ShardIteratorTypeLatest: Start from the newest record (skip all existing data)
//	ktypes.ShardIteratorTypeAtTimestamp: Start from iteratorStartTimestamp (must also call WithIteratorStartTimestamp)
func (c Config) WithIteratorType(iteratorType ktypes.ShardIteratorType) Config {
	c.iteratorType = iteratorType
	return c
}

// WithDynamoReadCapacity returns a Config with a modified dynamo read capacity
func (c Config) WithDynamoReadCapacity(readCapacity int64) Config {
	c.dynamoReadCapacity = readCapacity
	return c
}

// WithDynamoWriteCapacity returns a Config with a modified dynamo write capacity
func (c Config) WithDynamoWriteCapacity(writeCapacity int64) Config {
	c.dynamoWriteCapacity = writeCapacity
	return c
}

// WithDynamoWaiterDelay returns a Config with a modified dynamo waiter delay
func (c Config) WithDynamoWaiterDelay(delay time.Duration) Config {
	c.dynamoWaiterDelay = delay
	return c
}

// WithLogger returns a Config with a modified logger
func (c Config) WithLogger(logger Logger) Config {
	c.logger = logger
	return c
}

// WithUseListShardsForKinesisStreamReady returns a config with a modified useListShardsForKinesisStreamReady toggle
func (c Config) WithUseListShardsForKinesisStreamReady(shouldUse bool) Config {
	c.useListShardsForKinesisStreamReady = shouldUse
	return c
}

// WithGetRecordsLimit returns a Config with a modified maximum records per GetRecords request
// This controls how many records to fetch per GetRecords API call.
// AWS Kinesis allows up to 10,000 records per request. Reducing this helps control memory usage.
func (c Config) WithGetRecordsLimit(getRecordsLimit int) Config {
	c.getRecordsLimit = getRecordsLimit
	return c
}

// Verify that a config struct has sane and valid values
func validateConfig(c *Config) error {
	if c.throttleDelay < 200*time.Millisecond {
		return ErrConfigInvalidThrottleDelay
	}

	if c.commitFrequency == 0 {
		return ErrConfigInvalidCommitFrequency
	}

	if c.shardCheckFrequency == 0 {
		return ErrConfigInvalidShardCheckFrequency
	}

	if c.leaderActionFrequency == 0 {
		return ErrConfigInvalidLeaderActionFrequency
	}

	if c.clientRecordMaxAge != nil && *c.clientRecordMaxAge < c.shardCheckFrequency {
		return ErrConfigInvalidClientRecordMaxAge
	}

	if c.shardCheckFrequency > c.leaderActionFrequency {
		return ErrConfigInvalidLeaderActionFrequency
	}

	if c.bufferSize == 0 {
		return ErrConfigInvalidBufferSize
	}

	if c.stats == nil {
		return ErrConfigInvalidStats
	}

	if c.dynamoReadCapacity == 0 || c.dynamoWriteCapacity == 0 {
		return ErrConfigInvalidDynamoCapacity
	}

	if c.logger == nil {
		return ErrConfigInvalidLogger
	}

	// Validate iterator type is supported (empty string not allowed)
	if c.iteratorType != ktypes.ShardIteratorTypeTrimHorizon &&
		c.iteratorType != ktypes.ShardIteratorTypeLatest &&
		c.iteratorType != ktypes.ShardIteratorTypeAtTimestamp {
		return ErrConfigInvalidIteratorType
	}

	if c.iteratorType == ktypes.ShardIteratorTypeAtTimestamp && c.iteratorStartTimestamp == nil {
		return ErrConfigInvalidIteratorTimestamp
	}

	if c.getRecordsLimit <= 0 || c.getRecordsLimit > 10000 {
		return ErrConfigInvalidGetRecordsLimit
	}

	return nil
}
