// Copyright (c) 2016 Twitch Interactive

package mocks

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/twitchscience/kinsumer/kinsumeriface"
)

// mockKinesisCallRecord tracks individual calls to MockKinesis methods
type mockKinesisCallRecord struct {
	operation string
	shardID   string
	startTime time.Time
	endTime   time.Time
	err       error
}

// MockKinesis mocks the Kinesis API in memory with call tracking and concurrency monitoring
type MockKinesis struct {
	kinsumeriface.KinesisAPI

	mu sync.Mutex

	// Call tracking
	calls         []mockKinesisCallRecord
	activeCalls   int
	maxConcurrent int
	totalCalls    int

	// Configuration
	delay                         time.Duration
	shouldError                   bool
	errorTriggerShard             string
	useGenericError               bool
	shouldErrorOnGetShardIterator bool

	// Mock data
	shards       map[string]*mockShard
	streamStatus string
}

type mockShard struct {
	shardID      string
	records      []types.Record
	iterator     string
	nextIterator string
	closed       bool
}

// NewMockKinesis creates a new MockKinesis with the specified shards
func NewMockKinesis(shardIDs []string) *MockKinesis {
	mk := &MockKinesis{
		calls:        make([]mockKinesisCallRecord, 0),
		shards:       make(map[string]*mockShard),
		streamStatus: "ACTIVE",
	}

	// Create mock shards
	for _, shardID := range shardIDs {
		mk.shards[shardID] = &mockShard{
			shardID:      shardID,
			records:      make([]types.Record, 0),
			iterator:     "iter-" + shardID + "-0",
			nextIterator: "iter-" + shardID + "-1",
			closed:       false,
		}

		// Add some mock records for each shard
		for j := 0; j < 10; j++ {
			recordData := []byte("record-" + shardID + "-" + fmt.Sprintf("%d", j))
			mk.shards[shardID].records = append(mk.shards[shardID].records, types.Record{
				Data:           recordData,
				PartitionKey:   aws.String("partition-" + shardID),
				SequenceNumber: aws.String("seq-" + shardID + "-" + fmt.Sprintf("%d", j)),
			})
		}
	}

	return mk
}

// SetDelay configures artificial delay for GetRecords calls
func (mk *MockKinesis) SetDelay(delay time.Duration) {
	mk.mu.Lock()
	defer mk.mu.Unlock()
	mk.delay = delay
}

// SetError configures GetRecords to return errors for a specific shard
func (mk *MockKinesis) SetError(shardID string, shouldError bool) {
	mk.mu.Lock()
	defer mk.mu.Unlock()
	mk.errorTriggerShard = shardID
	mk.shouldError = shouldError
}

// SetGenericError configures GetRecords to return a generic error (not ExpiredIteratorException)
func (mk *MockKinesis) SetGenericError(shardID string) {
	mk.mu.Lock()
	defer mk.mu.Unlock()
	mk.errorTriggerShard = shardID
	mk.shouldError = true
	mk.useGenericError = true
}

// SetEmptyRecords configures GetRecords to return empty records for a specific shard
func (mk *MockKinesis) SetEmptyRecords(shardID string) {
	mk.mu.Lock()
	defer mk.mu.Unlock()
	if shard, exists := mk.shards[shardID]; exists {
		shard.records = []types.Record{} // Clear all records
	}
}

// SetBothGetRecordsAndGetShardIteratorErrors configures both GetRecords and GetShardIterator to fail
func (mk *MockKinesis) SetBothGetRecordsAndGetShardIteratorErrors(shardID string) {
	mk.mu.Lock()
	defer mk.mu.Unlock()
	mk.errorTriggerShard = shardID
	mk.shouldError = true
	mk.useGenericError = false // Use ExpiredIteratorException for GetRecords
	mk.shouldErrorOnGetShardIterator = true
}

// GetMaxConcurrentCalls returns the maximum concurrent calls observed
func (mk *MockKinesis) GetMaxConcurrentCalls() int {
	mk.mu.Lock()
	defer mk.mu.Unlock()
	return mk.maxConcurrent
}

// GetCurrentActiveCalls returns the current number of active calls
func (mk *MockKinesis) GetCurrentActiveCalls() int {
	mk.mu.Lock()
	defer mk.mu.Unlock()
	return mk.activeCalls
}

// GetTotalCalls returns total number of GetRecords calls made
func (mk *MockKinesis) GetTotalCalls() int {
	mk.mu.Lock()
	defer mk.mu.Unlock()
	return mk.totalCalls
}

// WaitForConcurrentCalls blocks until the specified number of concurrent calls is reached
func (mk *MockKinesis) WaitForConcurrentCalls(targetCount int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		mk.mu.Lock()
		current := mk.activeCalls
		mk.mu.Unlock()

		if current >= targetCount {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// recordCall tracks a method call
func (mk *MockKinesis) recordCall(operation, shardID string, err error) {
	call := mockKinesisCallRecord{
		operation: operation,
		shardID:   shardID,
		startTime: time.Now(),
		err:       err,
	}
	mk.calls = append(mk.calls, call)
}

// GetShardIterator mocks the Kinesis GetShardIterator operation
func (mk *MockKinesis) GetShardIterator(ctx context.Context, input *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
	mk.mu.Lock()
	defer mk.mu.Unlock()

	shardID := aws.ToString(input.ShardId)

	// Check for configured errors
	if mk.shouldErrorOnGetShardIterator && shardID == mk.errorTriggerShard {
		mk.recordCall("GetShardIterator", shardID, fmt.Errorf("simulated GetShardIterator error"))
		return nil, fmt.Errorf("simulated GetShardIterator error")
	}

	mk.recordCall("GetShardIterator", shardID, nil)

	shard, exists := mk.shards[shardID]
	if !exists {
		return nil, &types.ResourceNotFoundException{Message: aws.String("Shard not found")}
	}

	return &kinesis.GetShardIteratorOutput{
		ShardIterator: &shard.iterator,
	}, nil
}

// GetRecords mocks the Kinesis GetRecords operation with concurrency tracking
func (mk *MockKinesis) GetRecords(ctx context.Context, input *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
	// Track call start
	mk.mu.Lock()
	mk.activeCalls++
	if mk.activeCalls > mk.maxConcurrent {
		mk.maxConcurrent = mk.activeCalls
	}
	mk.totalCalls++
	delay := mk.delay
	shouldError := mk.shouldError
	errorTrigger := mk.errorTriggerShard
	useGenericError := mk.useGenericError
	mk.mu.Unlock()

	// Extract shard ID from iterator (simplified)
	iterator := aws.ToString(input.ShardIterator)
	var shardID string
	if len(iterator) > 5 && iterator[:5] == "iter-" {
		// Extract shard ID from iterator format "iter-{shardID}-{seq}"
		parts := iterator[5:]                           // Remove "iter-" prefix
		if dashIndex := len(parts) - 2; dashIndex > 0 { // Find last dash
			shardID = parts[:dashIndex]
		}
	}

	// Ensure we decrement active calls when done
	defer func() {
		mk.mu.Lock()
		mk.activeCalls--
		mk.mu.Unlock()
	}()

	// Apply artificial delay if configured
	if delay > 0 {
		time.Sleep(delay)
	}

	// Check for configured errors
	if shouldError && shardID == errorTrigger {
		mk.mu.Lock()
		if useGenericError {
			mk.recordCall("GetRecords", shardID, fmt.Errorf("simulated kinesis error"))
			mk.mu.Unlock()
			return nil, fmt.Errorf("simulated kinesis error")
		} else {
			mk.recordCall("GetRecords", shardID, &types.ExpiredIteratorException{})
			mk.mu.Unlock()
			return nil, &types.ExpiredIteratorException{Message: aws.String("Iterator expired")}
		}
	}

	mk.mu.Lock()
	defer mk.mu.Unlock()

	mk.recordCall("GetRecords", shardID, nil)

	shard, exists := mk.shards[shardID]
	if !exists {
		return &kinesis.GetRecordsOutput{
			Records:            []types.Record{},
			NextShardIterator:  nil,
			MillisBehindLatest: aws.Int64(0),
		}, nil
	}

	// Return some records and update iterator
	var records []types.Record
	if len(shard.records) > 0 && !shard.closed {
		// Return first few records
		recordCount := len(shard.records)
		if recordCount > 3 {
			recordCount = 3 // Return max 3 records per call
		}
		records = shard.records[:recordCount]
		shard.records = shard.records[recordCount:]
	}

	var nextIterator *string
	if len(shard.records) > 0 && !shard.closed {
		nextIterator = &shard.nextIterator
		// Update iterator for next call
		shard.iterator = shard.nextIterator
		shard.nextIterator = shard.nextIterator + "-next"
	} else {
		// End of shard
		nextIterator = nil
	}

	return &kinesis.GetRecordsOutput{
		Records:            records,
		NextShardIterator:  nextIterator,
		MillisBehindLatest: aws.Int64(100),
	}, nil
}

// DescribeStream mocks the Kinesis DescribeStream operation
func (mk *MockKinesis) DescribeStream(ctx context.Context, input *kinesis.DescribeStreamInput, optFns ...func(*kinesis.Options)) (*kinesis.DescribeStreamOutput, error) {
	mk.mu.Lock()
	defer mk.mu.Unlock()

	mk.recordCall("DescribeStream", "", nil)

	var shards []types.Shard
	for shardID := range mk.shards {
		shards = append(shards, types.Shard{
			ShardId: aws.String(shardID),
			HashKeyRange: &types.HashKeyRange{
				StartingHashKey: aws.String("0"),
				EndingHashKey:   aws.String("1000"),
			},
			SequenceNumberRange: &types.SequenceNumberRange{
				StartingSequenceNumber: aws.String("0"),
			},
		})
	}

	return &kinesis.DescribeStreamOutput{
		StreamDescription: &types.StreamDescription{
			StreamName:   input.StreamName,
			StreamStatus: types.StreamStatus(mk.streamStatus),
			Shards:       shards,
		},
	}, nil
}

// Implement other required methods as no-ops for interface compliance

func (mk *MockKinesis) CreateStream(ctx context.Context, input *kinesis.CreateStreamInput, optFns ...func(*kinesis.Options)) (*kinesis.CreateStreamOutput, error) {
	return &kinesis.CreateStreamOutput{}, nil
}

func (mk *MockKinesis) DeleteStream(ctx context.Context, input *kinesis.DeleteStreamInput, optFns ...func(*kinesis.Options)) (*kinesis.DeleteStreamOutput, error) {
	return &kinesis.DeleteStreamOutput{}, nil
}

func (mk *MockKinesis) PutRecords(ctx context.Context, input *kinesis.PutRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.PutRecordsOutput, error) {
	return &kinesis.PutRecordsOutput{
		FailedRecordCount: aws.Int32(0),
	}, nil
}

func (mk *MockKinesis) MergeShards(ctx context.Context, input *kinesis.MergeShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.MergeShardsOutput, error) {
	return &kinesis.MergeShardsOutput{}, nil
}
