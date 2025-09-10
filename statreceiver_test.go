// Copyright (c) 2016 Twitch Interactive

package kinsumer

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStatReceiver is a StatReceiver implementation that captures all method calls
// for testing purposes. It is thread-safe since StatReceiver methods can be called
// from multiple goroutines.
type TestStatReceiver struct {
	mu                        sync.Mutex
	recordsInMemoryCalls      []int64
	recordsInMemoryBytesCalls []int64
	checkpointCalls           int
	eventToClientCalls        int
	eventsFromKinesisCalls    []EventsFromKinesisCall
}

// EventsFromKinesisCall captures the parameters of EventsFromKinesis calls
type EventsFromKinesisCall struct {
	Num     int
	ShardID string
	Lag     time.Duration
}


// Checkpoint captures checkpoint calls
func (t *TestStatReceiver) Checkpoint() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.checkpointCalls++
}

// EventToClient captures event to client calls
func (t *TestStatReceiver) EventToClient(inserted, retrieved time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.eventToClientCalls++
}

// EventsFromKinesis captures events from kinesis calls
func (t *TestStatReceiver) EventsFromKinesis(num int, shardID string, lag time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.eventsFromKinesisCalls = append(t.eventsFromKinesisCalls, EventsFromKinesisCall{
		Num:     num,
		ShardID: shardID,
		Lag:     lag,
	})
}

// RecordsInMemory captures records in memory calls
func (t *TestStatReceiver) RecordsInMemory(count int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.recordsInMemoryCalls = append(t.recordsInMemoryCalls, count)
}

// RecordsInMemoryBytes captures records in memory bytes calls
func (t *TestStatReceiver) RecordsInMemoryBytes(bytes int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.recordsInMemoryBytesCalls = append(t.recordsInMemoryBytesCalls, bytes)
}

// Helper methods for testing

// GetRecordsInMemoryCalls returns a copy of all RecordsInMemory calls
func (t *TestStatReceiver) GetRecordsInMemoryCalls() []int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	calls := make([]int64, len(t.recordsInMemoryCalls))
	copy(calls, t.recordsInMemoryCalls)
	return calls
}

// GetRecordsInMemoryBytesCalls returns a copy of all RecordsInMemoryBytes calls
func (t *TestStatReceiver) GetRecordsInMemoryBytesCalls() []int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	calls := make([]int64, len(t.recordsInMemoryBytesCalls))
	copy(calls, t.recordsInMemoryBytesCalls)
	return calls
}

// GetLastRecordsInMemory returns the last RecordsInMemory value, or -1 if no calls
func (t *TestStatReceiver) GetLastRecordsInMemory() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.recordsInMemoryCalls) == 0 {
		return -1
	}
	return t.recordsInMemoryCalls[len(t.recordsInMemoryCalls)-1]
}

// GetLastRecordsInMemoryBytes returns the last RecordsInMemoryBytes value, or -1 if no calls
func (t *TestStatReceiver) GetLastRecordsInMemoryBytes() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.recordsInMemoryBytesCalls) == 0 {
		return -1
	}
	return t.recordsInMemoryBytesCalls[len(t.recordsInMemoryBytesCalls)-1]
}

// GetCallCounts returns counts of various method calls
func (t *TestStatReceiver) GetCallCounts() (recordsInMemory, recordsInMemoryBytes, checkpoint, eventToClient, eventsFromKinesis int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.recordsInMemoryCalls), len(t.recordsInMemoryBytesCalls), t.checkpointCalls, t.eventToClientCalls, len(t.eventsFromKinesisCalls)
}

// Reset clears all captured calls
func (t *TestStatReceiver) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.recordsInMemoryCalls = nil
	t.recordsInMemoryBytesCalls = nil
	t.checkpointCalls = 0
	t.eventToClientCalls = 0
	t.eventsFromKinesisCalls = nil
}

// WaitForRecordsInMemoryCalls waits until at least expectedCalls RecordsInMemory calls are made
// Returns true if the expected calls were made within the timeout
func (t *TestStatReceiver) WaitForRecordsInMemoryCalls(expectedCalls int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(t.GetRecordsInMemoryCalls()) >= expectedCalls {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// WaitForRecordsInMemoryBytesCalls waits until at least expectedCalls RecordsInMemoryBytes calls are made
// Returns true if the expected calls were made within the timeout
func (t *TestStatReceiver) WaitForRecordsInMemoryBytesCalls(expectedCalls int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(t.GetRecordsInMemoryBytesCalls()) >= expectedCalls {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// TestMetricsBasic tests that the new metrics are reported to StatReceiver as expected
func TestMetricsBasic(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	streamName := "TestMetricsBasic_stream"
	testStats := &TestStatReceiver{}

	k, d := kinesisAndDynamoInstances(t)

	defer func() {
		err := cleanupTestEnvironment(t, k, d, streamName)
		require.NoError(t, err, "Problems cleaning up the test environment")
	}()

	err := setupTestEnvironment(t, k, d, streamName, 1)
	require.NoError(t, err, "Problems setting up the test environment")

	// Configure with our test stat receiver
	config := NewConfig().WithBufferSize(100).WithStats(testStats)
	config = config.WithShardCheckFrequency(500 * time.Millisecond)
	config = config.WithLeaderActionFrequency(500 * time.Millisecond)

	kinsumer, err := NewWithInterfaces(k, d, streamName, *applicationName, "test_client", "", config)
	require.NoError(t, err, "NewWithInterfaces() failed")

	err = kinsumer.Run()
	require.NoError(t, err, "kinsumer.Run() failed")
	defer kinsumer.Stop()

	// Send records through the system
	recordCount := 200
	err = spamStream(t, k, int64(recordCount), streamName)
	require.NoError(t, err, "Problems sending test data")

	// Wait for metrics to be reported (should happen every 1 second)
	require.True(t, testStats.WaitForRecordsInMemoryCalls(2, 5*time.Second),
		"Expected RecordsInMemory calls after sending records")
	require.True(t, testStats.WaitForRecordsInMemoryBytesCalls(2, 5*time.Second),
		"Expected RecordsInMemoryBytes calls after sending records")

	// Verify metrics after sending records
	recordsAfterSend := testStats.GetLastRecordsInMemory()
	bytesAfterSend := testStats.GetLastRecordsInMemoryBytes()
	assert.Equal(t, int64(200), recordsAfterSend, "Should have 200 records in memory after sending")
	assert.Equal(t, int64(490), bytesAfterSend, "Should have 490 bytes in memory after sending") // 200 records * ~2.45 bytes avg

	// Consume 100 records to trigger metric decreases (should cause 2 batches of 50 decrements)
	var consumedCount int
	timeout := time.After(15 * time.Second)
	for consumedCount < 100 {
		select {
		case <-timeout:
			t.Fatalf("Timeout consuming records, got %d", consumedCount)
		default:
			data, err := kinsumer.Next()
			if err != nil {
				t.Fatalf("Error from Next(): %v", err)
			}
			if data != nil {
				consumedCount++
			}
		}
	}

	// Wait for additional metrics reporting after consumption
	time.Sleep(2 * time.Second)

	// Verify metrics after consuming records
	recordsAfterConsume := testStats.GetLastRecordsInMemory()
	bytesAfterConsume := testStats.GetLastRecordsInMemoryBytes()
	assert.Equal(t, int64(100), recordsAfterConsume, "Should have 100 records in memory after consuming 100")
	assert.Equal(t, int64(300), bytesAfterConsume, "Should have 300 bytes in memory after consuming 100")

	// Verify the new metrics are working
	recordCalls := testStats.GetRecordsInMemoryCalls()
	byteCalls := testStats.GetRecordsInMemoryBytesCalls()

	assert.True(t, len(recordCalls) > 0, "Should have RecordsInMemory calls")
	assert.True(t, len(byteCalls) > 0, "Should have RecordsInMemoryBytes calls")
	assert.Equal(t, len(recordCalls), len(byteCalls), "Record and byte calls should match")

	// Verify at least one call showed non-zero values (records were in memory)
	hasNonZeroRecords := false
	hasNonZeroBytes := false
	for _, count := range recordCalls {
		if count > 0 {
			hasNonZeroRecords = true
			break
		}
	}
	for _, bytes := range byteCalls {
		if bytes > 0 {
			hasNonZeroBytes = true
			break
		}
	}

	assert.True(t, hasNonZeroRecords, "Should have non-zero records in memory at some point")
	assert.True(t, hasNonZeroBytes, "Should have non-zero bytes in memory at some point")

	t.Logf("✓ Metrics test completed - RecordsInMemory calls: %v, RecordsInMemoryBytes calls: %v", 
		recordCalls, byteCalls)
}

// TestMetricsFiltering tests that metrics can be selectively enabled/disabled using the filteredStatReceiver
func TestMetricsFiltering(t *testing.T) {
	// Test that only RecordsInMemory metrics are captured when others are disabled
	testStats := &TestStatReceiver{}
	
	// Configure to only enable RecordsInMemory
	metricsConfig := MetricsConfig{
		EnableRecordsInMemory: true,
		// All others default to false
	}
	
	// Create a filtered stat receiver using the new approach (simulating WithStats behavior)
	filteredStats := newFilteredStatReceiver(testStats, metricsConfig)
	
	// Call all methods on the filtered receiver
	filteredStats.Checkpoint()
	filteredStats.EventToClient(time.Now(), time.Now())
	filteredStats.EventsFromKinesis(5, "shard-001", time.Second)
	filteredStats.RecordsInMemory(100)
	filteredStats.RecordsInMemoryBytes(500)
	
	// Verify only RecordsInMemory was called on the underlying receiver
	recordCount, byteCount, checkpointCount, eventToClientCount, eventsFromKinesisCount := testStats.GetCallCounts()
	
	assert.Equal(t, 1, recordCount, "Should have 1 RecordsInMemory call")
	assert.Equal(t, 0, byteCount, "Should have 0 RecordsInMemoryBytes calls (disabled)")
	assert.Equal(t, 0, checkpointCount, "Should have 0 Checkpoint calls (disabled)")
	assert.Equal(t, 0, eventToClientCount, "Should have 0 EventToClient calls (disabled)")
	assert.Equal(t, 0, eventsFromKinesisCount, "Should have 0 EventsFromKinesis calls (disabled)")
}

// TestMetricsFilteringOrderIndependence tests that the order of WithStats() and WithMetricsConfig() doesn't matter
func TestMetricsFilteringOrderIndependence(t *testing.T) {
	testStats1 := &TestStatReceiver{}
	testStats2 := &TestStatReceiver{}
	
	metricsConfig := MetricsConfig{
		EnableRecordsInMemory: true,
		// All others default to false
	}
	
	// Test order 1: WithStats first, then WithMetricsConfig
	config1 := NewConfig().
		WithStats(testStats1).
		WithMetricsConfig(metricsConfig)
	
	// Test order 2: WithMetricsConfig first, then WithStats  
	config2 := NewConfig().
		WithMetricsConfig(metricsConfig).
		WithStats(testStats2)
	
	// Simulate what happens in NewWithInterfaces - apply the filtering
	filteredStats1 := newFilteredStatReceiver(config1.stats, config1.metricsConfig)
	filteredStats2 := newFilteredStatReceiver(config2.stats, config2.metricsConfig)
	
	// Call all methods on both filtered receivers
	filteredStats1.Checkpoint()
	filteredStats1.RecordsInMemory(100)
	filteredStats1.RecordsInMemoryBytes(500)
	
	filteredStats2.Checkpoint() 
	filteredStats2.RecordsInMemory(100)
	filteredStats2.RecordsInMemoryBytes(500)
	
	// Both should have identical behavior regardless of order
	recordCount1, byteCount1, checkpointCount1, _, _ := testStats1.GetCallCounts()
	recordCount2, byteCount2, checkpointCount2, _, _ := testStats2.GetCallCounts()
	
	assert.Equal(t, recordCount1, recordCount2, "RecordsInMemory calls should be identical regardless of order")
	assert.Equal(t, byteCount1, byteCount2, "RecordsInMemoryBytes calls should be identical regardless of order") 
	assert.Equal(t, checkpointCount1, checkpointCount2, "Checkpoint calls should be identical regardless of order")
	
	// Verify the expected filtering (only RecordsInMemory should be called)
	assert.Equal(t, 1, recordCount1, "Should have 1 RecordsInMemory call")
	assert.Equal(t, 0, byteCount1, "Should have 0 RecordsInMemoryBytes calls (disabled)")
	assert.Equal(t, 0, checkpointCount1, "Should have 0 Checkpoint calls (disabled)")
}