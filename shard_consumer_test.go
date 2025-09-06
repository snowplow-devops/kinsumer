package kinsumer

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/twitchscience/kinsumer/kinsumeriface"
	"github.com/twitchscience/kinsumer/mocks"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestShardConsumer aims to isolate the basic behaviour of shard consumer, without needing to run the whole program.
func TestShardConsumer(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	streamName := "TestShardConsumer_stream"

	k, dynamo := kinesisAndDynamoInstances(t)

	defer func() {
		err := cleanupTestEnvironment(t, k, dynamo, streamName)
		require.NoError(t, err, "Problems cleaning up the test environment")
	}()

	err := setupTestEnvironment(t, k, dynamo, streamName, 1)
	require.NoError(t, err, "Problems setting up the test environment")

	config := NewConfig().
		WithBufferSize(1000).
		WithShardCheckFrequency(500 * time.Millisecond).
		WithLeaderActionFrequency(500 * time.Millisecond).
		WithIteratorType(types.ShardIteratorTypeTrimHorizon)
	kinsumer1, err := NewWithInterfaces(k, dynamo, streamName, *applicationName, "client_1", "", config)
	require.NoError(t, err)

	desc, err := k.DescribeStream(t.Context(), &kinesis.DescribeStreamInput{
		StreamName: &streamName,
		Limit:      aws.Int32(shardLimit),
	})
	require.NoError(t, err, "Error describing stream")
	shard := *desc.StreamDescription.Shards[0].ShardId // Get shard ID

	// Consume a shard manually
	kinsumer1.waitGroup.Add(1) // consume will mark waitgroup as done on exit, so we add to it to avoid a panic
	go kinsumer1.consume(shard)

	go spamStreamModified(t, k, 100, streamName, 0)

	result := readEventsToSlice(kinsumer1.records, 5*time.Second)

	assert.Equal(t, 100, len(result))
}

// TestForcefulOwnershipChange aims to isolate the basic conditions where a client claims ownership of a shard before another client has released it.
func TestForcefulOwnershipChange(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	streamName := "TestForcefulOwnershipChange_stream"

	k, dynamo := kinesisAndDynamoInstances(t)

	defer func() {
		err := cleanupTestEnvironment(t, k, dynamo, streamName)
		require.NoError(t, err, "Problems cleaning up the test environment")
	}()

	err := setupTestEnvironment(t, k, dynamo, streamName, 1)
	require.NoError(t, err, "Problems setting up the test environment")

	// Create two kinsumer instances, but don't call run
	config := NewConfig().
		WithBufferSize(1000).
		WithShardCheckFrequency(500 * time.Millisecond).
		WithLeaderActionFrequency(500 * time.Millisecond).
		WithCommitFrequency(100 * time.Millisecond).
		WithIteratorType(types.ShardIteratorTypeTrimHorizon)

	// Set vastly different max ages to synthetically create a forced ownership change
	maxAge1 := 10 * time.Second
	config1 := config.WithClientRecordMaxAge(&maxAge1)

	maxAge2 := 500 * time.Millisecond
	config2 := config.WithClientRecordMaxAge(&maxAge2)

	kinsumer1, err1 := NewWithInterfaces(k, dynamo, streamName, *applicationName, "client_1", "", config1)
	kinsumer2, err2 := NewWithInterfaces(k, dynamo, streamName, *applicationName, "client_2", "", config2)
	require.NoError(t, err1)
	require.NoError(t, err2)

	desc, err := k.DescribeStream(t.Context(), &kinesis.DescribeStreamInput{
		StreamName: &streamName,
		Limit:      aws.Int32(shardLimit),
	})
	require.NoError(t, err, "Error describing stream")
	shard := *desc.StreamDescription.Shards[0].ShardId // Get shard ID

	// Consume a shard manually
	kinsumer1.waitGroup.Add(1) // consume will mark waitgroup as done on exit, so we add to it to avoid a panic
	go kinsumer1.consume(shard)

	go spamStreamModified(t, k, 2000, streamName, 0)

	kinsumer1ResultBeforeOwnerChange := readEventsToSlice(kinsumer1.records, 5*time.Second)

	assert.Equal(t, 2000, len(kinsumer1ResultBeforeOwnerChange))

	kinsumer2.waitGroup.Add(1) // consume will mark waitgroup as done on exit, so we add to it to avoid a panic
	go kinsumer2.consume(shard)

	// Because we retain the shard if no data is coming through, we mimic a stale client scenario by sending data but not acking
	lastK1Record := &consumedRecord{
		checkpointer: &checkpointer{},
	}
OwnerChangeLoop:
	for {
		spamStreamModified(t, k, 1, streamName, 9999)
	getEventLoop:
		select {
		case k1record := <-kinsumer1.records: // if kinsumer1 gets it, don't ack
			lastK1Record = k1record
			break getEventLoop
		case k2record := <-kinsumer2.records: // if kisumer2 gets it, ownership has changed. Ack then move on to the test.
			k2record.checkpointer.update(aws.ToString(k2record.record.SequenceNumber))
			// because this may be called with no genuine record to k1, we use the k2 sequence number.
			// this shouldn't make a difference since this commit will fail.
			lastK1Record.checkpointer.update(aws.ToString(k2record.record.SequenceNumber)) // Ack the last k1 record we have, to instigate behaviour we would see for that client
			break OwnerChangeLoop
		}
		time.Sleep(120 * time.Millisecond)
	}

	time.Sleep(300 * time.Millisecond)

	go spamStreamModified(t, k, 1000, streamName, 5000)

	resultsAfterOwnerChange := readMultipleToSlice([]chan *consumedRecord{kinsumer1.records, kinsumer2.records}, 5*time.Second)
	kinsumer1ResultAfterOwnerChangePreClean := resultsAfterOwnerChange[0]
	kinsumer2ResultAfterOwnerChangePreClean := resultsAfterOwnerChange[1]

	// clean out the records we just used to instigate a change in ownership
	kinsumer1ResultAfterOwnerChange := make([]*consumedRecord, 0)
	for _, val := range kinsumer1ResultAfterOwnerChangePreClean {
		if string(val.record.Data) != "9999" {
			kinsumer1ResultAfterOwnerChange = append(kinsumer1ResultAfterOwnerChange, val)
		}
	}

	kinsumer2ResultAfterOwnerChange := make([]*consumedRecord, 0)
	for _, val := range kinsumer2ResultAfterOwnerChangePreClean {
		if string(val.record.Data) != "9999" {
			kinsumer2ResultAfterOwnerChange = append(kinsumer2ResultAfterOwnerChange, val)
		}
	}

	/*
		// Leaving this here but commented out since it's useful in inspecting the behaviour when something does look off.
		if len(resultsAfterOwnerChange) > 0 {
			investigationSlice := make([]string, 0)
			for _, record := range kinsumer1ResultAfterOwnerChange {
				investigationSlice = append(investigationSlice, string(record.record.Data))
			}
			fmt.Println(investigationSlice)
		}
	*/

	assert.Equal(t, 0, len(kinsumer1ResultAfterOwnerChange))
	assert.Equal(t, 1000, len(kinsumer2ResultAfterOwnerChange))

	dupes := make([]string, 0)

	for _, val1 := range kinsumer1ResultAfterOwnerChange {
		for _, val2 := range kinsumer2ResultAfterOwnerChange {
			if string(val1.record.Data) == string(val2.record.Data) {
				dupes = append(dupes, string(val1.record.Data))
			}
		}
	}

	// Check that every expected value is present in the results
	missingIntegers := make([]int, 0)
	for i := 5000; i < 6000; i++ {
		present := false
		for _, val := range kinsumer2ResultAfterOwnerChange {
			if string(val.record.Data) == fmt.Sprint(i) {
				present = true
			}
		}
		if !present {
			missingIntegers = append(missingIntegers, i)
		}
	}
	assert.Equal(t, 0, len(missingIntegers), fmt.Sprint("Missing data: ", missingIntegers))
}

// TestPotentialLegitimateDuplicates aims to recreate the case where an ownership change leads to legitimate, duplicates,
// and to ensure that a) the only duplicates found come from the batch of data for which duplicate cases can't be avoided
// and b) the number of duplicates found is within the lower bounds of what we expect as reasonable.
func TestPotentialLegitimateDuplicates(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	streamName := "TestPotentialLegitimateDuplicates_stream"

	k, dynamo := kinesisAndDynamoInstances(t)

	defer func() {
		err := cleanupTestEnvironment(t, k, dynamo, streamName)
		require.NoError(t, err, "Problems cleaning up the test environment")
	}()

	err := setupTestEnvironment(t, k, dynamo, streamName, 1)
	require.NoError(t, err, "Problems setting up the test environment")

	// Create two kinsumer instances, but don't call run
	config := NewConfig().
		WithBufferSize(1000).
		WithShardCheckFrequency(500 * time.Millisecond).
		WithLeaderActionFrequency(500 * time.Millisecond).
		WithCommitFrequency(100 * time.Millisecond).
		WithIteratorType(types.ShardIteratorTypeTrimHorizon)

	// Set vastly different max ages to synthetically create a forced ownership change
	maxAge1 := 10 * time.Second
	config1 := config.WithClientRecordMaxAge(&maxAge1)

	maxAge2 := 500 * time.Millisecond
	config2 := config.WithClientRecordMaxAge(&maxAge2)

	kinsumer1, err := NewWithInterfaces(k, dynamo, streamName, *applicationName, "client_1", "", config1)
	require.NoError(t, err)
	kinsumer2, err := NewWithInterfaces(k, dynamo, streamName, *applicationName, "client_2", "", config2)
	require.NoError(t, err)

	desc, err := k.DescribeStream(t.Context(), &kinesis.DescribeStreamInput{
		StreamName: &streamName,
		Limit:      aws.Int32(shardLimit),
	})
	require.NoError(t, err, "Error describing stream")
	shard := *desc.StreamDescription.Shards[0].ShardId // Get shard ID

	// Consume a shard manually
	kinsumer1.waitGroup.Add(1) // consume will mark waitgroup as done on exit, so we add to it to avoid a panic
	go kinsumer1.consume(shard)

	go spamStreamModified(t, k, 2000, streamName, 0)

	kinsumer1ResultBeforeOwnerChange := readEventsToSlice(kinsumer1.records, 5*time.Second)

	assert.Equal(t, 2000, len(kinsumer1ResultBeforeOwnerChange))

	go func() {
		for i := 2000; i < 4000; i += 200 {
			spamStreamModified(t, k, 200, streamName, int64(i)) // Send data while ownership is changing, to create the scenario where duplicates are legitimately possible. (They may still be unlikely)
			time.Sleep(600 * time.Millisecond)                  // Sleep for a bit between, to allow the forced ownership change to happen.
		}

	}()

	// Consume the same shard with a new client - because of our clients' configurations, this client will forcefully claim ownership of the shard.
	kinsumer2.waitGroup.Add(1) // consume will mark waitgroup as done on exit, so we add to it to avoid a panic
	go kinsumer2.consume(shard)
	// At some point here the ownership should change, but if this happens while data is coming in there is some chance thaT duplicates are produced. These are unavoidable and so acceptable duplicates.

	resultsDuringOwnerChange := readMultipleToSlice([]chan *consumedRecord{kinsumer1.records, kinsumer2.records}, 5*time.Second)
	kinsumer1ResultDuringOwnerChange := resultsDuringOwnerChange[0]
	kinsumer2ResultDuringOwnerChange := resultsDuringOwnerChange[1]

	bothResultsDuringChange := append(kinsumer1ResultDuringOwnerChange, kinsumer2ResultDuringOwnerChange...)

	assert.LessOrEqual(t, 2000, len(bothResultsDuringChange)) // Check that we got at least the total number of expected events

	// Check that every expected value is present in the results
	missingIntegers := make([]int, 0)
	for i := 2000; i < 4000; i++ {
		present := false
		for _, val := range bothResultsDuringChange {
			if string(val.record.Data) == fmt.Sprint(i) {
				present = true
			}
		}
		if !present {
			missingIntegers = append(missingIntegers, i)
		}
	}
	assert.Equal(t, 0, len(missingIntegers), fmt.Sprint("Missing data: ", missingIntegers))

	dupes := make([]string, 0)

	for _, val1 := range kinsumer1ResultDuringOwnerChange {
		for _, val2 := range kinsumer2ResultDuringOwnerChange {
			if string(val1.record.Data) == string(val2.record.Data) {
				dupes = append(dupes, string(val1.record.Data))
			}
		}
	}
	// If more than one batch of kinesis records is duplicated, something is likely wrong - the relinquishing checkpointer should stop processing data faster than that
	assert.Greater(t, 201, len(dupes))

	/*
		// Leaving this here but commented out since it's useful in inspecting the behaviour when something does look off.
		k1ResStrings := make([]string, 0)
		k2ResStrings := make([]string, 0)
		for _, record := range kinsumer1ResultDuringOwnerChange {
			k1ResStrings = append(k1ResStrings, string(record.record.Data))
		}
		for _, record := range kinsumer2ResultDuringOwnerChange {
			k2ResStrings = append(k2ResStrings, string(record.record.Data))
		}
	*/

	// After ownership has changed, there should no longer be any possibility of duplicates.
	go spamStreamModified(t, k, 2000, streamName, 4000)

	resultsAfterOwnerChange := readMultipleToSlice([]chan *consumedRecord{kinsumer1.records, kinsumer2.records}, 5*time.Second)
	kinsumer1ResultAfterOwnerChange := resultsAfterOwnerChange[0]
	kinsumer2ResultAfterOwnerChange := resultsAfterOwnerChange[1]

	/*
		// Leaving this here but commented out since it's useful in inspecting the behaviour when something does look off.
		investigationSlice := make([]string, 0)
		for _, record := range kinsumer1ResultAfterOwnerChange {
			investigationSlice = append(investigationSlice, string(record.record.Data))
		}
	*/

	assert.Equal(t, 0, len(kinsumer1ResultAfterOwnerChange))
	assert.Equal(t, 2000, len(kinsumer2ResultAfterOwnerChange))
}

// TestShardsMerged aims to isolate the behaviour of consumers when shards are merged. It was originally added to investigate the hypothesis that
// duplicates in TestSplit were down to some incorrect handling of merging shards. This proved not to be the case, and the unit tests below further isolate the cause
// for that phenomenon, but there's no harm in keeping this test to isolate the behaviour of consumers when shards merge.
func TestShardsMerged(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	streamName := "TestShardsMerged_stream"

	k, dynamo := kinesisAndDynamoInstances(t)

	defer func() {
		err := cleanupTestEnvironment(t, k, dynamo, streamName)
		require.NoError(t, err, "Problems cleaning up the test environment")
	}()

	err := setupTestEnvironment(t, k, dynamo, streamName, 2)
	require.NoError(t, err, "Problems setting up the test environment")

	// Create two kinsumer instances, but don't call run
	config := NewConfig().
		WithBufferSize(1000).
		WithShardCheckFrequency(500 * time.Millisecond).
		WithLeaderActionFrequency(500 * time.Millisecond).
		WithCommitFrequency(100 * time.Millisecond).
		WithIteratorType(types.ShardIteratorTypeLatest)

	kinsumer1, err := NewWithInterfaces(k, dynamo, streamName, *applicationName, "client_1", "", config)
	require.NoError(t, err)
	kinsumer2, err := NewWithInterfaces(k, dynamo, streamName, *applicationName, "client_2", "", config)
	require.NoError(t, err)
	kinsumer3, err := NewWithInterfaces(k, dynamo, streamName, *applicationName, "client_3", "", config)
	require.NoError(t, err)

	desc, err := k.DescribeStream(t.Context(), &kinesis.DescribeStreamInput{
		StreamName: &streamName,
		Limit:      aws.Int32(shardLimit),
	})
	require.NoError(t, err, "Error describing stream")
	shard1 := *desc.StreamDescription.Shards[0].ShardId // Get shard IDs
	shard2 := *desc.StreamDescription.Shards[1].ShardId

	// Consume both shards
	kinsumer1.waitGroup.Add(1) // consume will mark waitgroup as done on exit, so we add to it to avoid a panic
	go kinsumer1.consume(shard1)
	kinsumer2.waitGroup.Add(1) // consume will mark waitgroup as done on exit, so we add to it to avoid a panic
	go kinsumer2.consume(shard2)

	go spamStreamModified(t, k, 2000, streamName, 0)

	resultsBeforeMerge := readMultipleToSlice([]chan *consumedRecord{kinsumer1.records, kinsumer2.records}, 5*time.Second)
	kinsumer1ResultBeforeMerge := resultsBeforeMerge[0]
	kinsumer2ResultBeforeMerge := resultsBeforeMerge[1]

	assert.Equal(t, 2000, len(kinsumer1ResultBeforeMerge)+len(kinsumer2ResultBeforeMerge))

	go func() { // Send a bunch of data for a while as the shards merge
		for i := 9000; i < 21000; i += 50 {
			spamStreamModified(t, k, 50, streamName, int64(i))
			time.Sleep(50 * time.Millisecond)
		}

	}()

	//merge the shards
	_, err = k.MergeShards(t.Context(), &kinesis.MergeShardsInput{
		StreamName:           &streamName,
		ShardToMerge:         aws.String(shard1),
		AdjacentShardToMerge: aws.String(shard2),
	})
	require.NoError(t, err, "Problem merging shards")

	require.True(t, shardCount <= shardLimit, "Too many shards")
	timeout := time.After(time.Second)
	for {
		desc, err = k.DescribeStream(t.Context(), &kinesis.DescribeStreamInput{
			StreamName: &streamName,
			Limit:      aws.Int32(shardLimit),
		})
		require.NoError(t, err, "Error describing stream")
		if desc.StreamDescription.StreamStatus == "ACTIVE" {
			break
		}
		select {
		case <-timeout:
			require.FailNow(t, "Timedout after merging shards")
		default:
			time.Sleep(*resourceChangeTimeout)
		}
	}
	newShards := desc.StreamDescription.Shards
	require.Equal(t, int64(3), int64(len(newShards)), "Wrong number of shards after merging")

	// Consume the new shard with a different client
	shard3 := *desc.StreamDescription.Shards[2].ShardId
	kinsumer3.waitGroup.Add(1) // consume will mark waitgroup as done on exit, so we add to it to avoid a panic
	go kinsumer3.consume(shard3)

	/*
		// Not necessary for the test but it's useful to add print statements if we want to understand how refreshShards() reacts to this kind of shard change.

		go func() { // mimic the behaviour of refreshShards() until we have a scenario where it would detect the change correctly
			for {
				shardCache, err := loadShardCacheFromDynamo(dynamo, kinsumer1.metadataTableName)
				if err != nil {
					fmt.Printf("error loading shard cache from dynamo: %v", err)
				}
				cachedShardIDs := shardCache.ShardIDs

				curShardIDs, err := loadShardIDsFromKinesis(k, streamName)
				if err != nil {
					fmt.Printf("error loading shard IDs from kinesis: %v", err)
				}

				checkpoints, err := loadCheckpoints(dynamo, kinsumer1.checkpointTableName)
				if err != nil {
					fmt.Printf("error loading shard IDs from dynamo: %v", err)
				}

				updatedShardIDs, _ := diffShardIDs(curShardIDs, cachedShardIDs, checkpoints)

				if len(updatedShardIDs) == 1 {
					break
				}
			}
		}()
	*/

	resultsAfterMerge := readMultipleToSlice([]chan *consumedRecord{kinsumer1.records, kinsumer2.records, kinsumer3.records}, 5*time.Second)
	kinsumer1ResultAfterMerge := resultsAfterMerge[0]
	kinsumer2ResultAfterMerge := resultsAfterMerge[1]
	kinsumer3ResultAfterMerge := resultsAfterMerge[2]

	/*
		// Leaving this here but commented out since it's useful in inspecting the behaviour when something does look off.
		investigationSlice1 := make([]string, 0)
		for _, record := range kinsumer1ResultAfterMerge {
			investigationSlice1 = append(investigationSlice1, string(record.record.Data))
		}

		investigationSlice2 := make([]string, 0)
		for _, record := range kinsumer2ResultAfterMerge {
			investigationSlice2 = append(investigationSlice2, string(record.record.Data))
		}

		fmt.Println(investigationSlice1)
		fmt.Println(investigationSlice2)
	*/

	// Check for dupes between the two old clients
	dupes := make([]string, 0)

	for _, val1 := range kinsumer1ResultAfterMerge {
		for _, val2 := range kinsumer2ResultAfterMerge {
			if string(val1.record.Data) == string(val2.record.Data) {
				dupes = append(dupes, string(val1.record.Data))
			}
		}
	}

	assert.Equal(t, 0, len(dupes))

	// Check for dupes between the old clients and the new one
	dupes2 := make([]string, 0)
	oldShardResultsCombined := append(kinsumer1ResultAfterMerge, kinsumer2ResultAfterMerge...)

	for _, val1 := range oldShardResultsCombined {
		for _, val2 := range kinsumer3ResultAfterMerge {
			if string(val1.record.Data) == string(val2.record.Data) {
				dupes = append(dupes, string(val1.record.Data))
			}
		}
	}

	assert.Equal(t, 0, len(dupes2))
}

// TestConsumerStopStart isolates the behaviour which led to duplicates in TestSplit - where a consumer stops and starts itself more than once in relatively quick succession.
// This can lead to duplicates because the stop request causes the checkpointer to exit immediately, before it has time to checkpointer.update(). The deferred call to checkpointer.release()
// in this scenario does not take into account updates that were called after the stop request returned the function.
func TestConsumerStopStart(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	streamName := "TestConsumerStopStart_stream"

	// Setup
	k, dynamo := kinesisAndDynamoInstances(t)

	defer func() {
		err := cleanupTestEnvironment(t, k, dynamo, streamName)
		require.NoError(t, err, "Problems cleaning up the test environment")
	}()

	err := setupTestEnvironment(t, k, dynamo, streamName, 1)
	require.NoError(t, err, "Problems setting up the test environment")

	// Create two kinsumer instances, but don't call run
	config := NewConfig().
		WithBufferSize(1000).
		WithShardCheckFrequency(500 * time.Millisecond).
		WithLeaderActionFrequency(500 * time.Millisecond).
		WithCommitFrequency(50 * time.Millisecond).
		WithIteratorType(types.ShardIteratorTypeTrimHorizon)

	kinsumer, err := NewWithInterfaces(k, dynamo, streamName, *applicationName, "client_1", "", config)
	require.NoError(t, err)

	desc, err := k.DescribeStream(t.Context(), &kinesis.DescribeStreamInput{
		StreamName: &streamName,
		Limit:      aws.Int32(shardLimit),
	})
	require.NoError(t, err, "Error describing stream")
	shard1 := *desc.StreamDescription.Shards[0].ShardId // Get shard IDs

	// Consume the shard
	kinsumer.waitGroup.Add(1) // consume will mark waitgroup as done on exit, so we add to it to avoid a panic
	go kinsumer.consume(shard1)
	kinsumer.stop = make(chan struct{})

	// Send in some data in small batches
	go func() {
		for i := 0; i < 20000; i += 50 {
			spamStreamModified(t, k, 50, streamName, int64(i)) // Send data while ownership is changing, to create the scenario where duplicates are legitimately possible. (They may still be unlikely)
			time.Sleep(130 * time.Millisecond)                 // Sleep for a bit between, to allow the forced ownership change to happen.
		}
	}()

	// Repeatedly stop and start the client for a while.
	go func() {
		i := 0
		for {
			i++
			kinsumer.stopConsumers()
			time.Sleep(200 * time.Millisecond)
			kinsumer.waitGroup.Add(1) // consume will mark waitgroup as done on exit, so we add to it to avoid a panic
			go kinsumer.consume(shard1)
			kinsumer.stop = make(chan struct{})

			time.Sleep(600 * time.Millisecond) // wait a bit
		}
	}()

	result := readEventsToSlice(kinsumer.records, 5*time.Second)

	// get dupes
	dupes := getDupesFromSlice(result)

	assert.Equal(t, 0, len(dupes))
	assert.Equal(t, 20000, len(result))

}

// TestMultipleConsumerStopStart tests the same thing as TestConsumerStopStart, but for the scenario where there are multiple clients vying for control of the same shard.
// This is a common scenario when shards are merged, because the reported shard count will change relatively slowly over time (seconds), and for a period different clients will report different shard counts
// The aim of this test is to give us some more robust assurance that there are no additional issues for multiple consumers on a shard which aren't caught when we only have one consumer at a time.
func TestMultipleConsumerStopStart(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	streamName := "TestMultipleConsumerStopStart_stream"

	k, dynamo := kinesisAndDynamoInstances(t)

	defer func() {
		err := cleanupTestEnvironment(t, k, dynamo, streamName)
		require.NoError(t, err, "Problems cleaning up the test environment")
	}()

	err := setupTestEnvironment(t, k, dynamo, streamName, 3)
	require.NoError(t, err, "Problems setting up the test environment")

	// Create two kinsumer instances, but don't call run
	config := NewConfig().
		WithBufferSize(1000).
		WithShardCheckFrequency(500 * time.Millisecond).
		WithLeaderActionFrequency(500 * time.Millisecond).
		WithCommitFrequency(50 * time.Millisecond).
		WithIteratorType(types.ShardIteratorTypeTrimHorizon)

	kinsumer1, err := NewWithInterfaces(k, dynamo, streamName, *applicationName, "client_1", "", config)
	require.NoError(t, err)
	kinsumer2, err := NewWithInterfaces(k, dynamo, streamName, *applicationName, "client_2", "", config)
	require.NoError(t, err)
	kinsumer3, err := NewWithInterfaces(k, dynamo, streamName, *applicationName, "client_3", "", config)
	require.NoError(t, err)

	desc, err := k.DescribeStream(t.Context(), &kinesis.DescribeStreamInput{
		StreamName: &streamName,
		Limit:      aws.Int32(shardLimit),
	})
	require.NoError(t, err, "Error describing stream")
	shard1 := *desc.StreamDescription.Shards[0].ShardId // Get shard IDs
	shard2 := *desc.StreamDescription.Shards[1].ShardId
	shard3 := *desc.StreamDescription.Shards[2].ShardId

	// Consume the other shards normally
	kinsumer3.waitGroup.Add(2)
	go kinsumer3.consume(shard2)
	go kinsumer3.consume(shard3)

	// We'll start by manually stopping and starting, with a gap in between. This is just to ensure that our first ownership change has enough time for data to arrive in both clients.

	kinsumer1.stop = make(chan struct{})
	kinsumer2.stop = make(chan struct{})

	kinsumer1.waitGroup.Add(1) // consume will mark waitgroup as done on exit, so we add to it to avoid a panic
	kinsumer2.waitGroup.Add(1) // consume will mark waitgroup as done on exit, so we add to it to avoid a panic

	go kinsumer1.consume(shard1) // first client consumes shard

	time.Sleep(300 * time.Millisecond) // wait long enough for it to have data

	go kinsumer2.consume(shard1) // second client consumes shard, shouldn't get data yet.

	go func() { // Send a bunch of data for a while
		for i := 40000; i < 60000; i += 50 {
			spamStreamModified(t, k, 50, streamName, int64(i)) // Send data while ownership is changing, to create the scenario where duplicates are legitimately possible. (They may still be unlikely)
			time.Sleep(130 * time.Millisecond)                 // Sleep for a bit between, to allow the forced ownership change to happen.
		}
	}()

	// stop and start the consumers alternately, starting with the first, leaving enough time in between to receive some data.
	// Clients should alternate between consuming data, and both return some results.
	// The test should thereby ascertain if this behaviour produces duplicates across clients beyond those produced within clients.
	go func() {
		i := 0
		for {
			i++
			if i%2 == 1 {
				kinsumer1.stopConsumers()
				time.Sleep(500 * time.Millisecond) // Wait long enough for the stop to complete, release the shard and facilitate an ownership change for the shard.
				kinsumer1.waitGroup.Add(1)         // consume will mark waitgroup as done on exit, so we add to it to avoid a panic
				go kinsumer1.consume(shard1)
				kinsumer1.stop = make(chan struct{})
			} else {
				kinsumer2.stopConsumers()
				time.Sleep(500 * time.Millisecond) // Wait long enough for the stop to complete, release the shard and facilitate an ownership change for the shard.
				kinsumer2.waitGroup.Add(1)         // consume will mark waitgroup as done on exit, so we add to it to avoid a panic
				go kinsumer2.consume(shard1)
				kinsumer2.stop = make(chan struct{})
			}
			time.Sleep(600 * time.Millisecond) // wait a bit
		}
	}()

	results := readMultipleToSlice([]chan *consumedRecord{kinsumer1.records, kinsumer2.records, kinsumer3.records}, 10*time.Second)

	kinsumer1Result := results[0]
	kinsumer2Result := results[1]
	kinsumer3Result := results[2]

	// If any of these are empty, we failed to create the scenario we're testing for, so fail the test overall.
	assert.NotEqual(t, 0, len(kinsumer1Result))
	assert.NotEqual(t, 0, len(kinsumer2Result))
	assert.NotEqual(t, 0, len(kinsumer3Result))

	// Check for dupes within each client's results
	kinsumer1Dupes := getDupesFromSlice(kinsumer1Result)
	kinsumer2Dupes := getDupesFromSlice(kinsumer2Result)
	kinsumer3Dupes := getDupesFromSlice(kinsumer3Result)

	assert.Equal(t, 0, len(kinsumer1Dupes))
	assert.Equal(t, 0, len(kinsumer2Dupes))
	assert.Equal(t, 0, len(kinsumer3Dupes))

	// Check for dupes across clients' results
	dupes2 := make([]string, 0)

	for _, val1 := range kinsumer1Result {
		for _, val2 := range kinsumer2Result {
			if string(val1.record.Data) == string(val2.record.Data) {
				dupes2 = append(dupes2, string(val1.record.Data))
			}
		}
	}

	assert.Equal(t, 0, len(dupes2))
	assert.Equal(t, 20000, len(kinsumer1Result)+len(kinsumer2Result)+len(kinsumer3Result))
}

// TestDelayedUpdateDuplicates tests the hypothesis that the above test are responsible for duplicates. It manufactures the scenario we're worried about by
// delaying the call to checkpointer.update() for the
func TestDelayedUpdateDuplicates(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	streamName := "TestDelayedUpdateDuplicates_stream"

	// Setup
	k, dynamo := kinesisAndDynamoInstances(t)

	defer func() {
		err := cleanupTestEnvironment(t, k, dynamo, streamName)
		require.NoError(t, err, "Problems cleaning up the test environment")
	}()

	err := setupTestEnvironment(t, k, dynamo, streamName, 1)
	require.NoError(t, err, "Problems setting up the test environment")

	// Create two kinsumer instances, but don't call run
	config := NewConfig().
		WithBufferSize(1000).
		WithShardCheckFrequency(500 * time.Millisecond).
		WithLeaderActionFrequency(500 * time.Millisecond).
		WithCommitFrequency(50 * time.Millisecond).
		WithIteratorType(types.ShardIteratorTypeTrimHorizon)

	kinsumer, err := NewWithInterfaces(k, dynamo, streamName, *applicationName, "client_1", "", config)
	require.NoError(t, err)

	desc, err := k.DescribeStream(t.Context(), &kinesis.DescribeStreamInput{
		StreamName: &streamName,
		Limit:      aws.Int32(shardLimit),
	})
	require.NoError(t, err, "Error describing stream")
	shard1 := *desc.StreamDescription.Shards[0].ShardId // Get shard IDs

	// Consume the shard
	kinsumer.waitGroup.Add(1) // consume will mark waitgroup as done on exit, so we add to it to avoid a panic
	go kinsumer.consume(shard1)
	kinsumer.stop = make(chan struct{})

	// Send in some data in small batches
	go func() {
		for i := 0; i < 100; i += 50 {
			spamStreamModified(t, k, 50, streamName, int64(i)) // Send data while ownership is changing, to create the scenario where duplicates are legitimately possible. (They may still be unlikely)
			time.Sleep(130 * time.Millisecond)                 // Sleep for a bit between, to allow the forced ownership change to happen.
		}
	}()

	result := make([]*consumedRecord, 0)
	delayedAcks := make([]*consumedRecord, 0)

ProcessLoop:
	for {
		select {
		case record := <-kinsumer.records:
			if v, _ := strconv.Atoi(string(record.record.Data)); v > 89 { // Instead of acking the last few, delay
				delayedAcks = append(delayedAcks, record)
			} else {
				record.checkpointer.update(aws.ToString(record.record.SequenceNumber))
			}
			result = append(result, record) // Push everything, acked or not, to the result slice
			record = nil
		case <-time.After(3 * time.Second):
			break ProcessLoop
		}
	}

	// Use a goroutine to wait for a bit (giving kinsumer the chance to stop the consumer), then ack all the remaining records
	go func() {
		for _, record := range delayedAcks {
			record.checkpointer.update(aws.ToString(record.record.SequenceNumber))
		}
	}()

	kinsumer.stopConsumers()

	delayedAcksAsStrings := make([]string, 0)
	for _, rec := range delayedAcks {
		delayedAcksAsStrings = append(delayedAcksAsStrings, string(rec.record.Data))
	}

	assert.Equal(t, 100, len(result))     // Check we got 100 records
	assert.Equal(t, 10, len(delayedAcks)) // Check we had 50 delayed acks

	time.Sleep(3 * time.Second) // wait a bit for it to calm down

	// Re-consume the shard as normal
	kinsumer.waitGroup.Add(1) // consume will mark waitgroup as done on exit, so we add to it to avoid a panic
	go kinsumer.consume(shard1)
	kinsumer.stop = make(chan struct{})

	reconsumedResult := readEventsToSlice(kinsumer.records, 3*time.Second)

	resultAsStrings := make([]string, 0)
	for _, record := range result {
		resultAsStrings = append(resultAsStrings, string(record.record.Data))
	}

	// Converting and printing stuff out for debugging purposes while I build the test
	reconsumedAsStrings := make([]string, 0)
	for _, record := range reconsumedResult {
		reconsumedAsStrings = append(reconsumedAsStrings, string(record.record.Data))
	}

}

// readEventsToSlice outputs data from a the records channel to a slice that can be handled in our tests.
func readEventsToSlice(dataChannel chan *consumedRecord, delayBeforeReturn time.Duration) []*consumedRecord {

	eventsFound := make([]*consumedRecord, 0)
ProcessLoop:
	for {
		select {
		case record := <-dataChannel: // This mimics what Kinsumer does in the Run() function
			eventsFound = append(eventsFound, record)
			record.checkpointer.update(aws.ToString(record.record.SequenceNumber))
			record = nil
		case <-time.After(delayBeforeReturn):
			break ProcessLoop
		}
	}
	return eventsFound
}

// TestReadEventsToSlice tests that readEventsToSlice can be depended upon in our tests.
func TestReadEventsToSlice(t *testing.T) {
	dataChannel := make(chan (*consumedRecord))

	go func() {
		for i := 0; i < 100; i++ {
			dataChannel <- &consumedRecord{
				record:       &types.Record{},
				checkpointer: &checkpointer{},
				retrievedAt:  time.Time{},
			}
		}
	}()

	found := readEventsToSlice(dataChannel, 1*time.Second)

	assert.Equal(t, 100, len(found))

	time.Sleep(2 * time.Second)

	found2 := readEventsToSlice(dataChannel, 1*time.Second)

	assert.Equal(t, 0, len(found2))
}

// readMultipleToSlice gets results from more than one channel concurrently
func readMultipleToSlice(kinsumerChannels []chan *consumedRecord, delayBeforeReturn time.Duration) [][]*consumedRecord {
	resultsWg := new(sync.WaitGroup)
	allResults := make([][]*consumedRecord, len(kinsumerChannels))

	for i, kinsumerChannel := range kinsumerChannels {
		resultsWg.Add(1)
		result := make([]*consumedRecord, 0)

		// get results from each channel in a goroutine so we don't block
		go func(channel chan *consumedRecord, index int) {
			result = readEventsToSlice(channel, delayBeforeReturn)
			allResults[index] = result
			resultsWg.Done()
		}(kinsumerChannel, i)
	}
	resultsWg.Wait()

	return allResults
}

// TestReadEventsToSlice tests that readEventsToSlice can be depended upon in our tests.
func TestReadMultipleToSlice(t *testing.T) {
	dataChannel1 := make(chan (*consumedRecord))
	datachannel2 := make(chan (*consumedRecord))
	datachannel3 := make(chan (*consumedRecord))

	go func() {
		for i := 0; i < 100; i++ {
			dataChannel1 <- &consumedRecord{
				record:       &types.Record{},
				checkpointer: &checkpointer{},
				retrievedAt:  time.Time{},
			}
		}
	}()

	go func() {
		for i := 0; i < 25; i++ {
			datachannel2 <- &consumedRecord{
				record:       &types.Record{},
				checkpointer: &checkpointer{},
				retrievedAt:  time.Time{},
			}
		}
	}()

	go func() {
		for i := 0; i < 350; i++ {
			datachannel3 <- &consumedRecord{
				record:       &types.Record{},
				checkpointer: &checkpointer{},
				retrievedAt:  time.Time{},
			}
		}
	}()

	allFound := readMultipleToSlice([]chan *consumedRecord{dataChannel1, datachannel2, datachannel3}, 1*time.Second)

	assert.Equal(t, 100, len(allFound[0]))
	assert.Equal(t, 25, len(allFound[1]))
	assert.Equal(t, 350, len(allFound[2]))

	time.Sleep(2 * time.Second)

	allFound2 := readMultipleToSlice([]chan *consumedRecord{dataChannel1, datachannel2, datachannel3}, 3*time.Second)

	assert.Equal(t, 0, len(allFound2[0]))
	assert.Equal(t, 0, len(allFound2[1]))
	assert.Equal(t, 0, len(allFound2[2]))
}

// spamStreamModified modifies spamStream to allow us to configure a startingNumber for the data itself to make it easier to identify different chunks of data (and therefore where duplicates come from)
func spamStreamModified(t *testing.T, k kinsumeriface.KinesisAPI, numEvents int64, streamName string, startingNumber int64) error {

	var (
		records []types.PutRecordsRequestEntry
		counter int64
	)

	for counter = startingNumber; counter < numEvents+startingNumber; counter++ {
		records = append(records, types.PutRecordsRequestEntry{
			Data:         []byte(strconv.FormatInt(counter, 10)),
			PartitionKey: aws.String(randStringBytes(10)),
		})

		if len(records) == 100 {
			pro, err := k.PutRecords(t.Context(), &kinesis.PutRecordsInput{
				StreamName: &streamName,
				Records:    records,
			})

			if err != nil {
				return fmt.Errorf("Error putting records onto stream: %s", err)
			}

			failed := aws.ToInt32(pro.FailedRecordCount)
			require.EqualValues(t, 0, failed)
			records = nil
		}
	}
	if len(records) > 0 {

		pro, err := k.PutRecords(t.Context(), &kinesis.PutRecordsInput{
			StreamName: &streamName,
			Records:    records,
		})
		if err != nil {
			return fmt.Errorf("Error putting records onto stream: %s", err)
		}

		failed := aws.ToInt32(pro.FailedRecordCount)
		require.EqualValues(t, 0, failed)
	}

	return nil
}

// Probably not the fastest implementation but we don't need speed for now.
func getDupesFromSlice(records []*consumedRecord) []string {
	entries := make(map[string]struct{})
	dupes := make([]string, 0)

	for _, val := range records {
		if _, ok := entries[string(val.record.Data)]; ok {
			dupes = append(dupes, string(val.record.Data))
		} else {
			entries[string(val.record.Data)] = struct{}{}
		}
	}
	return dupes
}

// TestGetDupesFromSlice tests that getDupesFromSlice can be depended upon in our tests.
func TestGetDupesFromSlice(t *testing.T) {
	sliceWithDupes := make([]*consumedRecord, 0)
	sliceWithoutDupes := make([]*consumedRecord, 0)

	for i := 0; i < 20; i++ {
		rec := &consumedRecord{
			record: &types.Record{
				Data: []byte(fmt.Sprint(i)),
			},
			checkpointer: &checkpointer{},
			retrievedAt:  time.Time{},
		}
		sliceWithDupes = append(sliceWithDupes, rec)
		sliceWithoutDupes = append(sliceWithoutDupes, rec)
		if i < 10 {
			sliceWithDupes = append(sliceWithDupes, rec)
		}
	}

	dupes1 := getDupesFromSlice(sliceWithDupes)
	assert.Equal(t, 10, len(dupes1))

	dupes2 := getDupesFromSlice(sliceWithoutDupes)
	assert.Equal(t, 0, len(dupes2))
}

// TestMaxConcurrentShards tests the maxConcurrentShards feature using MockKinesis
// to verify that semaphore properly limits concurrent shard record fetching
func TestMaxConcurrentShards(t *testing.T) {

	// Test setup: Create 5 mock shards with concurrency limit of 2
	shardIDs := []string{"shard-001", "shard-002", "shard-003", "shard-004", "shard-005"}
	mockKinesis := mocks.NewMockKinesis(shardIDs)

	// Create MockDynamo with proper table names (applicationName + "_" + table_type)
	mockDynamo := mocks.NewMockDynamo([]string{"test-app_checkpoints", "test-app_clients", "test-app_metadata"})

	// Pre-populate checkpoint records for each shard (unowned so they can be captured)
	for _, shardID := range shardIDs {
		checkpointItem := map[string]dbtypes.AttributeValue{
			"Shard":          &dbtypes.AttributeValueMemberS{Value: shardID},
			"SequenceNumber": &dbtypes.AttributeValueMemberS{Value: ""},  // Start from beginning
			"LastUpdate":     &dbtypes.AttributeValueMemberN{Value: "0"}, // Old timestamp = unowned
			// OwnerName and OwnerID are nil (unowned)
			// Finished is nil (active shard)
		}
		_, err := mockDynamo.PutItem(t.Context(), &dynamodb.PutItemInput{
			TableName: aws.String("test-app_checkpoints"),
			Item:      checkpointItem,
		})
		require.NoError(t, err, "Failed to populate checkpoint for shard %s", shardID)
	}

	// Set a delay to make concurrent behavior observable
	mockKinesis.SetDelay(100 * time.Millisecond)

	// Test with concurrency limit
	config := NewConfig().
		WithBufferSize(1000).
		WithShardCheckFrequency(500 * time.Millisecond).
		WithLeaderActionFrequency(500 * time.Millisecond).
		WithCommitFrequency(100 * time.Millisecond).
		WithMaxConcurrentShards(2) // Limit to 2 concurrent shards

	kinsumer, err := NewWithInterfaces(mockKinesis, mockDynamo, "test-stream", "test-app", "test-client", "", config)
	require.NoError(t, err, "Failed to create kinsumer with concurrency limit")

	// Initialize channels that consume() expects to exist
	kinsumer.stop = make(chan struct{})
	kinsumer.shardErrors = make(chan shardConsumerError, 10)
	kinsumer.records = make(chan *consumedRecord, 1000)

	// Manually start consuming each shard
	var wg sync.WaitGroup
	for _, shardID := range shardIDs {
		wg.Add(1)
		go func(shard string) {
			defer wg.Done()
			kinsumer.waitGroup.Add(1) // consume() expects this
			kinsumer.consume(shard)
		}(shardID)
	}

	// Wait a moment for consumers to start up and begin processing
	time.Sleep(50 * time.Millisecond)

	// Verify that we reach the expected concurrency level (2)
	success := mockKinesis.WaitForConcurrentCalls(2, 2*time.Second)
	assert.True(t, success, "Expected to reach 2 concurrent calls")

	// Let it run for a bit to collect data
	time.Sleep(500 * time.Millisecond)

	// Verify concurrency never exceeded the limit
	maxConcurrent := mockKinesis.GetMaxConcurrentCalls()
	assert.LessOrEqual(t, maxConcurrent, 2, "Concurrent calls should never exceed limit of 2, got %d", maxConcurrent)

	// Verify we got some calls (processing is happening)
	totalCalls := mockKinesis.GetTotalCalls()
	assert.Greater(t, totalCalls, 5, "Should have made multiple GetRecords calls, got %d", totalCalls)

	// Stop consumers
	close(kinsumer.stop)

	// Wait for all goroutines to finish
	wg.Wait()

	t.Logf("Max concurrent calls observed: %d (limit was 2)", maxConcurrent)
	t.Logf("Total GetRecords calls made: %d", totalCalls)
}

// TestMaxConcurrentShardsUnlimited tests that maxConcurrentShards=0 means unlimited
func TestMaxConcurrentShardsUnlimited(t *testing.T) {

	// Test setup: Create 5 mock shards with no concurrency limit
	shardIDs := []string{"shard-001", "shard-002", "shard-003", "shard-004", "shard-005"}
	mockKinesis := mocks.NewMockKinesis(shardIDs)

	// Create MockDynamo with proper table names (applicationName + "_" + table_type)
	mockDynamo := mocks.NewMockDynamo([]string{"test-app_checkpoints", "test-app_clients", "test-app_metadata"})

	// Pre-populate checkpoint records for each shard (unowned so they can be captured)
	for _, shardID := range shardIDs {
		checkpointItem := map[string]dbtypes.AttributeValue{
			"Shard":          &dbtypes.AttributeValueMemberS{Value: shardID},
			"SequenceNumber": &dbtypes.AttributeValueMemberS{Value: ""},  // Start from beginning
			"LastUpdate":     &dbtypes.AttributeValueMemberN{Value: "0"}, // Old timestamp = unowned
			// OwnerName and OwnerID are nil (unowned)
			// Finished is nil (active shard)
		}
		_, err := mockDynamo.PutItem(t.Context(), &dynamodb.PutItemInput{
			TableName: aws.String("test-app_checkpoints"),
			Item:      checkpointItem,
		})
		require.NoError(t, err, "Failed to populate checkpoint for shard %s", shardID)
	}

	// Set a delay to make concurrent behavior observable
	mockKinesis.SetDelay(100 * time.Millisecond)

	// Test with unlimited concurrency (default)
	config := NewConfig().
		WithBufferSize(1000).
		WithShardCheckFrequency(500 * time.Millisecond).
		WithLeaderActionFrequency(500 * time.Millisecond).
		WithCommitFrequency(100 * time.Millisecond)
		// maxConcurrentShards defaults to 0 (unlimited)

	kinsumer, err := NewWithInterfaces(mockKinesis, mockDynamo, "test-stream", "test-app", "test-client", "", config)
	require.NoError(t, err, "Failed to create kinsumer without concurrency limit")

	// Initialize channels that consume() expects to exist
	kinsumer.stop = make(chan struct{})
	kinsumer.shardErrors = make(chan shardConsumerError, 10)
	kinsumer.records = make(chan *consumedRecord, 1000)

	// Manually start consuming each shard
	var wg sync.WaitGroup
	for _, shardID := range shardIDs {
		wg.Add(1)
		go func(shard string) {
			defer wg.Done()
			kinsumer.waitGroup.Add(1) // consume() expects this
			kinsumer.consume(shard)
		}(shardID)
	}

	// Wait a moment for consumers to start up
	time.Sleep(50 * time.Millisecond)

	// With unlimited concurrency, we should be able to reach all 5 concurrent calls
	success := mockKinesis.WaitForConcurrentCalls(5, 2*time.Second)
	assert.True(t, success, "Expected to reach 5 concurrent calls with unlimited setting")

	// Let it run for a bit
	time.Sleep(300 * time.Millisecond)

	// Verify we can process all shards concurrently
	maxConcurrent := mockKinesis.GetMaxConcurrentCalls()
	assert.Equal(t, 5, maxConcurrent, "Should allow all 5 shards to process concurrently, got %d", maxConcurrent)

	// Stop consumers
	close(kinsumer.stop)

	// Wait for all goroutines to finish
	wg.Wait()

	t.Logf("Max concurrent calls observed: %d (should be 5 with unlimited)", maxConcurrent)
	t.Logf("Total GetRecords calls made: %d", mockKinesis.GetTotalCalls())
}

// TestProcessRecordsBatchSemaphoreAllPaths is a focused unit test that directly tests
// the semaphore behavior in processRecordsBatch() for all possible code paths
func TestProcessRecordsBatchSemaphoreAllPaths(t *testing.T) {

	// Configuration for checkpointer setup
	type checkpointerConfig struct {
		tableName           string
		finished            bool
		finalSequenceNumber string
	}

	// Helper to inspect semaphore state directly
	getSemaphoreUsed := func(sem chan struct{}) int {
		if sem == nil {
			return 0
		}
		return len(sem)
	}

	// Helper to create kinsumer with test-specific configuration
	createTestKinsumer := func(semaphore chan struct{}, stopChan chan struct{}, recordsChan chan *consumedRecord) *Kinsumer {
		return &Kinsumer{
			kinesis:        nil, // Will be set by caller
			shardSemaphore: semaphore,
			records:        recordsChan,
			config:         NewConfig().WithMaxConcurrentShards(1),
			metricsManager: newMetricsManager(&DefaultLogger{}),
			stop:           stopChan,
		}
	}

	// Helper to create checkpointer and ticker with test-specific configuration
	createCheckpointerAndTicker := func(config checkpointerConfig, tickerInterval time.Duration) (*checkpointer, *time.Ticker) {
		mockDynamo := mocks.NewMockDynamo([]string{"test-table"})

		checkpointer := &checkpointer{
			sequenceNumber:      "",
			shardID:             "test-shard",
			tableName:           config.tableName,
			dynamodb:            mockDynamo,
			ownerName:           "test-owner",
			ownerID:             "test-owner-id",
			stats:               &NoopStatReceiver{},
			lastUpdate:          time.Now().UnixNano(),
			finished:            config.finished,
			finalSequenceNumber: config.finalSequenceNumber,
		}

		ticker := time.NewTicker(tickerInterval)

		return checkpointer, ticker
	}

	// Test cases covering all processRecordsBatch code paths
	testCases := []struct {
		name               string
		setupMock          func(*mocks.MockKinesis)
		expectError        bool
		description        string
		stopChan           chan struct{}
		recordsChan        chan *consumedRecord
		tickerInterval     time.Duration
		checkpointerConfig checkpointerConfig
		runStopGoroutine   bool
	}{
		{
			name:               "normal_success",
			setupMock:          func(mk *mocks.MockKinesis) {}, // Default behavior - returns records
			expectError:        false,
			description:        "Normal processing should acquire and release semaphore",
			stopChan:           nil,                             // No stop channel needed
			recordsChan:        make(chan *consumedRecord, 100), // Buffered - won't block
			tickerInterval:     100 * time.Millisecond,
			checkpointerConfig: checkpointerConfig{tableName: "test-table", finished: false, finalSequenceNumber: ""},
			runStopGoroutine:   false,
		},
		{
			name: "getRecords_error",
			setupMock: func(mk *mocks.MockKinesis) {
				mk.SetGenericError("test-shard") // Will return generic error (not ExpiredIteratorException)
			},
			expectError:        true,
			description:        "GetRecords error should still release semaphore via defer",
			stopChan:           nil,                             // No stop channel needed
			recordsChan:        make(chan *consumedRecord, 100), // Buffered - won't block
			tickerInterval:     100 * time.Millisecond,
			checkpointerConfig: checkpointerConfig{tableName: "test-table", finished: false, finalSequenceNumber: ""},
			runStopGoroutine:   false,
		},
		{
			name: "no_records_returned",
			setupMock: func(mk *mocks.MockKinesis) {
				mk.SetEmptyRecords("test-shard") // Return empty records array
			},
			expectError:        false,
			description:        "No records returned should still release semaphore",
			stopChan:           nil,                             // No stop channel needed
			recordsChan:        make(chan *consumedRecord, 100), // Buffered - won't block
			tickerInterval:     100 * time.Millisecond,
			checkpointerConfig: checkpointerConfig{tableName: "test-table", finished: false, finalSequenceNumber: ""},
			runStopGoroutine:   false,
		},
		{
			name: "expired_iterator_getShardIterator_succeeds",
			setupMock: func(mk *mocks.MockKinesis) {
				mk.SetError("test-shard", true) // Return ExpiredIteratorException, but GetShardIterator will succeed
			},
			expectError:        false, // batchContinue, not error
			description:        "ExpiredIteratorException with successful getShardIterator should release semaphore",
			stopChan:           nil,                             // No stop channel needed
			recordsChan:        make(chan *consumedRecord, 100), // Buffered - won't block
			tickerInterval:     100 * time.Millisecond,
			checkpointerConfig: checkpointerConfig{tableName: "test-table", finished: false, finalSequenceNumber: ""},
			runStopGoroutine:   false,
		},
		{
			name: "expired_iterator_getShardIterator_fails",
			setupMock: func(mk *mocks.MockKinesis) {
				mk.SetBothGetRecordsAndGetShardIteratorErrors("test-shard") // Both GetRecords and GetShardIterator fail
			},
			expectError:        true, // batchError when getShardIterator fails
			description:        "ExpiredIteratorException with failed getShardIterator should release semaphore",
			stopChan:           nil,                             // No stop channel needed
			recordsChan:        make(chan *consumedRecord, 100), // Buffered - won't block
			tickerInterval:     100 * time.Millisecond,
			checkpointerConfig: checkpointerConfig{tableName: "test-table", finished: false, finalSequenceNumber: ""},
			runStopGoroutine:   false,
		},
		{
			name: "stop_signal_received",
			setupMock: func(mk *mocks.MockKinesis) {
				// Keep default behavior - return records so we enter the RecordLoop
			},
			expectError:        false, // batchBreak, not error
			description:        "Stop signal during record processing should release semaphore",
			stopChan:           make(chan struct{}),             // Need stop channel
			recordsChan:        make(chan *consumedRecord, 100), // Buffered - won't block
			tickerInterval:     100 * time.Millisecond,
			checkpointerConfig: checkpointerConfig{tableName: "test-table", finished: false, finalSequenceNumber: ""},
			runStopGoroutine:   true,
		},
		{
			name: "commit_ticker_fires_commit_fails",
			setupMock: func(mk *mocks.MockKinesis) {
				// Keep default behavior - return records so we enter the RecordLoop
			},
			expectError:        true, // batchError when commit fails
			description:        "Commit ticker fires and commit fails should release semaphore",
			stopChan:           nil,                                                                                      // No stop channel needed
			recordsChan:        make(chan *consumedRecord),                                                               // Unbuffered - will block record sending to force commit ticker
			tickerInterval:     1 * time.Millisecond,                                                                     // Fast ticker
			checkpointerConfig: checkpointerConfig{tableName: "error-trigger", finished: false, finalSequenceNumber: ""}, // Trigger error
			runStopGoroutine:   false,
		},
		{
			name: "commit_ticker_fires_commit_succeeds_finished",
			setupMock: func(mk *mocks.MockKinesis) {
				// Keep default behavior - return records so we enter the RecordLoop
			},
			expectError:        false, // batchSuccess when commit succeeds with finishCommitted=true
			description:        "Commit ticker fires and commit succeeds with finishCommitted should release semaphore",
			stopChan:           nil,                                                                                  // No stop channel needed
			recordsChan:        make(chan *consumedRecord),                                                           // Unbuffered - will block record sending to force commit ticker
			tickerInterval:     1 * time.Millisecond,                                                                 // Fast ticker
			checkpointerConfig: checkpointerConfig{tableName: "test-table", finished: true, finalSequenceNumber: ""}, // Force finishCommitted=true
			runStopGoroutine:   false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Minimal setup - only what processRecordsBatch needs
			semaphore := make(chan struct{}, 1) // Capacity of 1 for easy verification
			mockKinesis := mocks.NewMockKinesis([]string{"test-shard"})

			// Apply test-specific mock setup
			tc.setupMock(mockKinesis)

			// Create kinsumer with test-specific configuration
			kinsumer := createTestKinsumer(semaphore, tc.stopChan, tc.recordsChan)
			kinsumer.kinesis = mockKinesis // Set the mock kinesis

			// Create checkpointer and ticker with test-specific configuration
			mockCheckpointer, ticker := createCheckpointerAndTicker(tc.checkpointerConfig, tc.tickerInterval)
			defer ticker.Stop()

			lastSeq := ""
			lastSeqNum := ""

			// Verify semaphore starts empty
			before := getSemaphoreUsed(semaphore)
			assert.Equal(t, 0, before, "Semaphore should start empty")

			// For stop signal test, close the stop channel after a delay to trigger batchBreak
			if tc.runStopGoroutine && tc.stopChan != nil {
				go func() {
					time.Sleep(10 * time.Millisecond) // Small delay to let processing start
					close(tc.stopChan)
				}()
			}

			// Call processRecordsBatch directly - this is where semaphore logic lives
			iterator := "iter-test-shard-0" // Use format that MockKinesis expects
			_, result, err := kinsumer.processRecordsBatch(iterator, "test-shard", mockCheckpointer, &lastSeq, &lastSeqNum, ticker)

			// Verify semaphore is released regardless of success or error
			after := getSemaphoreUsed(semaphore)
			assert.Equal(t, 0, after, "Semaphore should be released for case: %s - %s", tc.name, tc.description)

			// Verify expected error behavior
			if tc.expectError {
				assert.Equal(t, batchError, result, "Expected batchError result for %s", tc.name)
				assert.Error(t, err, "Expected error for %s", tc.name)
			} else {
				assert.NoError(t, err, "Expected no error for %s", tc.name)
				assert.NotEqual(t, batchError, result, "Expected non-error result for %s", tc.name)
			}

			// Verify MockKinesis saw exactly one call
			totalCalls := mockKinesis.GetTotalCalls()
			assert.Equal(t, 1, totalCalls, "Expected exactly 1 GetRecords call for %s", tc.name)

			t.Logf("✓ %s: Semaphore properly released (before=%d, after=%d)", tc.description, before, after)
		})
	}
}
