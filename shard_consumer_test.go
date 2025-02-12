package kinsumer

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestShardConsumer aims to isolate the basic behaviour of shard consumer, without needing to run the whole program.
func TestShardConsumer(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	streamName := "TestShardConsumer_stream"

	k, dynamo := kinesisAndDynamoInstances()

	defer func() {
		err := cleanupTestEnvironment(t, k, dynamo, streamName)
		require.NoError(t, err, "Problems cleaning up the test environment")
	}()

	err := setupTestEnvironment(t, k, dynamo, streamName, 1)
	require.NoError(t, err, "Problems setting up the test environment")

	config := NewConfig().WithBufferSize(1000)
	config = config.WithShardCheckFrequency(500 * time.Millisecond)
	config = config.WithLeaderActionFrequency(500 * time.Millisecond)

	kinsumer1, err := NewWithInterfaces(k, dynamo, streamName, *applicationName, "client_1", "", config)

	desc, err := k.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: &streamName,
		Limit:      aws.Int64(shardLimit),
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

	k, dynamo := kinesisAndDynamoInstances()

	defer func() {
		err := cleanupTestEnvironment(t, k, dynamo, streamName)
		require.NoError(t, err, "Problems cleaning up the test environment")
	}()

	err := setupTestEnvironment(t, k, dynamo, streamName, 1)
	require.NoError(t, err, "Problems setting up the test environment")

	// Create two kinsumer instances, but don't call run
	config := NewConfig().WithBufferSize(1000)
	config = config.WithShardCheckFrequency(500 * time.Millisecond)
	config = config.WithLeaderActionFrequency(500 * time.Millisecond)
	config = config.WithCommitFrequency(100 * time.Millisecond)

	// Set vastly different max ages to synthetically create a forced ownership change
	maxAge1 := 10 * time.Second
	config1 := config.WithClientRecordMaxAge(&maxAge1)

	maxAge2 := 500 * time.Millisecond
	config2 := config.WithClientRecordMaxAge(&maxAge2)

	kinsumer1, err1 := NewWithInterfaces(k, dynamo, streamName, *applicationName, "client_1", "", config1)
	kinsumer2, err2 := NewWithInterfaces(k, dynamo, streamName, *applicationName, "client_2", "", config2)
	require.NoError(t, err1)
	require.NoError(t, err2)

	desc, err := k.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: &streamName,
		Limit:      aws.Int64(shardLimit),
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
			k2record.checkpointer.update(aws.StringValue(k2record.record.SequenceNumber))
			// because this may be called with no genuine record to k1, we use the k2 sequence number.
			// this shouldn't make a difference since this commit will fail.
			lastK1Record.checkpointer.update(aws.StringValue(k2record.record.SequenceNumber)) // Ack the last k1 record we have, to instigate behaviour we would see for that client
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

	k, dynamo := kinesisAndDynamoInstances()

	defer func() {
		err := cleanupTestEnvironment(t, k, dynamo, streamName)
		require.NoError(t, err, "Problems cleaning up the test environment")
	}()

	err := setupTestEnvironment(t, k, dynamo, streamName, 1)
	require.NoError(t, err, "Problems setting up the test environment")

	// Create two kinsumer instances, but don't call run
	config := NewConfig().WithBufferSize(1000)
	config = config.WithShardCheckFrequency(500 * time.Millisecond)
	config = config.WithLeaderActionFrequency(500 * time.Millisecond)
	config = config.WithCommitFrequency(100 * time.Millisecond)

	// Set vastly different max ages to synthetically create a forced ownership change
	maxAge1 := 10 * time.Second
	config1 := config.WithClientRecordMaxAge(&maxAge1)

	maxAge2 := 500 * time.Millisecond
	config2 := config.WithClientRecordMaxAge(&maxAge2)

	kinsumer1, err := NewWithInterfaces(k, dynamo, streamName, *applicationName, "client_1", "", config1)
	kinsumer2, err := NewWithInterfaces(k, dynamo, streamName, *applicationName, "client_2", "", config2)

	desc, err := k.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: &streamName,
		Limit:      aws.Int64(shardLimit),
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

	k, dynamo := kinesisAndDynamoInstances()

	defer func() {
		err := cleanupTestEnvironment(t, k, dynamo, streamName)
		require.NoError(t, err, "Problems cleaning up the test environment")
	}()

	err := setupTestEnvironment(t, k, dynamo, streamName, 2)
	require.NoError(t, err, "Problems setting up the test environment")

	// Create two kinsumer instances, but don't call run
	config := NewConfig().WithBufferSize(1000)
	config = config.WithShardCheckFrequency(500 * time.Millisecond)
	config = config.WithLeaderActionFrequency(500 * time.Millisecond)
	config = config.WithCommitFrequency(100 * time.Millisecond)

	kinsumer1, err := NewWithInterfaces(k, dynamo, streamName, *applicationName, "client_1", "", config)
	kinsumer2, err := NewWithInterfaces(k, dynamo, streamName, *applicationName, "client_2", "", config)
	kinsumer3, err := NewWithInterfaces(k, dynamo, streamName, *applicationName, "client_3", "", config)

	desc, err := k.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: &streamName,
		Limit:      aws.Int64(shardLimit),
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
	_, err = k.MergeShards(&kinesis.MergeShardsInput{
		StreamName:           &streamName,
		ShardToMerge:         aws.String(shard1),
		AdjacentShardToMerge: aws.String(shard2),
	})
	require.NoError(t, err, "Problem merging shards")

	require.True(t, shardCount <= shardLimit, "Too many shards")
	timeout := time.After(time.Second)
	for {
		desc, err = k.DescribeStream(&kinesis.DescribeStreamInput{
			StreamName: &streamName,
			Limit:      aws.Int64(shardLimit),
		})
		require.NoError(t, err, "Error describing stream")
		if *desc.StreamDescription.StreamStatus == "ACTIVE" {
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
	k, dynamo := kinesisAndDynamoInstances()

	defer func() {
		err := cleanupTestEnvironment(t, k, dynamo, streamName)
		require.NoError(t, err, "Problems cleaning up the test environment")
	}()

	err := setupTestEnvironment(t, k, dynamo, streamName, 1)
	require.NoError(t, err, "Problems setting up the test environment")

	// Create two kinsumer instances, but don't call run
	config := NewConfig().WithBufferSize(1000)
	config = config.WithShardCheckFrequency(500 * time.Millisecond)
	config = config.WithLeaderActionFrequency(500 * time.Millisecond)
	config = config.WithCommitFrequency(50 * time.Millisecond)

	kinsumer, err := NewWithInterfaces(k, dynamo, streamName, *applicationName, "client_1", "", config)

	desc, err := k.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: &streamName,
		Limit:      aws.Int64(shardLimit),
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

	k, dynamo := kinesisAndDynamoInstances()

	defer func() {
		err := cleanupTestEnvironment(t, k, dynamo, streamName)
		require.NoError(t, err, "Problems cleaning up the test environment")
	}()

	err := setupTestEnvironment(t, k, dynamo, streamName, 3)
	require.NoError(t, err, "Problems setting up the test environment")

	// Create two kinsumer instances, but don't call run
	config := NewConfig().WithBufferSize(1000)
	config = config.WithShardCheckFrequency(500 * time.Millisecond)
	config = config.WithLeaderActionFrequency(500 * time.Millisecond)
	config = config.WithCommitFrequency(50 * time.Millisecond)

	kinsumer1, err := NewWithInterfaces(k, dynamo, streamName, *applicationName, "client_1", "", config)
	kinsumer2, err := NewWithInterfaces(k, dynamo, streamName, *applicationName, "client_2", "", config)
	kinsumer3, err := NewWithInterfaces(k, dynamo, streamName, *applicationName, "client_3", "", config)

	desc, err := k.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: &streamName,
		Limit:      aws.Int64(shardLimit),
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
	k, dynamo := kinesisAndDynamoInstances()

	defer func() {
		err := cleanupTestEnvironment(t, k, dynamo, streamName)
		require.NoError(t, err, "Problems cleaning up the test environment")
	}()

	err := setupTestEnvironment(t, k, dynamo, streamName, 1)
	require.NoError(t, err, "Problems setting up the test environment")

	// Create two kinsumer instances, but don't call run
	config := NewConfig().WithBufferSize(1000)
	config = config.WithShardCheckFrequency(500 * time.Millisecond)
	config = config.WithLeaderActionFrequency(500 * time.Millisecond)
	config = config.WithCommitFrequency(50 * time.Millisecond)

	kinsumer, err := NewWithInterfaces(k, dynamo, streamName, *applicationName, "client_1", "", config)

	desc, err := k.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: &streamName,
		Limit:      aws.Int64(shardLimit),
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
				record.checkpointer.update(aws.StringValue(record.record.SequenceNumber))
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
			record.checkpointer.update(aws.StringValue(record.record.SequenceNumber))
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
			record.checkpointer.update(aws.StringValue(record.record.SequenceNumber))
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
				record:       &kinesis.Record{},
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
				record:       &kinesis.Record{},
				checkpointer: &checkpointer{},
				retrievedAt:  time.Time{},
			}
		}
	}()

	go func() {
		for i := 0; i < 25; i++ {
			datachannel2 <- &consumedRecord{
				record:       &kinesis.Record{},
				checkpointer: &checkpointer{},
				retrievedAt:  time.Time{},
			}
		}
	}()

	go func() {
		for i := 0; i < 350; i++ {
			datachannel3 <- &consumedRecord{
				record:       &kinesis.Record{},
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
func spamStreamModified(t *testing.T, k kinesisiface.KinesisAPI, numEvents int64, streamName string, startingNumber int64) error {

	var (
		records []*kinesis.PutRecordsRequestEntry
		counter int64
	)

	for counter = startingNumber; counter < numEvents+startingNumber; counter++ {
		records = append(records, &kinesis.PutRecordsRequestEntry{
			Data:         []byte(strconv.FormatInt(counter, 10)),
			PartitionKey: aws.String(randStringBytes(10)),
		})

		if len(records) == 100 {
			pro, err := k.PutRecords(&kinesis.PutRecordsInput{
				StreamName: &streamName,
				Records:    records,
			})

			if err != nil {
				return fmt.Errorf("Error putting records onto stream: %s", err)
			}

			failed := aws.Int64Value(pro.FailedRecordCount)
			require.EqualValues(t, 0, failed)
			records = nil
		}
	}
	if len(records) > 0 {

		pro, err := k.PutRecords(&kinesis.PutRecordsInput{
			StreamName: &streamName,
			Records:    records,
		})
		if err != nil {
			return fmt.Errorf("Error putting records onto stream: %s", err)
		}

		failed := aws.Int64Value(pro.FailedRecordCount)
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
			record: &kinesis.Record{
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
