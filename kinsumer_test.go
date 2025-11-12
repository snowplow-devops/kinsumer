// Copyright (c) 2016 Twitch Interactive
package kinsumer

import (
	"errors"
	"flag"
	"fmt"
	"github.com/twitchscience/kinsumer/kinsumeriface"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	ktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	awsRegion             = flag.String("region", "eu-central-1", "Region to run tests in")
	customEndpoint        = flag.String("custom_endpoint", "http://localhost:4566", "Endpoint for custom AWS test server")
	resourceChangeTimeout = flag.Duration("resource_change_timeout", 50*time.Millisecond, "Timeout between changes to the resource infrastructure")
	applicationName       = flag.String("application_name", "kinsumer_test", "Name of the application, will impact dynamo table names")
)

const (
	shardCount int32 = 10
	shardLimit int32 = 100
)

func TestNewWithInterfaces(t *testing.T) {
	cfg, err := config.LoadDefaultConfig(t.Context())
	k := kinesis.NewFromConfig(cfg)
	d := dynamodb.NewFromConfig(cfg)

	// No kinesis
	_, err = NewWithInterfaces(nil, d, "stream", "app", "client", "", NewConfig())
	assert.NotEqual(t, err, nil)

	// No dynamodb
	_, err = NewWithInterfaces(k, nil, "stream", "app", "client", "", NewConfig())
	assert.NotEqual(t, err, nil)

	// No streamName
	_, err = NewWithInterfaces(k, d, "", "app", "client", "", NewConfig())
	assert.NotEqual(t, err, nil)

	// No applicationName
	_, err = NewWithInterfaces(k, d, "stream", "", "client", "", NewConfig())
	assert.NotEqual(t, err, nil)

	// Invalid config
	_, err = NewWithInterfaces(k, d, "stream", "app", "client", "", Config{})
	assert.NotEqual(t, err, nil)

	// All ok
	kinsumer, err := NewWithInterfaces(k, d, "stream", "app", "client", "", NewConfig())
	assert.Equal(t, err, nil)
	assert.NotEqual(t, kinsumer, nil)
}

func createFreshStream(t *testing.T, k kinsumeriface.KinesisAPI, streamName string, shardCount int32) error {
	exists, err := streamExists(t, k, streamName)
	if err != nil {
		return err
	}

	if exists {
		_, err = k.DeleteStream(t.Context(), &kinesis.DeleteStreamInput{
			StreamName: &streamName,
		})
		if err != nil {
			return err
		}

		time.Sleep(*resourceChangeTimeout)
	}

	_, err = k.CreateStream(t.Context(), &kinesis.CreateStreamInput{
		ShardCount: aws.Int32(shardCount),
		StreamName: aws.String(streamName),
	})

	if err != nil {
		return err
	}
	for {
		res, err1 := k.DescribeStream(t.Context(), &kinesis.DescribeStreamInput{
			StreamName: aws.String(streamName),
		})
		if err1 != nil {
			return err1
		}

		if res.StreamDescription.StreamStatus == "ACTIVE" {
			return nil
		}
	}
}

func streamExists(t *testing.T, client kinsumeriface.KinesisAPI, streamName string) (bool, error) {
	_, err := client.DescribeStream(t.Context(), &kinesis.DescribeStreamInput{
		StreamName: aws.String(streamName),
	})
	if err != nil {
		var rnfe *ktypes.ResourceNotFoundException
		if errors.As(err, &rnfe) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// TODO: factor out the setup to a utils package for a cleaner instrumentation?
// TODO: Add tests for our customisations in this fork
func setupTestEnvironment(t *testing.T, k kinsumeriface.KinesisAPI, d kinsumeriface.DynamoDBAPI, streamName string, shardCount int32) error {
	err := createFreshStream(t, k, streamName, shardCount)
	if err != nil {
		return fmt.Errorf("Error creating fresh stream: %s", err)
	}

	testConf := NewConfig().WithDynamoWaiterDelay(*resourceChangeTimeout)
	client, clientErr := NewWithInterfaces(k, d, streamName, *applicationName, "N/A", "", testConf)
	if clientErr != nil {
		return fmt.Errorf("Error creating new Kinsumer Client: %s", clientErr)
	}

	err = client.DeleteTables()
	if err != nil {
		return fmt.Errorf("Error deleting tables: %s", err)
	}

	err = client.CreateRequiredTables()
	if err != nil {
		return fmt.Errorf("Error creating fresh tables: %s", err)
	}

	// block until all three tables have been created
	for {
		res, err1 := d.DescribeTable(t.Context(), &dynamodb.DescribeTableInput{TableName: &client.checkpointTableName})
		if err1 != nil {
			return err1
		}

		if res.Table.TableStatus == "ACTIVE" {
			break
		}
	}

	for {
		res, err1 := d.DescribeTable(t.Context(), &dynamodb.DescribeTableInput{TableName: &client.clientsTableName})
		if err1 != nil {
			return err1
		}

		if res.Table.TableStatus == "ACTIVE" {
			break
		}
	}

	for {
		res, err1 := d.DescribeTable(t.Context(), &dynamodb.DescribeTableInput{TableName: &client.metadataTableName})
		if err1 != nil {
			return err1
		}

		if res.Table.TableStatus == "ACTIVE" {
			return nil
		}
	}

}

func ignoreResourceNotFound(err error) error {
	if err != nil {
		var rnfe *ktypes.ResourceNotFoundException
		if !errors.As(err, &rnfe) {
			return err
		}
	} else {
		time.Sleep(*resourceChangeTimeout)
	}

	return nil
}

func cleanupTestEnvironment(t *testing.T, k kinsumeriface.KinesisAPI, d kinsumeriface.DynamoDBAPI, streamName string) error {
	_, err := k.DeleteStream(t.Context(), &kinesis.DeleteStreamInput{
		StreamName: &streamName,
	})

	if e := ignoreResourceNotFound(err); e != nil {
		return fmt.Errorf("Error deleting kinesis stream: %s", e)
	}

	testConf := NewConfig().WithDynamoWaiterDelay(*resourceChangeTimeout)
	client, clientErr := NewWithInterfaces(k, d, "N/A", *applicationName, "N/A", "", testConf)
	if clientErr != nil {
		return fmt.Errorf("Error creating new Kinsumer Client: %s", clientErr)
	}

	err = client.DeleteTables()
	if err != nil {
		return fmt.Errorf("Error deleting tables: %s", err)
	}
	return nil
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func spamStream(t *testing.T, k kinsumeriface.KinesisAPI, numEvents int64, streamName string) error {

	var (
		records []ktypes.PutRecordsRequestEntry
		counter int64
	)

	for counter = 0; counter < numEvents; counter++ {
		records = append(records, ktypes.PutRecordsRequestEntry{
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

func kinesisAndDynamoInstances(t *testing.T) (kinsumeriface.KinesisAPI, kinsumeriface.DynamoDBAPI) {
	cfg, err := config.LoadDefaultConfig(t.Context(),
		config.WithRegion(*awsRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("foo", "var", "")),
		config.WithBaseEndpoint(*customEndpoint),
	)
	require.NoError(t, err, "Loading config failed")

	k := kinesis.NewFromConfig(cfg)
	d := dynamodb.NewFromConfig(cfg)
	return k, d
}

func testSetup(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	k, d := kinesisAndDynamoInstances(t)

	defer func() {
		err := cleanupTestEnvironment(t, k, d, "testSetupStream")
		require.NoError(t, err, "Problems cleaning up the test environment")
	}()

	err := setupTestEnvironment(t, k, d, "testSetupStream", shardCount)
	require.NoError(t, err, "Problems setting up the test environment")

	err = spamStream(t, k, 233, "testSetupStream")
	require.NoError(t, err, "Problems spamming stream with events")

}

// This is not a real final test. It's just a harness for development and to kind of think through the interface
func TestKinsumer(t *testing.T) {
	const (
		numberOfEventsToTest = 4321
		numberOfClients      = 3
		streamName           = "TestKinsumer_stream"
	)

	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	k, d := kinesisAndDynamoInstances(t)

	defer func() {
		err := cleanupTestEnvironment(t, k, d, streamName)
		require.NoError(t, err, "Problems cleaning up the test environment")
	}()

	err := setupTestEnvironment(t, k, d, streamName, shardCount)
	require.NoError(t, err, "Problems setting up the test environment")

	clients := make([]*Kinsumer, numberOfClients)
	eventsPerClient := make([]int, numberOfClients)

	output := make(chan int, numberOfClients)
	var waitGroup sync.WaitGroup

	config := NewConfig().WithBufferSize(numberOfEventsToTest)
	config = config.WithShardCheckFrequency(500 * time.Millisecond)
	config = config.WithLeaderActionFrequency(500 * time.Millisecond)
	config = config.WithCommitFrequency(100 * time.Millisecond)

	for i := 0; i < numberOfClients; i++ {
		if i > 0 {
			time.Sleep(50 * time.Millisecond) // Add the clients slowly
		}

		clients[i], err = NewWithInterfaces(k, d, streamName, *applicationName, fmt.Sprintf("test_%d", i), "", config)
		require.NoError(t, err, "NewWithInterfaces() failed")

		err = clients[i].Run()
		require.NoError(t, err, "kinsumer.Run() failed")
		err = clients[i].Run()
		assert.Error(t, err, "second time calling kinsumer.Run() should fail")

		waitGroup.Add(1)
		go func(client *Kinsumer, ci int) {
			defer waitGroup.Done()
			for {
				data, innerError := client.Next()
				require.NoError(t, innerError, "kinsumer.Next() failed")
				if data == nil {
					return
				}
				idx, _ := strconv.Atoi(string(data))
				output <- idx
				eventsPerClient[ci]++
			}
		}(clients[i], i)
		defer func(ci int) {
			if clients[ci] != nil {
				clients[ci].Stop()
			}
		}(i)
	}

	err = spamStream(t, k, numberOfEventsToTest, streamName)
	require.NoError(t, err, "Problems spamming stream with events")

	readEvents(t, output, numberOfEventsToTest)

	for ci, client := range clients {
		client.Stop()
		clients[ci] = nil
	}

	drain(t, output)

	// Make sure the go routines have finished
	waitGroup.Wait()
}

// TestLeader is an integration test of leadership claiming and deleting old clients.
func TestLeader(t *testing.T) {
	const (
		numberOfEventsToTest = 4321
		numberOfClients      = 2
		streamName           = "TestLeader_stream"
	)

	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	k, d := kinesisAndDynamoInstances(t)

	defer func() {
		err := cleanupTestEnvironment(t, k, d, streamName)
		require.NoError(t, err, "Problems cleaning up the test environment")
	}()

	err := setupTestEnvironment(t, k, d, streamName, shardCount)
	require.NoError(t, err, "Problems setting up the test environment")

	clients := make([]*Kinsumer, numberOfClients)

	output := make(chan int, numberOfClients)
	var waitGroup sync.WaitGroup

	// Put an old client that should be deleted.
	now := time.Now().Add(-time.Hour * 24 * 7)
	item, err := attributevalue.MarshalMap(clientRecord{
		ID:            "Old",
		Name:          "Old",
		LastUpdate:    now.UnixNano(),
		LastUpdateRFC: now.UTC().Format(time.RFC1123Z),
	})
	require.NoError(t, err, "Problems converting old client")

	clientsTableName := aws.String(*applicationName + "_clients")
	_, err = d.PutItem(t.Context(), &dynamodb.PutItemInput{
		TableName: clientsTableName,
		Item:      item,
	})
	require.NoError(t, err, "Problems putting old client")

	config := NewConfig().WithBufferSize(numberOfEventsToTest)
	config = config.WithShardCheckFrequency(500 * time.Millisecond)
	config = config.WithLeaderActionFrequency(500 * time.Millisecond)
	config = config.WithCommitFrequency(100 * time.Millisecond)

	for i := 0; i < numberOfClients; i++ {
		if i > 0 {
			time.Sleep(50 * time.Millisecond) // Add the clients slowly
		}

		clients[i], err = NewWithInterfaces(k, d, streamName, *applicationName, fmt.Sprintf("test_%d", i), "", config)
		require.NoError(t, err, "NewWithInterfaces() failed")
		clients[i].clientID = strconv.Itoa(i + 1)

		err = clients[i].Run()
		require.NoError(t, err, "kinsumer.Run() failed")

		waitGroup.Add(1)
		go func(client *Kinsumer, ci int) {
			defer waitGroup.Done()
			for {
				data, innerError := client.Next()
				require.NoError(t, innerError, "kinsumer.Next() failed")
				if data == nil {
					return
				}
				idx, _ := strconv.Atoi(string(data))
				output <- idx
			}
		}(clients[i], i)
		defer func(ci int) {
			if clients[ci] != nil {
				clients[ci].Stop()
			}
		}(i)
	}

	err = spamStream(t, k, numberOfEventsToTest, streamName)
	require.NoError(t, err, "Problems spamming stream with events")
	readEvents(t, output, numberOfEventsToTest)

	resp, err := d.GetItem(t.Context(), &dynamodb.GetItemInput{
		TableName:      clientsTableName,
		ConsistentRead: aws.Bool(true),
		Key: map[string]dbtypes.AttributeValue{
			"ID": &dbtypes.AttributeValueMemberS{Value: "Old"},
		},
	})
	require.NoError(t, err, "Problem getting old client")
	require.Equal(t, 0, len(resp.Item), "Old client was not deleted")

	assert.Equal(t, true, clients[0].isLeader, "First client is not leader")
	assert.Equal(t, false, clients[1].isLeader, "Second leader is also leader")

	c, err := NewWithInterfaces(k, d, streamName, *applicationName, fmt.Sprintf("_test_%d", numberOfClients), "", config)
	require.NoError(t, err, "NewWithInterfaces() failed")
	c.clientID = "0"

	err = c.Run()
	require.NoError(t, err, "kinsumer.Run() failed")
	require.Equal(t, true, c.isLeader, "New client is not leader")
	_, err = clients[0].refreshShards()
	require.NoError(t, err, "Problem refreshing shards of original leader")

	require.Equal(t, false, clients[0].isLeader, "Original leader is still leader")

	c.Stop()

	for ci, client := range clients {
		client.Stop()
		clients[ci] = nil
	}

	drain(t, output)

	// Make sure the go routines have finished
	waitGroup.Wait()
}

// TestSplit is an integration test of merging shards, checking the closed and new shards are handled correctly.
func TestSplit(t *testing.T) {
	const (
		numberOfEventsToTest = 4321
		numberOfClients      = 3
	)
	streamName := "TestSplit_stream"

	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	k, d := kinesisAndDynamoInstances(t)

	defer func() {
		err := cleanupTestEnvironment(t, k, d, streamName)
		require.NoError(t, err, "Problems cleaning up the test environment")
	}()

	err := setupTestEnvironment(t, k, d, streamName, shardCount)
	require.NoError(t, err, "Problems setting up the test environment")

	clients := make([]*Kinsumer, numberOfClients)

	output := make(chan int, numberOfClients)
	var waitGroup sync.WaitGroup

	config := NewConfig().WithBufferSize(numberOfEventsToTest)
	config = config.WithShardCheckFrequency(500 * time.Millisecond)
	config = config.WithLeaderActionFrequency(500 * time.Millisecond)
	config = config.WithCommitFrequency(50 * time.Millisecond)

	for i := 0; i < numberOfClients; i++ {
		if i > 0 {
			time.Sleep(50 * time.Millisecond) // Add the clients slowly
		}

		clients[i], err = NewWithInterfaces(k, d, streamName, *applicationName, fmt.Sprintf("test_%d", i), "", config)
		require.NoError(t, err, "NewWithInterfaces() failed")
		clients[i].clientID = strconv.Itoa(i + 1)

		err = clients[i].Run()
		require.NoError(t, err, "kinsumer.Run() failed")

		waitGroup.Add(1)
		go func(client *Kinsumer, ci int) {
			defer waitGroup.Done()
			for {
				data, innerError := client.Next()
				require.NoError(t, innerError, "kinsumer.Next() failed")
				if data == nil {
					return
				}
				idx, _ := strconv.Atoi(string(data))
				output <- idx
			}
		}(clients[i], i)
		defer func(ci int) {
			if clients[ci] != nil {
				clients[ci].Stop()
			}
		}(i)
	}

	err = spamStream(t, k, numberOfEventsToTest, streamName)
	require.NoError(t, err, "Problems spamming stream with events")

	readEvents(t, output, numberOfEventsToTest)

	desc, err := k.DescribeStream(t.Context(), &kinesis.DescribeStreamInput{
		StreamName: &streamName,
		Limit:      aws.Int32(shardLimit),
	})
	require.NoError(t, err, "Error describing stream")
	shards := desc.StreamDescription.Shards
	shardMap := make(map[string]*ktypes.Shard)
	for _, shard := range shards {
		shardMap[*shard.ShardId] = &shard
	}

	require.True(t, len(shards) >= 2, "Fewer than 2 shards")

	_, err = k.MergeShards(t.Context(), &kinesis.MergeShardsInput{
		StreamName:           &streamName,
		ShardToMerge:         aws.String(*shards[0].ShardId),
		AdjacentShardToMerge: aws.String(*shards[1].ShardId),
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
	require.Equal(t, shardCount+1, int32(len(newShards)), "Wrong number of shards after merging")

	err = spamStream(t, k, numberOfEventsToTest, streamName)
	require.NoError(t, err, "Problems spamming stream with events")

	readEvents(t, output, numberOfEventsToTest)

	// Sleep here to wait for stuff to calm down. When running this test
	// by itself it passes without the sleep but when running all the tests
	// it fails. Since we delete all tables I suspect it's kinesalite having
	// issues.
	time.Sleep(1000 * time.Millisecond)
	// Validate finished shards are no longer in the cache
	var expectedShards []string
	for _, shard := range newShards {
		if *shard.ShardId != *shards[0].ShardId && *shard.ShardId != *shards[1].ShardId {
			expectedShards = append(expectedShards, *shard.ShardId)
		}
	}
	sort.Strings(expectedShards)
	cachedShards, err := loadShardIDsFromDynamo(d, clients[0].metadataTableName)
	require.NoError(t, err, "Error loading cached shard IDs")
	require.Equal(t, expectedShards, cachedShards, "Finished shards are still in the cache")

	for ci, client := range clients {
		client.Stop()
		clients[ci] = nil
	}

	drain(t, output)
	// Make sure the go routines have finished
	waitGroup.Wait()
}

func drain(t *testing.T, output chan int) {
	extraEvents := 0
	// Drain in case events duplicated, so we don't hang.
DrainLoop:
	for {
		select {
		case <-output:
			extraEvents++
		default:
			break DrainLoop
		}
	}
	assert.Equal(t, 0, extraEvents, "Got %d extra events afterwards", extraEvents)
}

func readEvents(t *testing.T, output chan int, numberOfEventsToTest int) {
	eventsFound := make([]bool, numberOfEventsToTest)
	total := 0

ProcessLoop:
	for {
		select {
		case idx := <-output:
			assert.Equal(t, false, eventsFound[idx], "Got duplicate event %d", idx)
			eventsFound[idx] = true
			total++
			if total == numberOfEventsToTest {
				break ProcessLoop
			}
		case <-time.After(5 * time.Second):
			break ProcessLoop
		}
	}

	t.Logf("Got all %d out of %d events\n", total, numberOfEventsToTest)
}

// TestIteratorStartTimestampCheckpoints demonstrates what happens to checkpoint records
// when using iteratorStartTimestamp - specifically showing the behavior with old closed shards
// vs new open shards
func TestIteratorStartTimestampCheckpoints(t *testing.T) {
	streamName := "TestIteratorStartTimestamp_stream"

	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	k, d := kinesisAndDynamoInstances(t)

	defer func() {
		err := cleanupTestEnvironment(t, k, d, streamName)
		require.NoError(t, err, "Problems cleaning up the test environment")
	}()

	// Setup stream with 2 shards
	err := setupTestEnvironment(t, k, d, streamName, 2)
	require.NoError(t, err, "Problems setting up the test environment")

	// Put "old" data (before our cutoff timestamp)
	t.Log("=== Putting 100 'old' records ===")
	err = spamStream(t, k, 100, streamName)
	require.NoError(t, err, "spamStream() failed")

	// Establish cutoff timestamp with clear separation
	t.Log("=== Waiting to establish timestamp cutoff ===")
	time.Sleep(2 * time.Second)
	cutoffTimestamp := time.Now()
	t.Logf("Cutoff timestamp: %s", cutoffTimestamp.Format(time.RFC3339))
	time.Sleep(2 * time.Second)

	// Get initial shards before merge
	desc, err := k.DescribeStream(t.Context(), &kinesis.DescribeStreamInput{
		StreamName: aws.String(streamName),
		Limit:      aws.Int32(shardLimit),
	})
	require.NoError(t, err, "Error describing stream")
	initialShards := desc.StreamDescription.Shards
	require.True(t, len(initialShards) >= 2, "Need at least 2 shards for merge")

	// Merge shards to create CLOSED parent shards
	t.Log("=== Merging shards to create CLOSED shards ===")
	_, err = k.MergeShards(t.Context(), &kinesis.MergeShardsInput{
		StreamName:           aws.String(streamName),
		ShardToMerge:         aws.String(*initialShards[0].ShardId),
		AdjacentShardToMerge: aws.String(*initialShards[1].ShardId),
	})
	require.NoError(t, err, "Problem merging shards")

	// Wait for merge to complete
	timeout := time.After(30 * time.Second)
	for {
		desc, err = k.DescribeStream(t.Context(), &kinesis.DescribeStreamInput{
			StreamName: aws.String(streamName),
			Limit:      aws.Int32(shardLimit),
		})
		require.NoError(t, err, "Error describing stream during merge wait")
		if desc.StreamDescription.StreamStatus == "ACTIVE" {
			break
		}
		select {
		case <-timeout:
			require.FailNow(t, "Timeout waiting for merge to complete")
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Get all shards after merge and categorize them
	allShards, err := loadShardIDsFromKinesis(k, streamName)
	require.NoError(t, err, "Error loading shard IDs")

	// Manually categorize shards as open/closed by checking EndingSequenceNumber
	openShardIDs := make([]string, 0)
	closedShardIDs := make([]string, 0)
	for _, shard := range desc.StreamDescription.Shards {
		shardID := *shard.ShardId
		if shard.SequenceNumberRange.EndingSequenceNumber == nil {
			openShardIDs = append(openShardIDs, shardID)
		} else {
			closedShardIDs = append(closedShardIDs, shardID)
		}
	}

	t.Logf("=== After merge: %d total shards ===", len(allShards))
	t.Logf("  - %d OPEN shards: %v", len(openShardIDs), openShardIDs)
	t.Logf("  - %d CLOSED shards: %v", len(closedShardIDs), closedShardIDs)

	// Put "new" data (after the cutoff timestamp)
	t.Log("=== Putting 100 'new' records ===")
	err = spamStream(t, k, 100, streamName)
	require.NoError(t, err, "spamStream() failed")

	// Create Kinsumer with iteratorStartTimestamp
	t.Log("=== Creating Kinsumer with AT_TIMESTAMP iterator ===")
	config := NewConfig().
		WithBufferSize(1000).
		WithShardCheckFrequency(500 * time.Millisecond).
		WithLeaderActionFrequency(500 * time.Millisecond).
		WithCommitFrequency(100 * time.Millisecond).
		WithIteratorStartTimestamp(&cutoffTimestamp)

	kinsumer, err := NewWithInterfaces(k, d, streamName, *applicationName, "timestamp_test_client", "", config)
	require.NoError(t, err, "NewWithInterfaces() failed")

	// Start kinsumer and let it capture shards
	err = kinsumer.Run()
	require.NoError(t, err, "kinsumer.Run() failed")

	// Consume records in background
	go func() {
		for {
			_, err := kinsumer.Next()
			if err != nil || err == nil {
				// Just consume, don't care about errors for this test
			}
		}
	}()

	// Give it time to capture shards and create checkpoints
	t.Log("=== Waiting for shard capture and checkpoint creation (3 seconds) ===")
	time.Sleep(3 * time.Second)

	// Stop kinsumer
	kinsumer.Stop()

	// Load and analyze checkpoint records
	t.Log("")
	t.Log("=================================================================")
	t.Log("=== CHECKPOINT TABLE ANALYSIS ===")
	t.Log("=================================================================")
	t.Log("")

	checkpoints, err := loadCheckpoints(d, fmt.Sprintf("%s_checkpoints", *applicationName))
	require.NoError(t, err, "loadCheckpoints() failed")

	// Get shard details from Kinesis for status
	shardDetails := make(map[string]bool) // true = CLOSED, false = OPEN
	for _, shard := range desc.StreamDescription.Shards {
		shardID := *shard.ShardId
		isClosed := shard.SequenceNumberRange.EndingSequenceNumber != nil
		shardDetails[shardID] = isClosed
	}

	// Analyze checkpoints for old CLOSED shards
	t.Log("OLD CLOSED SHARDS (existed before timestamp):")
	t.Log("---------------------------------------------")
	orphanedCount := 0
	for _, shardID := range closedShardIDs {
		cp, exists := checkpoints[shardID]
		if !exists {
			t.Logf("  %s: NO CHECKPOINT RECORD", shardID)
			continue
		}

		status := "✓ OK"
		if cp.SequenceNumber == nil && cp.Finished == nil && cp.OwnerID == nil {
			status = "⚠️  ORPHANED"
			orphanedCount++
		}

		t.Logf("  %s (CLOSED):", shardID)
		t.Logf("    SequenceNumber: %v", formatPointer(cp.SequenceNumber))
		t.Logf("    Finished: %v", formatPointer(cp.Finished))
		t.Logf("    OwnerID: %v", formatPointer(cp.OwnerID))
		t.Logf("    Status: %s", status)
		t.Log("")
	}

	// Analyze checkpoints for new OPEN shards
	t.Log("NEW OPEN SHARDS (created after timestamp):")
	t.Log("-------------------------------------------")
	for _, shardID := range openShardIDs {
		cp, exists := checkpoints[shardID]
		if !exists {
			t.Logf("  %s: NO CHECKPOINT RECORD", shardID)
			continue
		}

		status := "✓ Being processed"
		if cp.OwnerID == nil {
			status = "○ Not yet captured"
		}

		t.Logf("  %s (OPEN):", shardID)
		t.Logf("    SequenceNumber: %v", formatPointer(cp.SequenceNumber))
		t.Logf("    Finished: %v", formatPointer(cp.Finished))
		t.Logf("    OwnerID: %v", formatPointer(cp.OwnerID))
		t.Logf("    Status: %s", status)
		t.Log("")
	}

	// Summary
	t.Log("=================================================================")
	t.Log("SUMMARY:")
	t.Log("=================================================================")
	t.Logf("Total shards in stream: %d", len(allShards))
	t.Logf("  - OPEN shards: %d", len(openShardIDs))
	t.Logf("  - CLOSED shards: %d", len(closedShardIDs))
	t.Log("")
	t.Logf("Total checkpoint records: %d", len(checkpoints))
	t.Logf("Orphaned checkpoint records: %d", orphanedCount)
	t.Log("")

	if orphanedCount > 0 {
		t.Logf("⚠️  PROBLEM DEMONSTRATED: %d old closed shards have orphaned checkpoints!", orphanedCount)
		t.Log("These checkpoints have no SequenceNumber, no Finished marker, and no Owner.")
		t.Log("They will remain in the table indefinitely unless manually cleaned up.")
	} else {
		t.Log("✓ No orphaned checkpoints found")
	}
	t.Log("=================================================================")
}

// formatPointer formats a pointer value for logging
func formatPointer(ptr interface{}) string {
	switch v := ptr.(type) {
	case *string:
		if v == nil {
			return "<nil>"
		}
		return fmt.Sprintf("\"%s\"", *v)
	case *int64:
		if v == nil {
			return "<nil>"
		}
		return fmt.Sprintf("%d", *v)
	default:
		return fmt.Sprintf("%v", ptr)
	}
}
