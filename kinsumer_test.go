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
