// Copyright (c) 2016 Twitch Interactive
package kinsumer

import (
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	awsRegion             = flag.String("region", "us-west-2", "Region to run tests in")
	dynamoEndpoint        = flag.String("dynamo_endpoint", "http://localhost:4566", "Endpoint for dynamo test server")
	kinesisEndpoint       = flag.String("kinesis_endpoint", "http://localhost:4568", "Endpoint for kinesis test server")
	resourceChangeTimeout = flag.Duration("resource_change_timeout", 50*time.Millisecond, "Timeout between changes to the resource infrastructure")
	// streamName            = flag.String("stream_name", "kinsumer_test", "Name of kinesis stream to use for tests")
	applicationName = flag.String("application_name", "kinsumer_test", "Name of the application, will impact dynamo table names")
)

const (
	shardCount int64 = 10
	shardLimit int64 = 100
)

func testNewWithInterfaces(t *testing.T) {
	s := session.Must(session.NewSession())
	k := kinesis.New(s)
	d := dynamodb.New(s)

	// No kinesis
	_, err := NewWithInterfaces(nil, d, "stream", "app", "client", NewConfig())
	assert.NotEqual(t, err, nil)

	// No dynamodb
	_, err = NewWithInterfaces(k, nil, "stream", "app", "client", NewConfig())
	assert.NotEqual(t, err, nil)

	// No streamName
	_, err = NewWithInterfaces(k, d, "", "app", "client", NewConfig())
	assert.NotEqual(t, err, nil)

	// No applicationName
	_, err = NewWithInterfaces(k, d, "stream", "", "client", NewConfig())
	assert.NotEqual(t, err, nil)

	// Invalid config
	_, err = NewWithInterfaces(k, d, "stream", "app", "client", Config{})
	assert.NotEqual(t, err, nil)

	// All ok
	kinsumer, err := NewWithInterfaces(k, d, "stream", "app", "client", NewConfig())
	assert.Equal(t, err, nil)
	assert.NotEqual(t, kinsumer, nil)
}

func createFreshStream(t *testing.T, k kinesisiface.KinesisAPI, streamName string) error {
	_, err := k.DeleteStream(&kinesis.DeleteStreamInput{
		StreamName: &streamName,
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() != "ResourceNotFoundException" {
				return err
			}
		}
	} else {
		// Wait for the stream to be deleted
		time.Sleep(*resourceChangeTimeout)
	}

	_, err = k.CreateStream(&kinesis.CreateStreamInput{
		ShardCount: aws.Int64(shardCount),
		StreamName: &streamName,
	})

	if err != nil {
		return err
	}
	for {
		res, err1 := k.DescribeStream(&kinesis.DescribeStreamInput{
			StreamName: aws.String(streamName),
		})
		if err1 != nil {
			return err1
		}

		if *res.StreamDescription.StreamStatus == "ACTIVE" {
			return nil
		}
	}
}

// TODO: Figure out if we also need to fix each DDB table to a different name also (via application name should work.)...
// TODO: Also look into running it all sequentially, since timings may be impacted by parallelism.
// TODO: factor out the setup for a cleaner instrumentation.
// TODO: Add tests for our customisations in this fork
func setupTestEnvironment(t *testing.T, k kinesisiface.KinesisAPI, d dynamodbiface.DynamoDBAPI, streamName string) error {
	err := createFreshStream(t, k, streamName)
	if err != nil {
		return fmt.Errorf("Error creating fresh stream: %s", err)
	}

	testConf := NewConfig().WithDynamoWaiterDelay(*resourceChangeTimeout)
	client, clientErr := NewWithInterfaces(k, d, streamName, *applicationName, "N/A", testConf)
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
		res, err1 := d.DescribeTable(&dynamodb.DescribeTableInput{TableName: &client.checkpointTableName})
		if err1 != nil {
			return err1
		}

		if *res.Table.TableStatus == "ACTIVE" {
			break
		}
	}

	for {
		res, err1 := d.DescribeTable(&dynamodb.DescribeTableInput{TableName: &client.clientsTableName})
		if err1 != nil {
			return err1
		}

		if *res.Table.TableStatus == "ACTIVE" {
			break
		}
	}

	for {
		res, err1 := d.DescribeTable(&dynamodb.DescribeTableInput{TableName: &client.metadataTableName})
		if err1 != nil {
			return err1
		}

		if *res.Table.TableStatus == "ACTIVE" {
			return nil
		}
	}

}

func ignoreResourceNotFound(err error) error {
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() != "ResourceNotFoundException" {
				return err
			}
		}
	} else {
		time.Sleep(*resourceChangeTimeout)
	}

	return nil
}

func cleanupTestEnvironment(t *testing.T, k kinesisiface.KinesisAPI, d dynamodbiface.DynamoDBAPI, streamName string) error {
	_, err := k.DeleteStream(&kinesis.DeleteStreamInput{
		StreamName: &streamName,
	})

	if e := ignoreResourceNotFound(err); e != nil {
		return fmt.Errorf("Error deleting kinesis stream: %s", e)
	}

	testConf := NewConfig().WithDynamoWaiterDelay(*resourceChangeTimeout)
	client, clientErr := NewWithInterfaces(k, d, "N/A", *applicationName, "N/A", testConf)
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

func spamStream(t *testing.T, k kinesisiface.KinesisAPI, numEvents int64, streamName string) error {

	var (
		records []*kinesis.PutRecordsRequestEntry
		counter int64
	)

	for counter = 0; counter < numEvents; counter++ {
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

func kinesisAndDynamoInstances() (kinesisiface.KinesisAPI, dynamodbiface.DynamoDBAPI) {
	kc := aws.NewConfig().WithRegion(*awsRegion).WithCredentials(credentials.NewStaticCredentials("foo", "var", "")).WithLogLevel(3)
	if len(*kinesisEndpoint) > 0 {
		kc = kc.WithEndpoint(*kinesisEndpoint)
	}

	dc := aws.NewConfig().WithRegion(*awsRegion).WithCredentials(credentials.NewStaticCredentials("foo", "var", "")).WithLogLevel(3)
	if len(*dynamoEndpoint) > 0 {
		dc = dc.WithEndpoint(*dynamoEndpoint)
	}

	k := kinesis.New(session.Must(session.NewSession(kc)))
	d := dynamodb.New(session.Must(session.NewSession(dc)))

	return k, d
}

func testSetup(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	k, d := kinesisAndDynamoInstances()

	defer func() {
		err := cleanupTestEnvironment(t, k, d, "testSetupStream")
		require.NoError(t, err, "Problems cleaning up the test environment")
	}()

	err := setupTestEnvironment(t, k, d, "testSetupStream")
	require.NoError(t, err, "Problems setting up the test environment")

	err = spamStream(t, k, 233, "testSetupStream")
	require.NoError(t, err, "Problems spamming stream with events")

}

// This is not a real final test. It's just a harness for development and to kind of think through the interface
func testKinsumer(t *testing.T) {
	const (
		numberOfEventsToTest = 4321
		numberOfClients      = 3
		streamName           = "TestKinsumer_stream"
	)

	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	k, d := kinesisAndDynamoInstances()

	defer func() {
		err := cleanupTestEnvironment(t, k, d, streamName)
		require.NoError(t, err, "Problems cleaning up the test environment")
	}()

	err := setupTestEnvironment(t, k, d, streamName)
	require.NoError(t, err, "Problems setting up the test environment")

	clients := make([]*Kinsumer, numberOfClients)
	eventsPerClient := make([]int, numberOfClients)

	output := make(chan int, numberOfClients)
	var waitGroup sync.WaitGroup

	config := NewConfig().WithBufferSize(numberOfEventsToTest)
	config = config.WithShardCheckFrequency(500 * time.Millisecond) // Low values here invoke our race conditions. TODO: Test fixes at various values.
	config = config.WithLeaderActionFrequency(500 * time.Millisecond)

	for i := 0; i < numberOfClients; i++ {
		if i > 0 {
			time.Sleep(50 * time.Millisecond) // Add the clients slowly
		}

		clients[i], err = NewWithInterfaces(k, d, streamName, *applicationName, fmt.Sprintf("test_%d", i), config)
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

// testLeader is an integration test of leadership claiming and deleting old clients.
func testLeader(t *testing.T) {
	const ( // TODO: Revert these values
		numberOfEventsToTest = 4321 // Upped to 9321 to increase chances of invoking issues with duplicates/races.
		numberOfClients      = 2
		streamName           = "TestLeader_stream"
	)

	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	k, d := kinesisAndDynamoInstances()

	defer func() {
		err := cleanupTestEnvironment(t, k, d, streamName)
		require.NoError(t, err, "Problems cleaning up the test environment")
	}()

	err := setupTestEnvironment(t, k, d, streamName)
	require.NoError(t, err, "Problems setting up the test environment")

	clients := make([]*Kinsumer, numberOfClients)

	// TODO: test whether buffer size actually has an effect on speed.
	output := make(chan int, numberOfEventsToTest) // Changed from numberOfClients to numberOfEventsToTest to see if that resolves the throttling issue
	var waitGroup sync.WaitGroup

	// Put an old client that should be deleted.
	now := time.Now().Add(-time.Hour * 24 * 7)
	item, err := dynamodbattribute.MarshalMap(clientRecord{
		ID:            "Old",
		Name:          "Old",
		LastUpdate:    now.UnixNano(),
		LastUpdateRFC: now.UTC().Format(time.RFC1123Z),
	})
	require.NoError(t, err, "Problems converting old client")

	clientsTableName := aws.String(*applicationName + "_clients")
	_, err = d.PutItem(&dynamodb.PutItemInput{
		TableName: clientsTableName,
		Item:      item,
	})
	require.NoError(t, err, "Problems putting old client")

	config := NewConfig().WithBufferSize(numberOfEventsToTest)
	config = config.WithShardCheckFrequency(500 * time.Millisecond)
	config = config.WithLeaderActionFrequency(500 * time.Millisecond)
	// TODO: Test both with and without this.
	//maxAge := 5000 * time.Millisecond
	//config = config.WithClientRecordMaxAge(&maxAge)

	for i := 0; i < numberOfClients; i++ {
		if i > 0 {
			time.Sleep(50 * time.Millisecond) // Add the clients slowly
		}

		clients[i], err = NewWithInterfaces(k, d, streamName, *applicationName, fmt.Sprintf("test_%d", i), config)
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

	resp, err := d.GetItem(&dynamodb.GetItemInput{
		TableName:      clientsTableName,
		ConsistentRead: aws.Bool(true),
		Key: map[string]*dynamodb.AttributeValue{
			"ID": {S: aws.String("Old")},
		},
	})
	require.NoError(t, err, "Problem getting old client")
	require.Equal(t, 0, len(resp.Item), "Old client was not deleted")

	assert.Equal(t, true, clients[0].isLeader, "First client is not leader")
	assert.Equal(t, false, clients[1].isLeader, "Second leader is also leader")

	c, err := NewWithInterfaces(k, d, streamName, *applicationName, fmt.Sprintf("_test_%d", numberOfClients), config)
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

// testSplit is an integration test of merging shards, checking the closed and new shards are handled correctly.
func testSplit(t *testing.T) {
	const (
		numberOfEventsToTest = 4321
		numberOfClients      = 3
	)
	streamName := "TestSplit_stream"

	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	k, d := kinesisAndDynamoInstances()

	defer func() {
		err := cleanupTestEnvironment(t, k, d, streamName)
		require.NoError(t, err, "Problems cleaning up the test environment")
	}()

	err := setupTestEnvironment(t, k, d, streamName)
	require.NoError(t, err, "Problems setting up the test environment")

	clients := make([]*Kinsumer, numberOfClients)

	output := make(chan int, numberOfClients)
	var waitGroup sync.WaitGroup

	config := NewConfig().WithBufferSize(numberOfEventsToTest)
	config = config.WithShardCheckFrequency(5000 * time.Millisecond) // Changing from 500 -> 5000 temporarily to avoid race condition
	config = config.WithLeaderActionFrequency(5000 * time.Millisecond)
	config = config.WithCommitFrequency(50 * time.Millisecond)

	for i := 0; i < numberOfClients; i++ {
		if i > 0 {
			time.Sleep(50 * time.Millisecond) // Add the clients slowly
		}

		clients[i], err = NewWithInterfaces(k, d, streamName, *applicationName, fmt.Sprintf("test_%d", i), config)
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

	desc, err := k.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: &streamName,
		Limit:      aws.Int64(shardLimit),
	})
	require.NoError(t, err, "Error describing stream")
	shards := desc.StreamDescription.Shards
	shardMap := make(map[string]*kinesis.Shard)
	for _, shard := range shards {
		shardMap[*shard.ShardId] = shard
	}

	require.True(t, len(shards) >= 2, "Fewer than 2 shards")

	_, err = k.MergeShards(&kinesis.MergeShardsInput{
		StreamName:           &streamName,
		ShardToMerge:         aws.String(*shards[0].ShardId),
		AdjacentShardToMerge: aws.String(*shards[1].ShardId),
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
	require.Equal(t, shardCount+1, int64(len(newShards)), "Wrong number of shards after merging")

	err = spamStream(t, k, numberOfEventsToTest, streamName)
	require.NoError(t, err, "Problems spamming stream with events")

	readEvents(t, output, numberOfEventsToTest)

	// Sleep here to wait for stuff to calm down. When running this test
	// by itself it passes without the sleep but when running all the tests
	// it fails. Since we delete all tables I suspect it's kinesalite having
	// issues.
	time.Sleep(500 * time.Millisecond)
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

// TODO: Cleanup all the print statements.
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
		case <-time.After(3 * time.Second):
			break ProcessLoop
		}
	}

	t.Logf("Got all %d out of %d events\n", total, numberOfEventsToTest)
}

func TestAllInSequence(t *testing.T) {
	t.Run("testNewWithInterfaces", testNewWithInterfaces)
	t.Run("testKinsumer", func(t *testing.T) { t.Run("testKinsumer", testKinsumer) })
}

func TestAllInSequence2(t *testing.T) {
	t.Run("testLeader", func(t *testing.T) { t.Run("testLeader", testLeader) })
}

func TestAllInSequence3(t *testing.T) {
	t.Run("testSplit", func(t *testing.T) { t.Run("testSplit", testSplit) })
}
