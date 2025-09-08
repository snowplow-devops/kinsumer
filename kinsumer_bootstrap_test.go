package kinsumer

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	ktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateBootstrapCheckpoints tests the createBootstrapCheckpoints function
// with a simple happy path scenario using real DynamoDB operations
func TestCreateBootstrapCheckpoints(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	streamName := "TestCreateBootstrapCheckpoints_stream"

	k, d := kinesisAndDynamoInstances(t)

	defer func() {
		err := cleanupTestEnvironment(t, k, d, streamName)
		require.NoError(t, err, "Problems cleaning up the test environment")
	}()

	// Setup stream and DynamoDB tables
	err := setupTestEnvironment(t, k, d, streamName, 2)
	require.NoError(t, err, "Problems setting up the test environment")

	// Create kinsumer instance to access the createBootstrapCheckpoints method
	config := NewConfig().WithIteratorType(ktypes.ShardIteratorTypeLatest)
	kinsumer, err := NewWithInterfaces(k, d, streamName, *applicationName, "test_client", "", config)
	require.NoError(t, err, "Error creating kinsumer instance")

	// Perform merge to create CLOSED shards
	desc, err := k.DescribeStream(t.Context(), &kinesis.DescribeStreamInput{
		StreamName: &streamName,
		Limit:      aws.Int32(shardLimit),
	})
	require.NoError(t, err, "Error describing stream")
	initialShards := desc.StreamDescription.Shards
	require.True(t, len(initialShards) >= 2, "Need at least 2 shards for merge")

	_, err = k.MergeShards(t.Context(), &kinesis.MergeShardsInput{
		StreamName:           &streamName,
		ShardToMerge:         aws.String(*initialShards[0].ShardId),
		AdjacentShardToMerge: aws.String(*initialShards[1].ShardId),
	})
	require.NoError(t, err, "Problem merging shards")

	// Wait for merge to complete
	timeout := time.After(30 * time.Second)
	for {
		desc, err = k.DescribeStream(t.Context(), &kinesis.DescribeStreamInput{
			StreamName: &streamName,
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

	// Get the current shards after merge (should have OPEN and CLOSED shards)
	allShardIDs, openShardIDs, closedShardIDs, err := loadShardIDsFromKinesis(k, streamName)
	require.NoError(t, err, "Error loading shard IDs from Kinesis")
	require.Greater(t, len(allShardIDs), 0, "Should have at least one shard")
	require.Greater(t, len(closedShardIDs), 0, "Should have at least one CLOSED shard after merge")
	require.Greater(t, len(openShardIDs), 0, "Should have at least one OPEN shard after merge")

	t.Logf("Found %d total shards: %d OPEN, %d CLOSED", len(allShardIDs), len(openShardIDs), len(closedShardIDs))

	// Test the createBootstrapCheckpoints function
	err = kinsumer.createBootstrapCheckpoints(openShardIDs, closedShardIDs)
	require.NoError(t, err, "createBootstrapCheckpoints failed")

	// Verify the checkpoint records were created correctly
	checkpoints, err := loadCheckpoints(d, kinsumer.checkpointTableName)
	require.NoError(t, err, "Error loading checkpoints from DynamoDB")

	// Verify all shards have checkpoint records
	assert.Equal(t, len(allShardIDs), len(checkpoints), "Should have checkpoint record for every shard")

	// Verify OPEN shards have "LATEST" sequence numbers and no Finished timestamp
	for _, shardID := range openShardIDs {
		checkpoint, exists := checkpoints[shardID]
		require.True(t, exists, "OPEN shard %s should have checkpoint record", shardID)
		require.NotNil(t, checkpoint.SequenceNumber, "OPEN shard %s should have sequence number", shardID)
		assert.Equal(t, "LATEST", *checkpoint.SequenceNumber, "OPEN shard %s should have LATEST sequence number", shardID)
		assert.Nil(t, checkpoint.Finished, "OPEN shard %s should not be marked as finished", shardID)
	}

	// Verify CLOSED shards have no sequence number and are marked as finished
	for _, shardID := range closedShardIDs {
		checkpoint, exists := checkpoints[shardID]
		require.True(t, exists, "CLOSED shard %s should have checkpoint record", shardID)
		assert.Nil(t, checkpoint.SequenceNumber, "CLOSED shard %s should have nil sequence number", shardID)
		assert.NotNil(t, checkpoint.Finished, "CLOSED shard %s should be marked as finished", shardID)
	}

	t.Logf("✓ Successfully verified %d OPEN shards with LATEST checkpoints", len(openShardIDs))
	t.Logf("✓ Successfully verified %d CLOSED shards with Finished checkpoints", len(closedShardIDs))
}



