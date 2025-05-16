// Copyright (c) 2016 Twitch Interactive

package kinsumer

import (
	"context"
	"errors"
	"fmt"
	"github.com/twitchscience/kinsumer/kinsumeriface"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	ktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

const (
	leaderKey       = "Leader"
	shardCacheKey   = "ShardCache"
	conditionalFail = "ConditionalCheckFailedException"
)

type shardCacheRecord struct {
	Key        string   // must be "ShardCache"
	ShardIDs   []string // Slice of unfinished shard IDs
	LastUpdate int64    // timestamp of last update

	// Debug versions of LastUpdate
	LastUpdateRFC string
}

// becomeLeader starts the leadership goroutine with a channel to stop it.
// TODO(dwe): Factor out dependencies and unit test
func (k *Kinsumer) becomeLeader() {
	if k.isLeader {
		return
	}
	k.leaderLost = make(chan bool)
	k.leaderWG.Add(1)
	go func() {
		defer k.leaderWG.Done()
		leaderActions := time.NewTicker(k.config.leaderActionFrequency)
		defer func() {
			leaderActions.Stop()
			err := k.deregisterLeadership()
			if err != nil {
				k.errors <- fmt.Errorf("error deregistering leadership: %v", err)
			}
		}()
		ok, err := k.registerLeadership()
		if err != nil {
			k.errors <- fmt.Errorf("error registering initial leadership: %v", err)
		}
		// Perform leadership actions immediately if we became leader. If we didn't
		// become leader yet, wait until the first tick to try again.
		if ok {
			err = k.performLeaderActions()
			if err != nil {
				k.errors <- fmt.Errorf("error performing initial leader actions: %v", err)
			}
		}
		for {
			select {
			case <-leaderActions.C:
				ok, err := k.registerLeadership()
				if err != nil {
					k.errors <- fmt.Errorf("error registering leadership: %v", err)
				}
				if !ok {
					continue
				}
				err = k.performLeaderActions()
				if err != nil {
					k.errors <- fmt.Errorf("error performing repeated leader actions: %v", err)
				}
			case <-k.leaderLost:
				return
			}
		}
	}()
	k.isLeader = true
}

// unbecomeLeader stops the leadership goroutine.
func (k *Kinsumer) unbecomeLeader() {
	// Lock until we're done
	k.unbecomingLeader.Lock()
	defer k.unbecomingLeader.Unlock()

	if !k.isLeader {
		return
	}
	if k.leaderLost == nil {
		k.config.logger.Log("Lost leadership but k.leaderLost was nil")
	} else {
		close(k.leaderLost)
		k.leaderWG.Wait()
		k.leaderLost = nil
	}
	k.isLeader = false
}

// performLeaderActions updates the shard ID cache and reaps old clients
// TODO(dwe): Factor out dependencies and unit test
func (k *Kinsumer) performLeaderActions() error {
	shardCache, err := loadShardCacheFromDynamo(k.dynamodb, k.metadataTableName)
	if err != nil {
		return fmt.Errorf("error loading shard cache from dynamo: %v", err)
	}
	cachedShardIDs := shardCache.ShardIDs
	now := time.Now().UnixNano()
	if now-shardCache.LastUpdate < k.config.leaderActionFrequency.Nanoseconds() {
		return nil
	}
	curShardIDs, err := loadShardIDsFromKinesis(k.kinesis, k.streamName)
	if err != nil {
		return fmt.Errorf("error loading shard IDs from kinesis: %v", err)
	}

	checkpoints, err := loadCheckpoints(k.dynamodb, k.checkpointTableName)
	if err != nil {
		return fmt.Errorf("error loading shard IDs from dynamo: %v", err)
	}

	updatedShardIDs, changed := diffShardIDs(curShardIDs, cachedShardIDs, checkpoints)
	if changed {
		err = k.setCachedShardIDs(updatedShardIDs)
		if err != nil {
			return fmt.Errorf("error caching shard IDs to dynamo: %v", err)
		}
	}

	err = reapClients(k.dynamodb, k.clientsTableName)
	if err != nil {
		return fmt.Errorf("error reaping old clients: %v", err)
	}

	return nil
}

// setCachedShardIDs updates the shard ID cache in dynamo.
func (k *Kinsumer) setCachedShardIDs(shardIDs []string) error {
	if len(shardIDs) == 0 {
		return nil
	}
	now := time.Now()
	item, err := attributevalue.MarshalMap(&shardCacheRecord{
		Key:           shardCacheKey,
		ShardIDs:      shardIDs,
		LastUpdate:    now.UnixNano(),
		LastUpdateRFC: now.UTC().Format(time.RFC1123Z),
	})
	if err != nil {
		return fmt.Errorf("error marshalling map: %v", err)
	}

	_, err = k.dynamodb.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String(k.metadataTableName),
		Item:      item,
	})
	if err != nil {
		return fmt.Errorf("error updating shard cache: %v", err)
	}
	return nil
}

// diffShardIDs takes the current shard IDs and cached shards and returns the new sorted cache, ignoring
// finished shards correctly.
func diffShardIDs(curShardIDs, cachedShardIDs []string, checkpoints map[string]*checkpointRecord) (updatedShardIDs []string, changed bool) {
	// Look for differences, ignoring Finished shards.
	cur := make(map[string]bool)
	for _, s := range curShardIDs {
		cur[s] = true
	}
	for _, s := range cachedShardIDs {
		if cur[s] {
			delete(cur, s)
			// Drop the shard if it's been finished.
			if c, ok := checkpoints[s]; ok && c.Finished != nil {
				changed = true
			} else {
				updatedShardIDs = append(updatedShardIDs, s)
			}
		} else {
			// If a shard is no longer returned by ListShards, drop it.
			changed = true
		}
	}
	for s := range cur {
		// If the shard is returned by ListShards and not already Finished, add it.
		if c, ok := checkpoints[s]; !ok || c.Finished == nil {
			updatedShardIDs = append(updatedShardIDs, s)
			changed = true
		}
	}
	sort.Strings(updatedShardIDs)
	return
}

// deregisterLeadership marks us as no longer the leader in dynamo.
func (k *Kinsumer) deregisterLeadership() error {
	now := time.Now()
	attrVals, err := attributevalue.MarshalMap(map[string]interface{}{
		":ID":            aws.String(k.clientID),
		":lastUpdate":    aws.Int64(now.UnixNano()),
		":lastUpdateRFC": aws.String(now.UTC().Format(time.RFC1123Z)),
	})
	if err != nil {
		return fmt.Errorf("error marshaling deregisterLeadership ExpressionAttributeValues: %v", err)
	}
	_, err = k.dynamodb.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		TableName: aws.String(k.metadataTableName),
		Key: map[string]dbtypes.AttributeValue{
			"Key": &dbtypes.AttributeValueMemberS{Value: leaderKey},
		},
		ConditionExpression:       aws.String("ID = :ID"),
		UpdateExpression:          aws.String("REMOVE ID SET LastUpdate = :lastUpdate, LastUpdateRFC = :lastUpdateRFC"),
		ExpressionAttributeValues: attrVals,
	})
	if err != nil {
		// It's ok if we never actually became leader.
		var ccfe *dbtypes.ConditionalCheckFailedException
		if errors.As(err, &ccfe) {
			return nil
		}
	}
	return err
}

// registerLeadership marks us as the leader or just refreshes LastUpdate in dynamo, returning false if
// another node is the leader.
func (k *Kinsumer) registerLeadership() (bool, error) {
	now := time.Now()
	cutoff := now.Add(-k.maxAgeForLeaderRecord).UnixNano()
	attrVals, err := attributevalue.MarshalMap(map[string]interface{}{
		":ID":     aws.String(k.clientID),
		":cutoff": aws.Int64(cutoff),
	})
	if err != nil {
		return false, fmt.Errorf("error marshaling registerLeadership ExpressionAttributeValues: %v", err)
	}
	item, err := attributevalue.MarshalMap(map[string]interface{}{
		"Key":           aws.String(leaderKey),
		"ID":            aws.String(k.clientID),
		"Name":          aws.String(k.clientName),
		"LastUpdate":    aws.Int64(now.UnixNano()),
		"LastUpdateRFC": aws.String(now.UTC().Format(time.RFC1123Z)),
	})
	if err != nil {
		return false, fmt.Errorf("error marshaling registerLeadership Item: %v", err)
	}
	_, err = k.dynamodb.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName:                 aws.String(k.metadataTableName),
		Item:                      item,
		ConditionExpression:       aws.String("ID = :ID OR attribute_not_exists(ID) OR LastUpdate <= :cutoff"),
		ExpressionAttributeValues: attrVals,
	})
	if err != nil {
		var ccfe *dbtypes.ConditionalCheckFailedException
		if errors.As(err, &ccfe) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// loadShardIDsFromKinesis returns a sorted slice of shardIDs from kinesis.
// This function used to use kinesis.DescribeStream, which has a very low throttling limit of 10/s per account.
// As such, the leader is responsible for caching the shard list.
// Now that it uses ListShards, you could potentially query the shard list directly from all clients.
// TODO: Write unit test - needs kinesis mocking
func loadShardIDsFromKinesis(kin kinsumeriface.KinesisAPI, streamName string) ([]string, error) {
	var innerError error

	shardIDs := make([]string, 0)
	var token *string

	// Manually page the results since aws-sdk-go has no ListShardsPages.
	for {
		inputParams := kinesis.ListShardsInput{}
		if token != nil {
			inputParams.NextToken = token
		} else {
			inputParams.StreamName = aws.String(streamName)
		}
		res, err := kin.ListShards(context.TODO(), &inputParams)

		if err != nil {
			var riue *ktypes.ResourceInUseException
			var rnfe *ktypes.ResourceNotFoundException
			if errors.As(err, &riue) {
				innerError = ErrStreamBusy
			} else if errors.As(err, &rnfe) {
				innerError = ErrNoSuchStream
			}
		}

		if innerError != nil {
			return nil, innerError
		}

		if err != nil {
			return nil, err
		}

		for _, s := range res.Shards {
			shardIDs = append(shardIDs, aws.ToString(s.ShardId))
		}
		if res.NextToken == nil {
			break
		}
		token = res.NextToken
	}
	sort.Strings(shardIDs)

	return shardIDs, nil
}

// loadShardIDsFromDynamo returns the sorted slice of shardIDs from the metadata table in dynamo.
func loadShardIDsFromDynamo(db kinsumeriface.DynamoDBAPI, tableName string) ([]string, error) {
	record, err := loadShardCacheFromDynamo(db, tableName)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, nil
	}
	return record.ShardIDs, nil
}

// loadShardCacheFromDynamo returns the ShardCache record from the metadata table in dynamo.
func loadShardCacheFromDynamo(db kinsumeriface.DynamoDBAPI, tableName string) (*shardCacheRecord, error) {
	resp, err := db.GetItem(context.TODO(), &dynamodb.GetItemInput{
		TableName:      aws.String(tableName),
		ConsistentRead: aws.Bool(true),
		Key: map[string]dbtypes.AttributeValue{
			"Key": &dbtypes.AttributeValueMemberS{Value: shardCacheKey},
		},
	})
	if err != nil {
		var rnfe *ktypes.ResourceNotFoundException
		if errors.As(err, &rnfe) {
			return nil, nil
		}
		return nil, err
	}
	var record shardCacheRecord
	if err = attributevalue.UnmarshalMap(resp.Item, &record); err != nil {
		return nil, err
	}
	return &record, nil
}
