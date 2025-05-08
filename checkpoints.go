// Copyright (c) 2016 Twitch Interactive

package kinsumer

import (
	"context"
	"errors"
	"fmt"
	"github.com/twitchscience/kinsumer/kinsumeriface"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Note: Not thread safe!

type checkpointer struct {
	shardID               string
	tableName             string
	dynamodb              kinsumeriface.DynamoDBAPI
	sequenceNumber        string
	ownerName             string
	ownerID               string
	maxAgeForClientRecord time.Duration
	stats                 StatReceiver
	captured              bool
	dirty                 bool
	mutex                 sync.Mutex
	finished              bool
	finalSequenceNumber   string
	updateSequencer       chan struct{}
	lastUpdate            int64
	commitIntervalCounter time.Duration
	lastRecordPassed      time.Time
}

type checkpointRecord struct {
	Shard          string
	SequenceNumber *string // last read sequence number, null if the shard has never been consumed
	LastUpdate     int64   // timestamp of last commit/ownership change
	OwnerName      *string // uuid of owning client, null if the shard is unowned
	Finished       *int64  // timestamp of when the shard was fully consumed, null if it's active

	// Columns added to the table that are never used for decision making in the
	// library, rather they are useful for manual troubleshooting
	OwnerID       *string
	LastUpdateRFC string
	FinishedRFC   *string
}

// capture is a non-blocking call that attempts to capture the given shard/checkpoint.
// It returns a checkpointer on success, or nil if it fails to capture the checkpoint
func capture(
	shardID string,
	tableName string,
	dynamodbiface kinsumeriface.DynamoDBAPI,
	ownerName string,
	ownerID string,
	maxAgeForClientRecord time.Duration,
	stats StatReceiver) (*checkpointer, error) {

	cutoff := time.Now().Add(-maxAgeForClientRecord).UnixNano()

	// Grab the entry from dynamo assuming there is one
	resp, err := dynamodbiface.GetItem(context.TODO(), &dynamodb.GetItemInput{
		TableName:      aws.String(tableName),
		ConsistentRead: aws.Bool(true),
		Key: map[string]types.AttributeValue{
			"Shard": &types.AttributeValueMemberS{Value: shardID},
		},
	})

	if err != nil {
		return nil, fmt.Errorf("error calling GetItem on shard checkpoint: %v", err)
	}

	// Convert to struct so we can work with the values
	var record checkpointRecord
	if err = attributevalue.UnmarshalMap(resp.Item, &record); err != nil {
		return nil, err
	}

	// If the record is marked as owned by someone else, and has not expired
	if record.OwnerID != nil && record.LastUpdate > cutoff {
		// We fail to capture it
		return nil, nil
	}

	// Make sure the Shard is set in case there was no record
	record.Shard = shardID

	// Mark us as the owners
	record.OwnerID = &ownerID
	record.OwnerName = &ownerName

	// Update timestamp
	now := time.Now()
	record.LastUpdate = now.UnixNano()
	record.LastUpdateRFC = now.UTC().Format(time.RFC1123Z)

	item, err := attributevalue.MarshalMap(record)
	if err != nil {
		return nil, err
	}

	attrVals, err := attributevalue.MarshalMap(map[string]interface{}{
		":cutoff":   aws.Int64(cutoff),
		":nullType": aws.String("NULL"),
	})
	if err != nil {
		return nil, err
	}
	if _, err = dynamodbiface.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item:      item,
		// The OwnerID doesn't exist if the entry doesn't exist, but PutItem with a marshaled
		// checkpointRecord sets a nil OwnerID to the NULL type.
		ConditionExpression: aws.String(
			"attribute_not_exists(OwnerID) OR attribute_type(OwnerID, :nullType) OR LastUpdate <= :cutoff"),
		ExpressionAttributeValues: attrVals,
	}); err != nil {
		var ccfe *types.ConditionalCheckFailedException
		if errors.As(err, &ccfe) {
			// We failed to capture it
			return nil, nil
		}
		return nil, err
	}

	checkpointer := &checkpointer{
		shardID:               shardID,
		tableName:             tableName,
		dynamodb:              dynamodbiface,
		ownerName:             ownerName,
		ownerID:               ownerID,
		stats:                 stats,
		sequenceNumber:        aws.ToString(record.SequenceNumber),
		maxAgeForClientRecord: maxAgeForClientRecord,
		captured:              true,
		lastUpdate:            record.LastUpdate,
	}

	return checkpointer, nil
}

// commit writes the latest SequenceNumber consumed to dynamo and updates LastUpdate.
// Returns true if we set Finished in dynamo because the library user finished consuming the shard.
// Once that has happened, the checkpointer should be released and never grabbed again.
func (cp *checkpointer) commit(commitFrequency time.Duration) (bool, error) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	if !cp.dirty && !cp.finished {
		cp.commitIntervalCounter += commitFrequency

		// If we have recently passed a record to the user, don't update the table when we don't have a new sequence number
		// If we haven't, update at a rate of maxAgeForClientRecord/2
		if (time.Now().Sub(cp.lastRecordPassed) < cp.maxAgeForClientRecord/2) || (cp.commitIntervalCounter < cp.maxAgeForClientRecord/2) {
			return false, nil
		}
	}
	cp.commitIntervalCounter = 0 // Reset the counter if we're registering a commit
	now := time.Now()

	sn := &cp.sequenceNumber
	if cp.sequenceNumber == "" {
		// We are not allowed to pass empty strings to dynamo, so instead pass a nil *string
		// to 'unset' it
		sn = nil
	}

	record := checkpointRecord{
		Shard:          cp.shardID,
		SequenceNumber: sn,
		LastUpdate:     now.UnixNano(),
		LastUpdateRFC:  now.UTC().Format(time.RFC1123Z),
	}
	finished := false
	if cp.finished && (cp.sequenceNumber == cp.finalSequenceNumber || cp.finalSequenceNumber == "") {
		record.Finished = aws.Int64(now.UnixNano())
		record.FinishedRFC = aws.String(now.UTC().Format(time.RFC1123Z))
		finished = true
	}
	record.OwnerID = &cp.ownerID
	record.OwnerName = &cp.ownerName

	item, err := attributevalue.MarshalMap(&record)
	if err != nil {
		return false, err
	}

	attrVals, err := attributevalue.MarshalMap(map[string]interface{}{
		":ownerID": aws.String(cp.ownerID),
	})
	if err != nil {
		return false, err
	}
	if _, err = cp.dynamodb.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName:                 aws.String(cp.tableName),
		Item:                      item,
		ConditionExpression:       aws.String("OwnerID = :ownerID"),
		ExpressionAttributeValues: attrVals,
	}); err != nil {
		var ccfe *types.ConditionalCheckFailedException
		if errors.As(err, &ccfe) && cp.lastUpdate < time.Now().Add(-cp.maxAgeForClientRecord).UnixNano() {
			return false, nil
		}

		return false, fmt.Errorf("error committing checkpoint: %s", err)
	}

	cp.lastUpdate = record.LastUpdate // update our internal copy of last update.

	if sn != nil {
		cp.stats.Checkpoint()
	}
	cp.dirty = false
	return finished, nil
}

// release releases our ownership of the checkpoint in dynamo so another client can take it
func (cp *checkpointer) release() error {
	now := time.Now()

	attrVals, err := attributevalue.MarshalMap(map[string]interface{}{
		":ownerID":        aws.String(cp.ownerID),
		":sequenceNumber": aws.String(cp.sequenceNumber),
		":lastUpdate":     aws.Int64(now.UnixNano()),
		":lastUpdateRFC":  aws.String(now.UTC().Format(time.RFC1123Z)),
	})
	if err != nil {
		return err
	}
	if _, err = cp.dynamodb.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		TableName: aws.String(cp.tableName),
		Key: map[string]types.AttributeValue{
			"Shard": &types.AttributeValueMemberS{Value: cp.shardID},
		},
		UpdateExpression: aws.String("REMOVE OwnerID, OwnerName " +
			"SET LastUpdate = :lastUpdate, LastUpdateRFC = :lastUpdateRFC, " +
			"SequenceNumber = :sequenceNumber"),
		ConditionExpression:       aws.String("OwnerID = :ownerID"),
		ExpressionAttributeValues: attrVals,
	}); err != nil {
		var ccfe *types.ConditionalCheckFailedException
		if errors.As(err, &ccfe) && cp.lastUpdate < time.Now().Add(-cp.maxAgeForClientRecord).UnixNano() {
			// If we failed conditional check, and the record has expired, assume that another client has legitimately siezed the shard.
			return nil
		}
		return fmt.Errorf("error releasing checkpoint: %s", err)
	}

	if cp.sequenceNumber != "" {
		cp.stats.Checkpoint()
	}

	cp.captured = false

	return nil
}

// update updates the current sequenceNumber of the checkpoint, marking it dirty if necessary
func (cp *checkpointer) update(sequenceNumber string) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()
	cp.dirty = cp.dirty || cp.sequenceNumber != sequenceNumber
	cp.sequenceNumber = sequenceNumber
}

// updateFunc returns a function that will update to sequenceNumber when called, but maintains ordering
func (cp *checkpointer) updateFunc(sequenceNumber string) func() {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()
	// cp.updateSequencer represents whether the previous updateFunc has been called
	// If nil there is no previous so we should act like there was one already called
	if cp.updateSequencer == nil {
		cp.updateSequencer = make(chan struct{})
		close(cp.updateSequencer)
	}
	// Copy the previous channel and create a new one for the link to the next updateFunc
	updateSequencer := cp.updateSequencer
	cp.updateSequencer = make(chan struct{})
	// Return everything in a closure to ensure references are maintained properly
	return func(prev chan struct{}, sequenceNumber string, next chan struct{}) func() {
		var once sync.Once
		return func() {
			once.Do(func() {
				<-prev // Wait for all prior updateFuncs to be called
				cp.update(sequenceNumber)
				close(next) // Allow the next updateFunc to be called
			})
		}
	}(updateSequencer, sequenceNumber, cp.updateSequencer)
}

// finish marks the given sequence number as the final one for the shard.
// sequenceNumber is the empty string if we never read anything from the shard.
func (cp *checkpointer) finish(sequenceNumber string) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()
	cp.finalSequenceNumber = sequenceNumber
	cp.finished = true
}

// loadCheckpoints returns checkpoint records from dynamo mapped by shard id.
func loadCheckpoints(db kinsumeriface.DynamoDBAPI, tableName string) (map[string]*checkpointRecord, error) {
	params := &dynamodb.ScanInput{
		TableName:      aws.String(tableName),
		ConsistentRead: aws.Bool(true),
	}

	var allRecords []checkpointRecord
	paginator := dynamodb.NewScanPaginator(db, params)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			return nil, err
		}

		var recordsIn []checkpointRecord
		err = attributevalue.UnmarshalListOfMaps(page.Items, &recordsIn)
		if err != nil {
			return nil, err
		}

		allRecords = append(allRecords, recordsIn...)
	}

	checkpointMap := make(map[string]*checkpointRecord, len(allRecords))
	for _, checkpoint := range allRecords {
		checkpointMap[checkpoint.Shard] = &checkpoint
	}
	return checkpointMap, nil
}
