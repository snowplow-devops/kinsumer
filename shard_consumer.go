// Copyright (c) 2016 Twitch Interactive

package kinsumer

import (
	"context"
	"errors"
	"fmt"
	"github.com/twitchscience/kinsumer/kinsumeriface"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	ktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	smithy "github.com/aws/smithy-go"
)

// batchProcessResult represents the flow control decision from processing a batch of records
type batchProcessResult int

const (
	batchSuccess  batchProcessResult = iota // continue normal processing
	batchContinue                           // equivalent to continue mainloop
	batchBreak                              // equivalent to break mainloop
	batchError                              // error occurred, exit function
)

// getShardIterator gets a shard iterator after the last sequence number we read or at the start of the stream
func getShardIterator(k kinsumeriface.KinesisAPI, streamName string, shardID string, sequenceNumber string, iteratorStartTimestamp *time.Time) (string, error) {
	shardIteratorType := ktypes.ShardIteratorTypeAfterSequenceNumber

	// If we do not have a sequenceNumber yet we need to get a shardIterator
	// from the horizon
	ps := aws.String(sequenceNumber)
	if sequenceNumber == "" && iteratorStartTimestamp != nil {
		shardIteratorType = ktypes.ShardIteratorTypeAtTimestamp
		ps = nil
	} else if sequenceNumber == "" {
		shardIteratorType = ktypes.ShardIteratorTypeTrimHorizon
		ps = nil
	} else if sequenceNumber == "LATEST" {
		// Backward compatibility: support "LATEST" as sequence number
		shardIteratorType = ktypes.ShardIteratorTypeLatest
		ps = nil
	}

	resp, err := k.GetShardIterator(context.Background(), &kinesis.GetShardIteratorInput{
		ShardId:                aws.String(shardID),
		ShardIteratorType:      shardIteratorType,
		StartingSequenceNumber: ps,
		StreamName:             aws.String(streamName),
		Timestamp:              iteratorStartTimestamp,
	})
	if err != nil {
		return "", err
	}
	return aws.ToString(resp.ShardIterator), err
}

// getRecords returns the next records and shard iterator from the given shard iterator
func getRecords(k kinsumeriface.KinesisAPI, iterator string, limit int) (records []ktypes.Record, nextIterator string, lag time.Duration, err error) {
	params := &kinesis.GetRecordsInput{
		Limit:         aws.Int32(int32(limit)),
		ShardIterator: aws.String(iterator),
	}

	output, err := k.GetRecords(context.Background(), params)

	if err != nil {
		return nil, "", 0, err
	}

	records = output.Records
	nextIterator = aws.ToString(output.NextShardIterator)
	lag = time.Duration(aws.ToInt64(output.MillisBehindLatest)) * time.Millisecond

	return records, nextIterator, lag, nil
}

// captureShard blocks until we capture the given shardID
func (k *Kinsumer) captureShard(shardID string) (*checkpointer, error) {
	// Attempt to capture the shard in dynamo
	for {
		// Ask the checkpointer to capture the shard
		checkpointer, err := capture(
			shardID,
			k.checkpointTableName,
			k.dynamodb,
			k.clientName,
			k.clientID,
			k.maxAgeForClientRecord,
			k.config.stats)
		if err != nil {
			return nil, err
		}

		if checkpointer != nil {
			return checkpointer, nil
		}

		// Throttle requests so that we don't hammer dynamo
		select {
		case <-k.stop:
			// If we are told to stop consuming we should stop attempting to capture
			return nil, nil
		case <-time.After(k.config.throttleDelay):
		}
	}
}

// processRecordsBatch handles getting and processing a batch of records with automatic semaphore management
func (k *Kinsumer) processRecordsBatch(iterator string, shardID string, checkpointer *checkpointer, lastSeqToCheckp *string, lastSeqNum *string, commitTicker *time.Ticker) (nextIterator string, result batchProcessResult, err error) {
	// Acquire semaphore to limit concurrent shard record fetching
	if k.shardSemaphore != nil {
		k.shardSemaphore <- struct{}{} // May block if limit reached
	}

	// Ensure semaphore is released regardless of how this function exits
	defer func() {
		if k.shardSemaphore != nil {
			<-k.shardSemaphore
		}
	}()

	// Get records from kinesis
	records, next, lag, err := getRecords(k.kinesis, iterator, k.config.getRecordsLimit)

	if err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) {
			origErrStr := fmt.Sprintf("(%s) ", ae)
			k.config.logger.Log("Got error: %s %s %s", ae.ErrorCode(), ae.ErrorMessage(), origErrStr)

			var eie *ktypes.ExpiredIteratorException
			if errors.As(err, &eie) {
				k.config.logger.Log("Got error: %s %s %s", eie.ErrorCode(), eie.ErrorMessage(), origErrStr)
				newIterator, ierr := getShardIterator(k.kinesis, k.streamName, shardID, *lastSeqToCheckp, nil)
				if ierr != nil {
					return "", batchError, fmt.Errorf("getShardIterator after expired iterator: %w", ierr)
				}
				// retry infinitely after expired iterator is renewed successfully
				return newIterator, batchContinue, nil
			}
		}
		return "", batchError, fmt.Errorf("getRecords: %w", err)
	}

	// Put all the records we got onto the channel
	k.config.stats.EventsFromKinesis(len(records), shardID, lag)
	k.metricsManager.updateMetric(RecordsIncrement, int64(len(records)))
	
	// Calculate total payload bytes in the batch
	totalBytes := int64(0)
	for _, record := range records {
		totalBytes += int64(len(record.Data))
	}
	k.metricsManager.updateMetric(BytesIncrement, totalBytes)
	
	// Track records for cleanup in case of early return
	recordsToCleanup := int64(len(records))
	bytesToCleanup := totalBytes
	defer func() {
		// Decrement any records that weren't successfully processed
		if recordsToCleanup > 0 {
			k.metricsManager.updateMetric(RecordsDecrement, recordsToCleanup)
		}
		if bytesToCleanup > 0 {
			k.metricsManager.updateMetric(BytesDecrement, bytesToCleanup)
		}
	}()
	
	if len(records) > 0 {
		retrievedAt := time.Now()
		for _, record := range records {
			// Loop until we stop or the record is consumed, checkpointing if necessary.
			recordPayloadBytes := int64(len(record.Data))
		RecordLoop:
			for {
				select {
				case <-commitTicker.C:
					finishCommitted, err := checkpointer.commit(k.config.commitFrequency)
					if err != nil {
						return "", batchError, fmt.Errorf("checkpointer.commit: %w", err)
					}
					if finishCommitted {
						return "", batchSuccess, nil
					}
				case <-k.stop:
					return "", batchBreak, nil
				case k.records <- &consumedRecord{
					record:       &record,
					checkpointer: checkpointer,
					retrievedAt:  retrievedAt,
					payloadBytes: recordPayloadBytes,
				}:
					recordsToCleanup-- // Record successfully sent to channel
					bytesToCleanup -= recordPayloadBytes // Decrement bytes for successfully sent record
					checkpointer.lastRecordPassed = time.Now() // Mark the time so we don't retain shards when we're too slow to do so
					*lastSeqToCheckp = aws.ToString(record.SequenceNumber)
					break RecordLoop
				}
			}
		}

		// Update the last sequence number we saw, in case we reached the end of the stream.
		*lastSeqNum = aws.ToString(records[len(records)-1].SequenceNumber)
	}
	
	return next, batchSuccess, nil
}

// consume is a blocking call that captures then consumes the given shard in a loop.
// It is also responsible for writing out the checkpoint updates to dynamo.
// TODO: There are no tests for this file. Not sure how to even unit test this.
func (k *Kinsumer) consume(shardID string) {
	defer k.waitGroup.Done()

	// commitTicker is used to periodically commit, so that we don't hammer dynamo every time
	// a shard wants to be check pointed
	commitTicker := time.NewTicker(k.config.commitFrequency)
	defer commitTicker.Stop()

	// capture the checkpointer
	checkpointer, err := k.captureShard(shardID)
	if err != nil {
		k.shardErrors <- shardConsumerError{shardID: shardID, action: "captureShard", err: err}
		return
	}

	// if we failed to capture the checkpointer but there was no errors
	// we must have stopped, so don't process this shard at all
	if checkpointer == nil {
		return
	}

	sequenceNumber := checkpointer.sequenceNumber

	// finished means we have reached the end of the shard but haven't necessarily processed/committed everything
	finished := false

	// Make sure we release the shard when we are done.
	defer func() {
		innerErr := checkpointer.release()
		if innerErr != nil {
			k.shardErrors <- shardConsumerError{shardID: shardID, action: "checkpointer.release", err: innerErr}
			return
		}
	}()

	// Get the starting shard iterator
	iterator, err := getShardIterator(k.kinesis, k.streamName, shardID, sequenceNumber, k.config.iteratorStartTimestamp)
	if err != nil {
		k.shardErrors <- shardConsumerError{shardID: shardID, action: "getShardIterator", err: err}
		return
	}

	// no throttle on the first request.
	nextThrottle := time.After(0)

	// lastSeqToCheckp is used to check if we have more data to checkpoint before we exit
	var lastSeqToCheckp string
	// lastSeqNum is used to check if a batch of data is the last in the stream
	var lastSeqNum string
mainloop:
	for {
		// We have reached the end of the shard's data. Set Finished in dynamo and stop processing.
		if iterator == "" && !finished {
			checkpointer.finish(lastSeqNum)
			finished = true
		}

		// Handle async actions, and throttle requests to keep kinesis happy
		select {
		case <-k.stop:
			break mainloop
		case <-commitTicker.C:
			finishCommitted, err := checkpointer.commit(k.config.commitFrequency)
			if err != nil {
				k.shardErrors <- shardConsumerError{shardID: shardID, action: "checkpointer.commit", err: err}
				return
			}
			if finishCommitted {
				return
			}
			// Go back to waiting for a throttle/stop.
			continue mainloop
		case <-nextThrottle:
		}

		// Reset the nextThrottle
		nextThrottle = time.After(k.config.throttleDelay)

		if finished {
			continue mainloop
		}

		// Process records batch with automatic semaphore management
		nextIterator, result, err := k.processRecordsBatch(iterator, shardID, checkpointer, &lastSeqToCheckp, &lastSeqNum, commitTicker)
		if err != nil {
			k.shardErrors <- shardConsumerError{shardID: shardID, action: "processRecordsBatch", err: err}
			return
		}
		
		// Handle flow control based on batch processing result
		switch result {
		case batchBreak:
			break mainloop
		case batchContinue:
			continue mainloop
		case batchError:
			return // Error already sent to shardErrors channel above
		case batchSuccess:
			// Continue to next iteration normally
		}
		
		iterator = nextIterator
	}
	// Handle checkpointer updates which occur after a stop request comes in (whose originating records were before)

	// commit first in case the checkpointer has been updates since the last commit.
	checkpointer.commitIntervalCounter = 0 // Reset commitIntervalCounter to avoid retaining ownership if there's no new sequence number
	_, err1 := checkpointer.commit(0 * time.Millisecond)
	if err1 != nil {
		k.shardErrors <- shardConsumerError{shardID: shardID, action: "checkpointer.commit", err: err}
		return
	}
	// Resume commit loop for some time, ensuring that we don't retain ownership unless there's a new sequence number.
	timeoutCounter := 0

	// If we have committed the last sequence number returned to the user, just return. Otherwise, keep committing until we reach that state
	if !checkpointer.dirty && checkpointer.sequenceNumber == lastSeqToCheckp {
		return
	}

	for {
		select {
		case <-commitTicker.C:
			timeoutCounter += int(k.config.commitFrequency)
			checkpointer.commitIntervalCounter = 0
			// passing 0 to commit ensures we no longer retain the shard.
			finishCommitted, err := checkpointer.commit(0 * time.Millisecond)
			if err != nil {
				k.shardErrors <- shardConsumerError{shardID: shardID, action: "checkpointer.commit", err: err}
				return
			}
			if finishCommitted {
				return
			}
			// Once we have committed the last sequence Number we passed to the user, return.
			if !checkpointer.dirty && checkpointer.sequenceNumber == lastSeqToCheckp {
				return
			}
			if timeoutCounter >= int(k.maxAgeForClientRecord/2) {
				return
			}
		}
	}
}
