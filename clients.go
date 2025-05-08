// Copyright (c) 2016 Twitch Interactive

package kinsumer

//TODO: The filename is bad

import (
	"context"
	"github.com/twitchscience/kinsumer/kinsumeriface"
	"sort"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const clientReapAge = 48 * time.Hour

type clientRecord struct {
	ID         string
	LastUpdate int64

	// Columns added to the table that are never used for decision making in the
	// library, rather they are useful for manual troubleshooting
	Name          string
	LastUpdateRFC string
}

type sortableClients []clientRecord

func (sc sortableClients) Len() int {
	return len(sc)
}

func (sc sortableClients) Less(left, right int) bool {
	return sc[left].ID < sc[right].ID
}

func (sc sortableClients) Swap(left, right int) {
	sc[left], sc[right] = sc[right], sc[left]
}

// registerWithClientsTable adds or updates our client with a current LastUpdate in dynamo
func registerWithClientsTable(db kinsumeriface.DynamoDBAPI, id, name, tableName string) error {
	now := time.Now()
	item, err := attributevalue.MarshalMap(clientRecord{
		ID:            id,
		Name:          name,
		LastUpdate:    now.UnixNano(),
		LastUpdateRFC: now.UTC().Format(time.RFC1123Z),
	})

	if err != nil {
		return err
	}

	if _, err = db.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item:      item,
	}); err != nil {
		return err
	}

	return nil
}

// deregisterWithClientsTable deletes our client from dynamo
func deregisterFromClientsTable(db kinsumeriface.DynamoDBAPI, id, tableName string) error {
	idStruct := struct{ ID string }{ID: id}
	item, err := attributevalue.MarshalMap(idStruct)

	if err != nil {
		return err
	}

	if _, err = db.DeleteItem(context.TODO(), &dynamodb.DeleteItemInput{
		TableName: aws.String(tableName),
		Key:       item,
	}); err != nil {
		return err
	}

	return nil
}

// getClients returns a sorted list of all recently-updated clients in dynamo
func getClients(db kinsumeriface.DynamoDBAPI, name string, tableName string, maxAgeForClientRecord time.Duration, referenceTime time.Time, shardCheckFrequency time.Duration) (clients []clientRecord, err error) {
	filterExpression := "LastUpdate > :cutoff"

	// shardCheckFrequency added to cutoff to avoid race condition caused by slow clients
	subtime := maxAgeForClientRecord + shardCheckFrequency
	cutoff := strconv.FormatInt(referenceTime.Add(-subtime).UnixNano(), 10)

	params := &dynamodb.ScanInput{
		TableName:        aws.String(tableName),
		ConsistentRead:   aws.Bool(true),
		FilterExpression: aws.String(filterExpression),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":cutoff": &types.AttributeValueMemberN{Value: cutoff},
		},
	}

	paginator := dynamodb.NewScanPaginator(db, params)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			return nil, err
		}

		var records []clientRecord

		err = attributevalue.UnmarshalListOfMaps(page.Items, &records)
		if err != nil {
			return nil, err
		}
		clients = append(clients, records...)
	}

	sort.Sort(sortableClients(clients))
	return clients, nil
}

// reapClients deletes any sufficiently old clients from dynamo
func reapClients(db kinsumeriface.DynamoDBAPI, tableName string) error {
	filterExpression := "LastUpdate < :cutoff"
	cutoff := strconv.FormatInt(time.Now().Add(-clientReapAge).UnixNano(), 10)

	params := &dynamodb.ScanInput{
		TableName:        aws.String(tableName),
		ConsistentRead:   aws.Bool(true),
		FilterExpression: aws.String(filterExpression),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":cutoff": &types.AttributeValueMemberN{Value: cutoff},
		},
	}

	var clients []clientRecord
	paginator := dynamodb.NewScanPaginator(db, params)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			return err
		}

		var records []clientRecord

		err = attributevalue.UnmarshalListOfMaps(page.Items, &records)
		if err != nil {
			return err
		}
		clients = append(clients, records...)
	}

	for _, client := range clients {
		idStruct := struct{ ID string }{ID: client.ID}
		item, err := attributevalue.MarshalMap(idStruct)
		if err != nil {
			return err
		}
		if _, err = db.DeleteItem(context.TODO(), &dynamodb.DeleteItemInput{
			TableName:           aws.String(tableName),
			Key:                 item,
			ConditionExpression: aws.String(filterExpression),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":cutoff": &types.AttributeValueMemberN{Value: cutoff},
			},
		}); err != nil {
			return err
		}
	}
	return nil
}
