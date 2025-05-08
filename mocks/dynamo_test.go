// Copyright (c) 2016 Twitch Interactive

package mocks

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"reflect"
	"testing"
)

func TestMockDynamo(t *testing.T) {
	var table = "users"

	mock := NewMockDynamo([]string{table})

	// Make a few objects storable in dynamo
	type user struct {
		Name string
		ID   int64
	}

	ken := user{
		Name: "Ken Thompson",
		ID:   1,
	}
	user1, err := attributevalue.MarshalMap(ken)
	if err != nil {
		t.Fatalf("MarshalMap(user1) err=%q", err)
	}
	rob := user{
		Name: "Rob Pike",
		ID:   2,
	}
	user2, err := attributevalue.MarshalMap(rob)
	if err != nil {
		t.Fatalf("MarshalMap(user2) err=%q", err)
	}

	// Put the objects in
	if _, err = mock.PutItem(t.Context(), &dynamodb.PutItemInput{
		TableName: aws.String(table),
		Item:      user1,
	}); err != nil {
		t.Errorf("PutItem(user1) err=%q", err)
	}

	if _, err = mock.PutItem(t.Context(), &dynamodb.PutItemInput{
		TableName: aws.String(table),
		Item:      user2,
	}); err != nil {
		t.Errorf("PutItem(user2) err=%q", err)
	}

	// Try putting one into a nonexistent table - this should error
	if _, err = mock.PutItem(t.Context(), &dynamodb.PutItemInput{
		TableName: aws.String("nonexistent table"),
		Item:      user1,
	}); err == nil {
		t.Errorf("Writing to a nonexistent table should error")
	}

	// Get user1 back out
	resp, err := mock.GetItem(t.Context(), &dynamodb.GetItemInput{
		TableName: aws.String(table),
		Key: map[string]types.AttributeValue{
			"ID": &types.AttributeValueMemberN{Value: "1"},
		},
	})
	if err != nil {
		t.Errorf("GetItem(key1) err=%q", err)
	}

	var returnedUser user
	if err = attributevalue.UnmarshalMap(resp.Item, &returnedUser); err != nil {
		t.Fatalf("UnmarshalMap(GetItem response) err=%q", err)
	}

	if !reflect.DeepEqual(ken, returnedUser) {
		t.Errorf("Unexpected response from GetItem call. have=%+v  want=%+v", returnedUser, ken)
	}
}
