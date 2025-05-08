// Copyright (c) 2016 Twitch Interactive

package mocks

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/twitchscience/kinsumer/kinsumeriface"
	"testing"
)

var (
	// If mockDynamoErrorTrigger is passed in as the table name in a MockDynamo
	// request, then the MockDynamo will respond with nil output and
	// mockDynamoError.
	mockDynamoErrorTrigger = "error-trigger"
)

func errInternalError() error {
	return errors.New("triggered error")
}

func errMissingParameter(param string) error {
	return errors.New(fmt.Sprintf("missing required parameter %s", param))
}

func errTableNotFound(tableName string) error {
	return errors.New(fmt.Sprintf("table %q not found", tableName))
}

// Record of a call to MockDynamo. Stores a string name of the API endpoint
// ("PutItem", "GetItem", etc), the input struct received, the output struct
// sent back (if any), and the error sent back. The input and output are stored
// in interface{} so you'll need to do type assertions to pull out meaningful
// values.
type mockDynamoCallRecord struct {
	operation string
	input     interface{}
	output    interface{}
	err       error
}

// MockDynamo mocks the DynamoDB API in memory. It only supports GetItem,
// PutItem, and ScanPages. It only supports the most simple filter expressions:
// they must be of the form <column> <operator> :<value>, and operator must be
// =, <, <=, >, >=, or <>.
type MockDynamo struct {
	kinsumeriface.DynamoDBAPI

	// Stored data
	tables map[string][]mockDynamoItem

	// Diagnostic tools
	requests []mockDynamoCallRecord
}

// NewMockDynamo gets a dynamo interface for testing
func NewMockDynamo(tables []string) kinsumeriface.DynamoDBAPI {
	d := &MockDynamo{
		tables:   make(map[string][]mockDynamoItem),
		requests: make([]mockDynamoCallRecord, 0),
	}
	for _, t := range tables {
		d.addTable(t)
	}
	return d
}

func (d *MockDynamo) addTable(name string) {
	d.tables[name] = make([]mockDynamoItem, 0)
}

func (d *MockDynamo) recordCall(operation string, in, out interface{}, err error) {
	d.requests = append(d.requests, mockDynamoCallRecord{
		operation: operation,
		input:     in,
		output:    out,
		err:       err,
	})
}

// PutItem mocks the dynamo PutItem method
func (d *MockDynamo) PutItem(ctx context.Context, in *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (out *dynamodb.PutItemOutput, err error) {
	defer d.recordCall("PutItem", in, out, err)
	if in.TableName == nil {
		return nil, errMissingParameter("TableName")
	}
	if in.Item == nil {
		return nil, errMissingParameter("Item")
	}

	if aws.ToString(in.TableName) == mockDynamoErrorTrigger {
		return nil, errInternalError()
	}

	tableName := aws.ToString(in.TableName)
	if _, ok := d.tables[tableName]; !ok {
		return nil, errTableNotFound(tableName)
	}

	d.tables[tableName] = append(d.tables[tableName], in.Item)
	return &dynamodb.PutItemOutput{}, nil
}

// UpdateItem mocks the dynamo UpdateItem method
func (d *MockDynamo) UpdateItem(ctx context.Context, in *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (out *dynamodb.UpdateItemOutput, err error) {
	defer d.recordCall("UpdateItem", in, out, err)
	if in.TableName == nil {
		return nil, errMissingParameter("TableName")
	}
	if in.Key == nil {
		return nil, errMissingParameter("Key")
	}

	return &dynamodb.UpdateItemOutput{}, nil
}

// GetItem mocks the dynamo GetItem method
func (d *MockDynamo) GetItem(ctx context.Context, in *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (out *dynamodb.GetItemOutput, err error) {
	defer d.recordCall("GetItem", in, out, err)

	if in.TableName == nil {
		return nil, errMissingParameter("TableName")
	}
	if in.Key == nil {
		return nil, errMissingParameter("Key")
	}
	if aws.ToString(in.TableName) == mockDynamoErrorTrigger {
		return nil, errInternalError()
	}

	tableName := aws.ToString(in.TableName)
	if _, ok := d.tables[tableName]; !ok {
		return nil, errTableNotFound(tableName)
	}

	var filters []dynamoFilter
	for col, operand := range in.Key {
		filters = append(filters, dynamoFilter{
			col:     col,
			comp:    attrEq,
			operand: operand,
		})
	}

	var match map[string]types.AttributeValue
ItemLoop:
	for _, item := range d.tables[tableName] {
		for _, filter := range filters {
			if !item.applyFilter(filter) {
				continue ItemLoop
			}
		}
		match = item
		break
	}

	return &dynamodb.GetItemOutput{Item: match}, nil
}

type mockDynamoItem map[string]types.AttributeValue

func attrEq(l, r types.AttributeValue) bool {
	return attributeValueEqualS(l, r) || attributeValueEqualN(l, r)
}

func attributeValueEqualS(l, r types.AttributeValue) bool {
	l1, ok1 := l.(*types.AttributeValueMemberS)
	r1, ok2 := r.(*types.AttributeValueMemberS)
	return ok1 && ok2 && (*l1).Value == (*r1).Value
}

func attributeValueEqualN(l, r types.AttributeValue) bool {
	l1, ok1 := l.(*types.AttributeValueMemberN)
	r1, ok2 := r.(*types.AttributeValueMemberN)
	return ok1 && ok2 && (*l1).Value == (*r1).Value
}

type dynamoFilter struct {
	col     string
	comp    func(l, r types.AttributeValue) bool
	operand types.AttributeValue
}

func (i mockDynamoItem) applyFilter(f dynamoFilter) bool {
	// Special case: an empty string is a filter which always returns true
	if f.col == "" {
		return true
	}

	itemVal, ok := i[f.col]
	if !ok {
		return false
	}

	return f.comp(itemVal, f.operand)
}

// AssertNoRequestsMade will Execute a function, asserting that no requests
// are made over the course of the function.
func AssertNoRequestsMade(t *testing.T, mock *MockDynamo, msg string, f func()) {
	nStart := len(mock.requests)
	f()
	nEnd := len(mock.requests)
	if nEnd > nStart {
		for i := nStart; i < nEnd; i++ {
			t.Errorf("%s: unexpected %s request made to dynamo", msg, mock.requests[i].operation)
		}
	}
}

// AssertRequestMade will Execute a function, asserting that at least one request
// is made over the course of the function.
func AssertRequestMade(t *testing.T, mock *MockDynamo, msg string, f func()) {
	nStart := len(mock.requests)
	f()
	nEnd := len(mock.requests)
	if nEnd == nStart {
		t.Errorf("%s: expected a call to be made to dynamo, but didn't see one", msg)
	}
}
