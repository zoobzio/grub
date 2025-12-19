// Package dynamo provides a grub Provider implementation for Amazon DynamoDB.
package dynamo

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/zoobzio/grub"
)

// item represents the DynamoDB item structure for records.
type item struct {
	PK   string `dynamodbav:"pk"`
	Data []byte `dynamodbav:"data"`
}

// Client defines the DynamoDB client interface used by this provider.
// This allows for easy mocking in tests.
type Client interface {
	GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	Scan(ctx context.Context, params *dynamodb.ScanInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
	DescribeTable(ctx context.Context, params *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
}

// Provider implements grub.Provider for Amazon DynamoDB.
type Provider struct {
	client    Client
	tableName string
}

// New creates a DynamoDB provider using the given table.
// The table must have a partition key named "pk" of type String.
func New(client Client, tableName string) *Provider {
	return &Provider{
		client:    client,
		tableName: tableName,
	}
}

// Get retrieves raw bytes for the given key.
func (p *Provider) Get(ctx context.Context, key string) ([]byte, error) {
	output, err := p.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(p.tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: key},
		},
	})
	if err != nil {
		return nil, err
	}

	if output.Item == nil {
		return nil, grub.ErrNotFound
	}

	var it item
	if err := attributevalue.UnmarshalMap(output.Item, &it); err != nil {
		return nil, err
	}

	return it.Data, nil
}

// Set stores raw bytes at the given key.
func (p *Provider) Set(ctx context.Context, key string, data []byte) error {
	it := item{PK: key, Data: data}
	av, err := attributevalue.MarshalMap(it)
	if err != nil {
		return err
	}

	_, err = p.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(p.tableName),
		Item:      av,
	})
	return err
}

// Exists checks whether a key exists.
func (p *Provider) Exists(ctx context.Context, key string) (bool, error) {
	output, err := p.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(p.tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: key},
		},
		ProjectionExpression: aws.String("pk"),
	})
	if err != nil {
		return false, err
	}
	return output.Item != nil, nil
}

// Count returns the total number of items in the table.
// Note: This scans the entire table and may be slow/expensive for large datasets.
func (p *Provider) Count(ctx context.Context) (int64, error) {
	var count int64
	var lastKey map[string]types.AttributeValue

	for {
		output, err := p.client.Scan(ctx, &dynamodb.ScanInput{
			TableName:         aws.String(p.tableName),
			Select:            types.SelectCount,
			ExclusiveStartKey: lastKey,
		})
		if err != nil {
			return 0, err
		}

		count += int64(output.Count)

		if output.LastEvaluatedKey == nil {
			break
		}
		lastKey = output.LastEvaluatedKey
	}

	return count, nil
}

// List returns a paginated list of keys.
// The cursor should be empty for the first page, or the last key from the previous call.
func (p *Provider) List(ctx context.Context, cursor string, limit int) ([]string, string, error) {
	var exclusiveStartKey map[string]types.AttributeValue
	if cursor != "" {
		exclusiveStartKey = map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: cursor},
		}
	}

	output, err := p.client.Scan(ctx, &dynamodb.ScanInput{
		TableName:            aws.String(p.tableName),
		ProjectionExpression: aws.String("pk"),
		Limit:                aws.Int32(int32(limit)),
		ExclusiveStartKey:    exclusiveStartKey,
	})
	if err != nil {
		return nil, "", err
	}

	keys := make([]string, 0, len(output.Items))
	for _, itm := range output.Items {
		var it struct {
			PK string `dynamodbav:"pk"`
		}
		if err := attributevalue.UnmarshalMap(itm, &it); err != nil {
			return nil, "", err
		}
		keys = append(keys, it.PK)
	}

	// Determine next cursor
	var nextCursor string
	if output.LastEvaluatedKey != nil {
		if pk, ok := output.LastEvaluatedKey["pk"].(*types.AttributeValueMemberS); ok {
			nextCursor = pk.Value
		}
	}

	return keys, nextCursor, nil
}

// Delete removes the item at the given key.
func (p *Provider) Delete(ctx context.Context, key string) error {
	// Check existence first to return ErrNotFound
	exists, err := p.Exists(ctx, key)
	if err != nil {
		return err
	}
	if !exists {
		return grub.ErrNotFound
	}

	_, err = p.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(p.tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: key},
		},
	})
	return err
}

// Connect is a no-op as DynamoDB clients are stateless HTTP clients.
func (p *Provider) Connect(_ context.Context) error {
	return nil
}

// Close is a no-op as DynamoDB clients do not require closing.
func (p *Provider) Close(_ context.Context) error {
	return nil
}

// Health checks DynamoDB connectivity by describing the table.
func (p *Provider) Health(ctx context.Context) error {
	_, err := p.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(p.tableName),
	})
	return err
}

// Ensure Provider implements grub.Provider and grub.Lifecycle.
var (
	_ grub.Provider  = (*Provider)(nil)
	_ grub.Lifecycle = (*Provider)(nil)
)
