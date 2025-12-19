package dynamo

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/zoobzio/grub"
)

// mockClient implements the Client interface for testing.
type mockClient struct {
	items     map[string]map[string]types.AttributeValue
	getErr    error
	putErr    error
	deleteErr error
	scanErr   error
}

func newMockClient() *mockClient {
	return &mockClient{
		items: make(map[string]map[string]types.AttributeValue),
	}
}

func (m *mockClient) GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	pk := params.Key["pk"].(*types.AttributeValueMemberS).Value
	item, ok := m.items[pk]
	if !ok {
		return &dynamodb.GetItemOutput{Item: nil}, nil
	}
	return &dynamodb.GetItemOutput{Item: item}, nil
}

func (m *mockClient) PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	if m.putErr != nil {
		return nil, m.putErr
	}
	pk := params.Item["pk"].(*types.AttributeValueMemberS).Value
	m.items[pk] = params.Item
	return &dynamodb.PutItemOutput{}, nil
}

func (m *mockClient) DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	if m.deleteErr != nil {
		return nil, m.deleteErr
	}
	pk := params.Key["pk"].(*types.AttributeValueMemberS).Value
	delete(m.items, pk)
	return &dynamodb.DeleteItemOutput{}, nil
}

func (m *mockClient) Scan(ctx context.Context, params *dynamodb.ScanInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	if m.scanErr != nil {
		return nil, m.scanErr
	}

	var items []map[string]types.AttributeValue
	for _, item := range m.items {
		items = append(items, item)
	}

	limit := int32(100)
	if params.Limit != nil {
		limit = *params.Limit
	}

	if int32(len(items)) > limit {
		items = items[:limit]
	}

	return &dynamodb.ScanOutput{
		Items: items,
		Count: int32(len(items)),
	}, nil
}

func TestProvider_New(t *testing.T) {
	client := newMockClient()
	provider := New(client, "test-table")

	if provider == nil {
		t.Fatal("expected non-nil provider")
	}

	if provider.tableName != "test-table" {
		t.Errorf("tableName: got %s, want test-table", provider.tableName)
	}
}

func TestProvider_Get(t *testing.T) {
	client := newMockClient()
	provider := New(client, "test-table")
	ctx := context.Background()

	// Set up test data
	client.items["key1"] = map[string]types.AttributeValue{
		"pk":   &types.AttributeValueMemberS{Value: "key1"},
		"data": &types.AttributeValueMemberB{Value: []byte("test-data")},
	}

	data, err := provider.Get(ctx, "key1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if string(data) != "test-data" {
		t.Errorf("got %s, want test-data", data)
	}
}

func TestProvider_Get_NotFound(t *testing.T) {
	client := newMockClient()
	provider := New(client, "test-table")
	ctx := context.Background()

	_, err := provider.Get(ctx, "missing")
	if err != grub.ErrNotFound {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestProvider_Set(t *testing.T) {
	client := newMockClient()
	provider := New(client, "test-table")
	ctx := context.Background()

	err := provider.Set(ctx, "key1", []byte("new-data"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, ok := client.items["key1"]; !ok {
		t.Error("item not stored")
	}
}

func TestProvider_Exists(t *testing.T) {
	client := newMockClient()
	provider := New(client, "test-table")
	ctx := context.Background()

	// Key doesn't exist
	exists, err := provider.Exists(ctx, "missing")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exists {
		t.Error("expected false for missing key")
	}

	// Add key
	client.items["present"] = map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "present"},
	}

	exists, err = provider.Exists(ctx, "present")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !exists {
		t.Error("expected true for present key")
	}
}

func TestProvider_Count(t *testing.T) {
	client := newMockClient()
	provider := New(client, "test-table")
	ctx := context.Background()

	// Empty
	count, err := provider.Count(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0, got %d", count)
	}

	// Add items
	client.items["a"] = map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}}
	client.items["b"] = map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "b"}}

	count, err = provider.Count(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2, got %d", count)
	}
}

func TestProvider_List(t *testing.T) {
	client := newMockClient()
	provider := New(client, "test-table")
	ctx := context.Background()

	client.items["key1"] = map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "key1"}}
	client.items["key2"] = map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "key2"}}

	keys, _, err := provider.List(ctx, "", 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(keys) != 2 {
		t.Errorf("expected 2 keys, got %d", len(keys))
	}
}

func TestProvider_Delete(t *testing.T) {
	client := newMockClient()
	provider := New(client, "test-table")
	ctx := context.Background()

	client.items["key1"] = map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "key1"}}

	err := provider.Delete(ctx, "key1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, ok := client.items["key1"]; ok {
		t.Error("item should be deleted")
	}
}

func TestProvider_Delete_NotFound(t *testing.T) {
	client := newMockClient()
	provider := New(client, "test-table")
	ctx := context.Background()

	err := provider.Delete(ctx, "missing")
	if err != grub.ErrNotFound {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestProvider_ImplementsProvider(t *testing.T) {
	var _ grub.Provider = (*Provider)(nil)
}

func TestItem_Structure(t *testing.T) {
	it := item{
		PK:   "test-pk",
		Data: []byte("test-data"),
	}

	if it.PK != "test-pk" {
		t.Errorf("PK: got %s, want test-pk", it.PK)
	}
	if string(it.Data) != "test-data" {
		t.Errorf("Data: got %s, want test-data", it.Data)
	}
}

// Verify mock implements Client
var _ Client = (*mockClient)(nil)

// Verify aws package is used
var _ = aws.String
