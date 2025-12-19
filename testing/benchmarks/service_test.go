package benchmarks

import (
	"context"
	"testing"

	"github.com/zoobzio/grub"
	grubtesting "github.com/zoobzio/grub/testing"
)

// TestRecord is a sample record type for benchmarks.
type TestRecord struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Value int    `json:"value"`
}

// BenchmarkService_Get measures Get operation performance.
func BenchmarkService_Get(b *testing.B) {
	provider := grubtesting.NewMockProvider()
	service := grub.New[TestRecord](provider)

	ctx := context.Background()
	record := TestRecord{ID: "test-1", Name: "Test", Value: 42}
	_ = service.Set(ctx, "key-1", record)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = service.Get(ctx, "key-1")
	}
}

// BenchmarkService_Set measures Set operation performance.
func BenchmarkService_Set(b *testing.B) {
	provider := grubtesting.NewMockProvider()
	service := grub.New[TestRecord](provider)

	ctx := context.Background()
	record := TestRecord{ID: "test-1", Name: "Test", Value: 42}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = service.Set(ctx, "key-1", record)
	}
}

// BenchmarkService_Exists measures Exists operation performance.
func BenchmarkService_Exists(b *testing.B) {
	provider := grubtesting.NewMockProvider()
	service := grub.New[TestRecord](provider)

	ctx := context.Background()
	record := TestRecord{ID: "test-1", Name: "Test", Value: 42}
	_ = service.Set(ctx, "key-1", record)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = service.Exists(ctx, "key-1")
	}
}

// BenchmarkService_Delete measures Delete operation performance.
func BenchmarkService_Delete(b *testing.B) {
	provider := grubtesting.NewMockProvider()
	service := grub.New[TestRecord](provider)

	ctx := context.Background()
	record := TestRecord{ID: "test-1", Name: "Test", Value: 42}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		_ = service.Set(ctx, "key-1", record)
		b.StartTimer()

		_ = service.Delete(ctx, "key-1")
	}
}

// BenchmarkService_Count measures Count operation performance.
func BenchmarkService_Count(b *testing.B) {
	provider := grubtesting.NewMockProvider()
	service := grub.New[TestRecord](provider)

	ctx := context.Background()

	// Populate with some records
	for i := 0; i < 100; i++ {
		record := TestRecord{ID: "test", Name: "Test", Value: i}
		_ = service.Set(ctx, "key-"+string(rune('0'+i%10))+string(rune('0'+i/10)), record)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = service.Count(ctx)
	}
}

// BenchmarkService_List measures List operation performance.
func BenchmarkService_List(b *testing.B) {
	provider := grubtesting.NewMockProvider()
	service := grub.New[TestRecord](provider)

	ctx := context.Background()

	// Populate with some records
	for i := 0; i < 100; i++ {
		record := TestRecord{ID: "test", Name: "Test", Value: i}
		_ = service.Set(ctx, "key-"+string(rune('0'+i%10))+string(rune('0'+i/10)), record)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _, _ = service.List(ctx, "", 10)
	}
}

// BenchmarkCodec_JSONMarshal measures JSON encoding performance.
func BenchmarkCodec_JSONMarshal(b *testing.B) {
	codec := grub.JSONCodec{}
	record := TestRecord{ID: "test-1", Name: "Test Record Name", Value: 42}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = codec.Marshal(record)
	}
}

// BenchmarkCodec_JSONUnmarshal measures JSON decoding performance.
func BenchmarkCodec_JSONUnmarshal(b *testing.B) {
	codec := grub.JSONCodec{}
	data := []byte(`{"id":"test-1","name":"Test Record Name","value":42}`)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var record TestRecord
		_ = codec.Unmarshal(data, &record)
	}
}

// BenchmarkService_GetSet_Parallel measures concurrent Get/Set performance.
func BenchmarkService_GetSet_Parallel(b *testing.B) {
	provider := grubtesting.NewMockProvider()
	service := grub.New[TestRecord](provider)

	ctx := context.Background()
	record := TestRecord{ID: "test-1", Name: "Test", Value: 42}
	_ = service.Set(ctx, "key-1", record)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = service.Get(ctx, "key-1")
		}
	})
}

// BenchmarkService_LargeRecord measures performance with larger records.
func BenchmarkService_LargeRecord(b *testing.B) {
	type LargeRecord struct {
		ID          string            `json:"id"`
		Name        string            `json:"name"`
		Description string            `json:"description"`
		Tags        []string          `json:"tags"`
		Metadata    map[string]string `json:"metadata"`
		Values      []int             `json:"values"`
	}

	provider := grubtesting.NewMockProvider()
	service := grub.New[LargeRecord](provider)

	ctx := context.Background()
	record := LargeRecord{
		ID:          "test-1",
		Name:        "Test Record",
		Description: "This is a longer description for the test record to simulate real-world data.",
		Tags:        []string{"tag1", "tag2", "tag3", "tag4", "tag5"},
		Metadata: map[string]string{
			"created_by": "test",
			"version":    "1.0.0",
			"region":     "us-east-1",
		},
		Values: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	}

	b.Run("Set", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = service.Set(ctx, "key-1", record)
		}
	})

	_ = service.Set(ctx, "key-1", record)

	b.Run("Get", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = service.Get(ctx, "key-1")
		}
	})
}
