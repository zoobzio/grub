package pgvector

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	grubpgvector "github.com/zoobzio/grub/pgvector"
	"github.com/zoobzio/grub/testing/integration/vector"
)

var tc *vector.TestContext

func TestMain(m *testing.M) {
	ctx := context.Background()

	// Use pgvector/pgvector image which has the extension pre-installed
	pgContainer, err := postgres.Run(ctx,
		"pgvector/pgvector:pg16",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		panic("failed to start postgres container: " + err.Error())
	}

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		panic("failed to get connection string: " + err.Error())
	}

	db, err := sqlx.Connect("postgres", connStr)
	if err != nil {
		panic("failed to connect to postgres: " + err.Error())
	}

	// Enable pgvector extension and create test table
	if err := setupSchema(db); err != nil {
		panic("failed to setup schema: " + err.Error())
	}

	provider := grubpgvector.New(db, grubpgvector.Config{
		Table:          "vectors",
		IDColumn:       "id",
		VectorColumn:   "embedding",
		MetadataColumn: "metadata",
		Distance:       grubpgvector.L2,
	})

	tc = &vector.TestContext{
		Provider: provider,
		Cleanup: func() {
			_ = db.Close()
			_ = pgContainer.Terminate(ctx)
		},
	}

	code := m.Run()

	tc.Cleanup()

	os.Exit(code)
}

func setupSchema(db *sqlx.DB) error {
	queries := []string{
		`CREATE EXTENSION IF NOT EXISTS vector`,
		`DROP TABLE IF EXISTS vectors`,
		`CREATE TABLE vectors (
			id VARCHAR(255) PRIMARY KEY,
			embedding vector(3),
			metadata JSONB
		)`,
		`CREATE INDEX ON vectors USING ivfflat (embedding vector_l2_ops) WITH (lists = 100)`,
	}

	for _, q := range queries {
		if _, err := db.Exec(q); err != nil {
			return fmt.Errorf("query %q failed: %w", q, err)
		}
	}

	return nil
}

func TestPgvector_CRUD(t *testing.T) {
	vector.RunCRUDTests(t, tc)
}

func TestPgvector_Search(t *testing.T) {
	vector.RunSearchTests(t, tc)
}

func TestPgvector_Batch(t *testing.T) {
	vector.RunBatchTests(t, tc)
}

func TestPgvector_Atomic(t *testing.T) {
	vector.RunAtomicTests(t, tc)
}

func TestPgvector_Query(t *testing.T) {
	vector.RunQueryTests(t, tc, vector.QueryOperators{
		Range:    true,
		Like:     true,
		Contains: true,
	})
}

func TestPgvector_Filter(t *testing.T) {
	vector.RunFilterTests(t, tc, true)
}
