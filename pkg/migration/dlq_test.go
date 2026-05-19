package migration

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/gsbingo17/mongodb-migration/pkg/logger"
)

func TestDLQWriterLifecycle(t *testing.T) {
	tmpDir := t.TempDir()
	dlqFilePath := filepath.Join(tmpDir, "test_dlq.jsonl")
	log := logger.New()

	// 1. Initialize DLQ
	writer, err := NewDLQWriter(dlqFilePath, log)
	if err != nil {
		t.Fatalf("failed to create DLQWriter: %v", err)
	}

	// 2. Write multiple failures
	err1 := errors.New("connection timed out")
	writer.WriteFailed("source_db", "col_1", "doc_id_123", err1, "initial", "insert", map[string]interface{}{"name": "value"})

	err2 := errors.New("duplicate key error")
	writer.WriteFailed("source_db", "col_1", "doc_id_456", err2, "incremental", "replace", map[string]interface{}{"name": "value2"})

	// 3. Verify memory counter
	if writer.Count() != 2 {
		t.Errorf("expected count to be 2, got %d", writer.Count())
	}

	// 4. Close writer
	writer.Close()

	// 5. Read file and assert contents
	file, err := os.Open(dlqFilePath)
	if err != nil {
		t.Fatalf("failed to open DLQ file for verification: %v", err)
	}
	defer file.Close()

	var records []DLQRecord
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		var record DLQRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("failed to parse DLQ line: %v", err)
		}
		records = append(records, record)
	}

	if err := scanner.Err(); err != nil {
		t.Fatalf("scanner error: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("expected 2 records on disk, got %d", len(records))
	}

	// Verify record 1
	r1 := records[0]
	if r1.SourceDB != "source_db" || r1.SourceCollection != "col_1" || r1.DocumentID != "doc_id_123" {
		t.Errorf("invalid record 1 metadata: %+v", r1)
	}
	if r1.Error != "connection timed out" || r1.Phase != "initial" || r1.OpType != "insert" {
		t.Errorf("invalid record 1 error/phase details: %+v", r1)
	}

	// Verify record 2
	r2 := records[1]
	if r2.SourceDB != "source_db" || r2.SourceCollection != "col_1" || r2.DocumentID != "doc_id_456" {
		t.Errorf("invalid record 2 metadata: %+v", r2)
	}
	if r2.Error != "duplicate key error" || r2.Phase != "incremental" || r2.OpType != "replace" {
		t.Errorf("invalid record 2 error/phase details: %+v", r2)
	}
}

func TestDLQWriterConcurrency(t *testing.T) {
	tmpDir := t.TempDir()
	dlqFilePath := filepath.Join(tmpDir, "concurrency_dlq.jsonl")
	log := logger.New()

	writer, err := NewDLQWriter(dlqFilePath, log)
	if err != nil {
		t.Fatalf("failed to create DLQWriter: %v", err)
	}
	defer writer.Close()

	var wg sync.WaitGroup
	numGoroutines := 10
	writesPerGoroutine := 50

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < writesPerGoroutine; j++ {
				writer.WriteFailed(
					"db",
					"col",
					j,
					errors.New("concurrent error"),
					"initial",
					"insert",
					nil,
				)
			}
		}(i)
	}

	wg.Wait()

	expectedCount := int64(numGoroutines * writesPerGoroutine)
	if writer.Count() != expectedCount {
		t.Errorf("expected count to be %d, got %d", expectedCount, writer.Count())
	}
}

func TestNopDLQWriter(t *testing.T) {
	nop := &NopDLQWriter{}
	nop.WriteFailed("db", "col", "id", errors.New("test"), "initial", "insert", nil)

	if nop.Count() != 0 {
		t.Errorf("expected NopDLQWriter count to always be 0")
	}

	// Should not panic
	nop.Close()
}
