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

func TestDLQWriterInitialCountIsZero(t *testing.T) {
	tmpDir := t.TempDir()
	dlqFilePath := filepath.Join(tmpDir, "test_dlq.jsonl")
	log := logger.New()

	writer, err := NewDLQWriter(dlqFilePath, log)
	if err != nil {
		t.Fatalf("failed to create DLQWriter: %v", err)
	}
	defer writer.Close()

	if writer.Count() != 0 {
		t.Errorf("expected initial count to be 0, got %d", writer.Count())
	}
}

func TestDLQWriterIncrementsCountOnWrite(t *testing.T) {
	tmpDir := t.TempDir()
	dlqFilePath := filepath.Join(tmpDir, "test_dlq.jsonl")
	log := logger.New()

	writer, err := NewDLQWriter(dlqFilePath, log)
	if err != nil {
		t.Fatalf("failed to create DLQWriter: %v", err)
	}
	defer writer.Close()

	writer.WriteFailed("source_db", "col_1", "doc_id_123", errors.New("timeout"), "initial", "insert", nil)
	writer.WriteFailed("source_db", "col_1", "doc_id_456", errors.New("dup key"), "incremental", "replace", nil)

	if writer.Count() != 2 {
		t.Errorf("expected count to be 2, got %d", writer.Count())
	}
}

func TestDLQWriterWritesJSONLRecordsToDisk(t *testing.T) {
	tmpDir := t.TempDir()
	dlqFilePath := filepath.Join(tmpDir, "test_dlq.jsonl")
	log := logger.New()

	writer, err := NewDLQWriter(dlqFilePath, log)
	if err != nil {
		t.Fatalf("failed to create DLQWriter: %v", err)
	}

	err1 := errors.New("connection timed out")
	writer.WriteFailed("source_db", "col_1", "doc_id_123", err1, "initial", "insert", map[string]interface{}{"name": "value"})

	err2 := errors.New("duplicate key error")
	writer.WriteFailed("source_db", "col_1", "doc_id_456", err2, "incremental", "replace", map[string]interface{}{"name": "value2"})

	writer.Close()

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

	if len(records) != 2 {
		t.Fatalf("expected 2 records on disk, got %d", len(records))
	}

	// Verify record 1
	r1 := records[0]
	if r1.SourceDB != "source_db" || r1.SourceCollection != "col_1" || r1.DocumentID != "doc_id_123" {
		t.Errorf("invalid record 1 metadata: %+v", r1)
	}
	if r1.Error != "connection timed out" || r1.Phase != "initial" || r1.OpType != "insert" {
		t.Errorf("invalid record 1 error/phase: %+v", r1)
	}

	// Verify record 2
	r2 := records[1]
	if r2.SourceDB != "source_db" || r2.SourceCollection != "col_1" || r2.DocumentID != "doc_id_456" {
		t.Errorf("invalid record 2 metadata: %+v", r2)
	}
	if r2.Error != "duplicate key error" || r2.Phase != "incremental" || r2.OpType != "replace" {
		t.Errorf("invalid record 2 error/phase: %+v", r2)
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

// TestDLQWriterWriteFailedAfterClose verifies post-close thread-safety and panic prevention.
// When DLQWriter is closed, the underlying file resource is set to nil. Concurrently invoking
// WriteFailed from multiple worker threads must be handled gracefully, raising clean error logs but NEVER panicking.
func TestDLQWriterWriteFailedAfterClose(t *testing.T) {
	tmpDir := t.TempDir()
	dlqFilePath := filepath.Join(tmpDir, "closed_dlq.jsonl")
	log := logger.New()

	writer, err := NewDLQWriter(dlqFilePath, log)
	if err != nil {
		t.Fatalf("failed to create DLQWriter: %v", err)
	}

	// Close the writer immediately to set w.file = nil
	writer.Close()

	// Concurrently call WriteFailed and expect no panics
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// Should log error but NOT panic!
			writer.WriteFailed("db", "col", id, errors.New("write after close"), "incremental", "insert", nil)
		}(i)
	}
	wg.Wait()
}
