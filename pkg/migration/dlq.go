package migration

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gsbingo17/mongodb-migration/pkg/logger"
)

// DLQRecord represents a single failed document record in the Dead Letter Queue
type DLQRecord struct {
	SourceDB         string      `json:"sourceDB"`
	SourceCollection string      `json:"sourceCollection"`
	DocumentID       interface{} `json:"documentID"`
	Error            string      `json:"error"`
	Phase            string      `json:"phase"`  // "initial" or "incremental"
	OpType           string      `json:"opType"` // "insert", "update", "replace", "delete"
	Timestamp        string      `json:"timestamp"`
	Document         interface{} `json:"document,omitempty"`
}

// DLQWriter provides thread-safe writing of failed documents to a JSONL file
type DLQWriter struct {
	filePath string
	file     *os.File
	mu       sync.Mutex
	count    int64 // atomic counter for total records written
	log      *logger.Logger
}

// NewDLQWriter creates a new DLQ writer that appends to the given file path.
// The file is created if it doesn't exist, or appended to if it does.
func NewDLQWriter(filePath string, log *logger.Logger) (*DLQWriter, error) {
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open DLQ file %s: %w", filePath, err)
	}

	log.Infof("DLQ file opened: %s", filePath)

	return &DLQWriter{
		filePath: filePath,
		file:     file,
		log:      log,
	}, nil
}

// WriteFailed appends a failed document record to the DLQ file.
// This method is safe to call from multiple goroutines.
func (w *DLQWriter) WriteFailed(sourceDB, sourceCollection string, documentID interface{}, err error, phase, opType string, document interface{}) {
	record := DLQRecord{
		SourceDB:         sourceDB,
		SourceCollection: sourceCollection,
		DocumentID:       documentID,
		Error:            err.Error(),
		Phase:            phase,
		OpType:           opType,
		Timestamp:        time.Now().Format(time.RFC3339),
		Document:         document,
	}

	data, marshalErr := json.Marshal(record)
	if marshalErr != nil {
		w.log.Errorf("DLQ: Failed to marshal record for %s.%s _id=%v: %v", sourceDB, sourceCollection, documentID, marshalErr)
		return
	}

	// Append newline for JSONL format
	data = append(data, '\n')

	w.mu.Lock()
	_, writeErr := w.file.Write(data)
	w.mu.Unlock()

	if writeErr != nil {
		w.log.Errorf("DLQ: Failed to write record for %s.%s _id=%v: %v", sourceDB, sourceCollection, documentID, writeErr)
		return
	}

	atomic.AddInt64(&w.count, 1)
	w.log.Warnf("DLQ: Document written to dead letter queue [db=%s, collection=%s, _id=%v, phase=%s, opType=%s, error=%v]",
		sourceDB, sourceCollection, documentID, phase, opType, err)
}

// Count returns the total number of records written to this DLQ file.
func (w *DLQWriter) Count() int64 {
	return atomic.LoadInt64(&w.count)
}

// Close flushes and closes the DLQ file, logging a summary.
func (w *DLQWriter) Close() {
	if w.file == nil {
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Sync(); err != nil {
		w.log.Errorf("DLQ: Failed to sync file %s: %v", w.filePath, err)
	}

	if err := w.file.Close(); err != nil {
		w.log.Errorf("DLQ: Failed to close file %s: %v", w.filePath, err)
	}

	count := atomic.LoadInt64(&w.count)
	if count > 0 {
		w.log.Warnf("DLQ: %d failed documents written to %s", count, w.filePath)
	} else {
		w.log.Infof("DLQ: No failed documents (file: %s)", w.filePath)
	}

	w.file = nil
}

// NopDLQWriter is a no-op DLQ writer that discards all records.
// Used as a safe default when DLQ is not configured.
type NopDLQWriter struct{}

// WriteFailed is a no-op for NopDLQWriter.
func (w *NopDLQWriter) WriteFailed(sourceDB, sourceCollection string, documentID interface{}, err error, phase, opType string, document interface{}) {
}

// Count always returns 0 for NopDLQWriter.
func (w *NopDLQWriter) Count() int64 { return 0 }

// Close is a no-op for NopDLQWriter.
func (w *NopDLQWriter) Close() {}

// DLQ is the interface that both DLQWriter and NopDLQWriter implement.
type DLQ interface {
	WriteFailed(sourceDB, sourceCollection string, documentID interface{}, err error, phase, opType string, document interface{})
	Count() int64
	Close()
}
