package migration

import (
	"context"
	"hash/fnv"
	"testing"
	"time"

	"github.com/gsbingo17/mongodb-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestWorkerProcessEventUpdateWithNullFullDocumentAndDescription(t *testing.T) {
	log := logger.New()
	ctx := context.Background()

	// Create a worker with no targetDB (we won't flush/write to the DB in this test)
	worker := NewWorker(
		1,                                   // id
		ctx,                                 // ctx
		log,                                 // logger
		nil,                                 // targetDB
		nil,                                 // collectionMap
		10,                                  // batch size (larger than 1, so it won't flush)
		false,                               // forceOrdered
		nil,                                 // dlq
		nil,                                 // retryManager
		nil,                                 // statsManager
		false,                               // groupOpsByDistinctID
		5*time.Minute,                       // flushInterval
		8192,                                // incomingQueueSize
		2,                                   // processingQueueSize
	)

	// Create an update event where both fullDocument and updateDescription are nil/null
	event := bson.M{
		"operationType": "update",
		"ns": bson.M{
			"db":   "testdb",
			"coll": "testcoll",
		},
		"documentKey": bson.M{
			"_id": "doc_123",
		},
		"fullDocument":      nil,
		"updateDescription": nil,
	}

	// Process the event
	worker.ProcessEvent(event)

	// Verify that currentGroup exists and has the operation
	if worker.currentGroup == nil {
		t.Fatal("expected currentGroup to be populated, but got nil")
	}

	if len(worker.currentGroup.Operations) != 1 {
		t.Fatalf("expected 1 operation in group, got %d", len(worker.currentGroup.Operations))
	}

	op := worker.currentGroup.Operations[0]

	if op.DocumentID != "doc_123" {
		t.Errorf("expected DocumentID 'doc_123', got %v", op.DocumentID)
	}

	if op.OpType != "update" {
		t.Errorf("expected OpType 'update', got %q", op.OpType)
	}

	if op.Document != nil {
		t.Errorf("expected Document to be nil, got %v", op.Document)
	}

	if op.UpdateDescription != nil {
		t.Errorf("expected UpdateDescription to be nil, got %v", op.UpdateDescription)
	}
}

func TestWorkerProcessEventUpdateWithNullFullDocumentAndStatsManager(t *testing.T) {
	log := logger.New()
	ctx := context.Background()
	statsMgr := NewStatsManager(log, 0, false)

	worker := NewWorker(
		1,                                   // id
		ctx,                                 // ctx
		log,                                 // logger
		nil,                                 // targetDB
		nil,                                 // collectionMap
		10,                                  // batch size
		false,                               // forceOrdered
		nil,                                 // dlq
		nil,                                 // retryManager
		statsMgr,                            // statsManager
		false,                               // groupOpsByDistinctID
		5*time.Minute,                       // flushInterval
		8192,                                // incomingQueueSize
		2,                                   // processingQueueSize
	)

	// Create an update event where fullDocument is nil
	event := bson.M{
		"operationType": "update",
		"ns": bson.M{
			"db":   "testdb",
			"coll": "testcoll",
		},
		"documentKey": bson.M{
			"_id": "doc_123",
		},
		"fullDocument":      nil,
		"updateDescription": nil,
	}

	// Process the event
	worker.ProcessEvent(event)

	// Verify that the event was skipped (currentGroup is nil)
	if worker.currentGroup != nil {
		t.Fatalf("expected event to be skipped (currentGroup == nil), but got currentGroup with %d operations", len(worker.currentGroup.Operations))
	}

	// Verify that "update-doc-missing" metric was incremented
	statsMgr.mu.Lock()
	count := statsMgr.updatedThenDeletedSinceLastStats
	statsMgr.mu.Unlock()

	if count != 1 {
		t.Errorf("expected update-doc-missing metric count to be 1, got %v", count)
	}
}

// BenchmarkOldHashing measures the old way of hashing ObjectID
func BenchmarkOldHashing(b *testing.B) {
	id := primitive.NewObjectID()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bytes := []byte(id.Hex())
		h := fnv.New32a()
		h.Write(bytes)
		_ = int(h.Sum32())
	}
}

// BenchmarkNewHashing measures the new way of hashing ObjectID
func BenchmarkNewHashing(b *testing.B) {
	id := primitive.NewObjectID()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bytes := id[:]
		hash := uint32(2166136261)
		for _, v := range bytes {
			hash ^= uint32(v)
			hash *= 16777619
		}
		_ = int(hash)
	}
}
