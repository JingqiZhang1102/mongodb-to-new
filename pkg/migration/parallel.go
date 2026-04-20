package migration

import (
	"context"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"time"

	"github.com/gsbingo17/mongodb-migration/pkg/config"
	"github.com/gsbingo17/mongodb-migration/pkg/db"
	"github.com/gsbingo17/mongodb-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// EventDistributor manages the distribution of change events to workers
type EventDistributor struct {
	workers                   []*Worker
	incrementalWorkerCount    int
	sourceDB                  *db.MongoDB
	targetDB                  *db.MongoDB
	collectionMap             map[string]map[string]string
	changeStream              *mongo.ChangeStream
	ctx                       context.Context
	log                       *logger.Logger
	resumeTokenPath           string
	checkpointInterval        time.Duration
	saveThreshold             int
	incrementalWriteBatchSize int
	forceOrderedOperations    bool
	flushInterval             time.Duration // Flush interval in milliseconds

	// Statistics tracking
	statsMu              sync.Mutex    // Mutex for thread-safe updates to statistics
	statsInterval        time.Duration // Statistics reporting interval
	lastStatsTime        time.Time     // Time of last stats report
	eventsSinceLastStats int           // Events processed since last stats report
}

// NewEventDistributor creates a new event distributor
func NewEventDistributor(ctx context.Context, sourceDB, targetDB *db.MongoDB,
	collectionMap map[string]map[string]string,
	changeStream *mongo.ChangeStream, log *logger.Logger,
	resumeTokenPath string, checkpointInterval time.Duration,
	saveThreshold, incrementalWorkerCount, incrementalWriteBatchSize int,
	forceOrderedOperations bool, flushInterval time.Duration, cfg *config.Config) *EventDistributor {

	// Use stats interval from config
	statsInterval := time.Duration(cfg.StatsIntervalMinutes) * time.Minute

	return &EventDistributor{
		workers:                   make([]*Worker, incrementalWorkerCount),
		incrementalWorkerCount:    incrementalWorkerCount,
		sourceDB:                  sourceDB,
		targetDB:                  targetDB,
		collectionMap:             collectionMap,
		changeStream:              changeStream,
		ctx:                       ctx,
		log:                       log,
		resumeTokenPath:           resumeTokenPath,
		checkpointInterval:        checkpointInterval,
		saveThreshold:             saveThreshold,
		incrementalWriteBatchSize: incrementalWriteBatchSize,
		forceOrderedOperations:    forceOrderedOperations,
		flushInterval:             flushInterval,

		// Initialize statistics tracking
		statsInterval:        statsInterval,
		lastStatsTime:        time.Now(),
		eventsSinceLastStats: 0,
	}
}

// hashDocumentID creates a hash of the document ID
func hashDocumentID(docID interface{}) int {
	// Convert docID to a byte representation
	var bytes []byte

	switch id := docID.(type) {
	case string:
		bytes = []byte(id)
	case primitive.ObjectID:
		bytes = []byte(id.Hex())
	case int, int32, int64, float64, float32:
		bytes = []byte(fmt.Sprintf("%v", id))
	case bson.D:
		data, err := bson.Marshal(id)
		if err != nil {
			bytes = []byte(fmt.Sprintf("%v", id))
		} else {
			bytes = data
		}
	case bson.M:
		data, err := bson.Marshal(id)
		if err != nil {
			bytes = []byte(fmt.Sprintf("%v", id))
		} else {
			bytes = data
		}
	default:
		bytes = []byte(fmt.Sprintf("%v", id))
	}

	// Use FNV-1a hash algorithm
	h := fnv.New32a()
	h.Write(bytes)
	return int(h.Sum32())
}

// getWorkerIndex determines which worker should handle a document based on its ID
func (d *EventDistributor) getWorkerIndex(docID interface{}) int {
	hash := hashDocumentID(docID)
	// Ensure positive result
	return ((hash % d.incrementalWorkerCount) + d.incrementalWorkerCount) % d.incrementalWorkerCount
}

// waitForWorkersToFinish waits for all workers to finish processing
func (d *EventDistributor) waitForWorkersToFinish() {
	d.log.Info("Waiting for all workers to finish processing...")
	for _, worker := range d.workers {
		worker.WaitForCompletion()
	}
	d.log.Info("All workers have completed processing.")
}

// Start begins the event distribution process
func (d *EventDistributor) Start() error {
	// Initialize workers
	for i := 0; i < d.incrementalWorkerCount; i++ {
		d.workers[i] = NewWorker(i, d.ctx, d.log, d.targetDB, d.collectionMap, d.incrementalWriteBatchSize, d.forceOrderedOperations)
	}

	// Set up context cancellation handling
	go func() {
		<-d.ctx.Done()
		d.log.Info("Context canceled. Shutting down workers...")
		for _, worker := range d.workers {
			worker.Shutdown()
		}
	}()

	// Set up periodic flushing
	flushTicker := time.NewTicker(d.flushInterval)
	defer flushTicker.Stop()

	go func() {
		for {
			select {
			case <-flushTicker.C:
				// Check all workers for groups that need flushing
				for _, worker := range d.workers {
					worker.mu.Lock()
					if worker.currentGroup != nil && len(worker.currentGroup.Operations) > 0 {
						// If the group has been waiting for more than the flush interval
						if time.Since(worker.currentGroup.CreatedAt) >= d.flushInterval {
							d.log.Debugf("Flushing group in worker %d due to timeout: %s.%s with %d operations",
								worker.id, worker.currentGroup.Namespace, worker.currentGroup.OpType,
								len(worker.currentGroup.Operations))

							worker.flushCurrentGroup()
						}
					}
					worker.mu.Unlock()
				}
			case <-d.ctx.Done():
				return
			}
		}
	}()

	// Set up statistics reporting
	statsTicker := time.NewTicker(d.statsInterval)
	defer statsTicker.Stop()

	go func() {
		for {
			select {
			case <-statsTicker.C:
				// Calculate and log statistics
				d.statsMu.Lock()
				eventCount := d.eventsSinceLastStats
				duration := time.Since(d.lastStatsTime)
				d.eventsSinceLastStats = 0
				d.lastStatsTime = time.Now()
				d.statsMu.Unlock()

				if duration > 0 && eventCount > 0 {
					rate := float64(eventCount) / duration.Seconds()
					d.log.Infof("Change stream statistics: Processed %d events in the last %v (%.2f events/second)",
						eventCount, duration.Round(time.Second), rate)
				} else if eventCount > 0 {
					d.log.Infof("Change stream statistics: Processed %d events since last report", eventCount)
				} else {
					d.log.Info("Change stream statistics: No events processed since last report")
				}
			case <-d.ctx.Done():
				return
			}
		}
	}()

	// Main loop to read from change stream and distribute events
	var changeCount int
	lastCheckpointTime := time.Now()

	for {
		// Check for context cancellation before processing next event
		if d.ctx.Err() != nil {
			d.log.Info("Context canceled. Stopping event distribution...")
			// Wait for all workers to finish processing their current operations
			d.waitForWorkersToFinish()
			// Return nil instead of context.Canceled to avoid error logging
			return nil
		}

		// Try to get next change event
		ok := d.changeStream.Next(d.ctx)
		if !ok {
			// Check if this is due to an error or end of stream
			if err := d.changeStream.Err(); err != nil {
				// Check if the error is due to context cancellation
				if err == context.Canceled {
					d.log.Info("Change stream interrupted due to context cancellation")
				} else {
					d.log.Errorf("Change stream error: %v", err)
				}
				// Wait for all workers to finish processing their current operations
				d.waitForWorkersToFinish()
				// Don't propagate context.Canceled as an error
				if err == context.Canceled {
					return nil
				}
				return err
			}
			// End of stream, break out of the loop
			break
		}

		// Get change event
		var changeEvent bson.M
		if err := d.changeStream.Decode(&changeEvent); err != nil {
			d.log.Errorf("Error decoding change event: %v", err)
			continue
		}

		// Extract document ID for hashing
		documentKey, ok := changeEvent["documentKey"].(bson.M)
		if !ok {
			d.log.Errorf("Invalid document key in change event: %v", changeEvent)
			continue
		}

		docID := documentKey["_id"]

		// Determine worker based on hash modulo worker count
		workerIndex := d.getWorkerIndex(docID)

		// Send event to appropriate worker
		d.workers[workerIndex].ProcessEvent(changeEvent)

		// Update statistics counter
		d.statsMu.Lock()
		d.eventsSinceLastStats++
		d.statsMu.Unlock()

		// Handle resume token checkpointing
		changeCount++
		now := time.Now()
		timeBasedCheckpoint := now.Sub(lastCheckpointTime) >= d.checkpointInterval
		countBasedCheckpoint := changeCount >= d.saveThreshold

		if timeBasedCheckpoint || countBasedCheckpoint {
			d.saveResumeToken(d.changeStream.ResumeToken())

			// Reset counters
			lastCheckpointTime = now
			changeCount = 0
		}
	}

	// Wait for all workers to finish processing their current operations
	d.waitForWorkersToFinish()

	return nil
}

// saveResumeToken saves the current resume token
func (d *EventDistributor) saveResumeToken(resumeToken bson.Raw) {
	var resumeTokenDoc bson.M
	if err := bson.Unmarshal(resumeToken, &resumeTokenDoc); err != nil {
		d.log.Errorf("Error unmarshaling resume token: %v", err)
		return
	}

	if err := SaveResumeToken(d.resumeTokenPath, resumeTokenDoc); err != nil {
		d.log.Errorf("Error saving resume token: %v", err)
	} else {
		d.log.Infof("Saved resume token successfully")
	}
}

// WriteOperation represents a single write operation
type WriteOperation struct {
	DocumentID        interface{}
	Document          interface{}
	UpdateDescription interface{} // For modifier updates ($set, $inc, etc.)
	Namespace         string
	OpType            string
}

// OperationGroup represents a group of operations of the same type and namespace
type OperationGroup struct {
	Namespace  string
	OpType     string
	Operations []WriteOperation
	CreatedAt  time.Time // New field to track when the group was created
}

// Worker processes change events for a subset of documents
type Worker struct {
	id            int
	ctx           context.Context
	log           *logger.Logger
	targetDB      *db.MongoDB
	collectionMap map[string]map[string]string

	// Current group being built
	currentGroup *OperationGroup

	// Queue of groups waiting to be processed
	processingQueue []*OperationGroup

	// Flag to indicate if processing is in progress
	processing bool

	// Maximum group size
	incrementalWriteBatchSize int

	// Force ordered operations for all types
	forceOrderedOperations bool

	// For shutdown coordination
	wg sync.WaitGroup

	// Mutex to protect concurrent access
	mu sync.RWMutex

	// Flag to indicate if shutdown is in progress
	shutdownInProgress bool
}

// flushCurrentGroup moves the current group to the processing queue if it exists
func (w *Worker) flushCurrentGroup() bool {
	// Must be called with lock held
	if w.currentGroup != nil && len(w.currentGroup.Operations) > 0 {
		w.log.Debugf("Worker %d: Flushing group: %s.%s with %d operations",
			w.id, w.currentGroup.Namespace, w.currentGroup.OpType,
			len(w.currentGroup.Operations))

		w.processingQueue = append(w.processingQueue, w.currentGroup)
		w.currentGroup = nil

		// Start processing if not already in progress
		if !w.processing {
			w.processing = true
			go w.processGroups()
		}
		return true
	}
	return false
}

// NewWorker creates a new worker
func NewWorker(id int, ctx context.Context, log *logger.Logger,
	targetDB *db.MongoDB, collectionMap map[string]map[string]string,
	incrementalWriteBatchSize int, forceOrderedOperations bool) *Worker {

	return &Worker{
		id:                        id,
		ctx:                       ctx,
		log:                       log,
		targetDB:                  targetDB,
		collectionMap:             collectionMap,
		processingQueue:           make([]*OperationGroup, 0),
		incrementalWriteBatchSize: incrementalWriteBatchSize,
		forceOrderedOperations:    forceOrderedOperations,
	}
}

// ProcessEvent handles a single change event
func (w *Worker) ProcessEvent(event bson.M) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Extract operation details
	opType, _ := event["operationType"].(string)
	ns, _ := event["ns"].(bson.M)
	dbName, _ := ns["db"].(string)
	collName, _ := ns["coll"].(string)
	namespace := fmt.Sprintf("%s.%s", dbName, collName)

	documentKey, _ := event["documentKey"].(bson.M)
	docID := documentKey["_id"]

	// Get fullDocument as interface{} to support both bson.M and map[string]interface{}
	// This is needed because legacy oplog replicator returns map[string]interface{}
	fullDocument := event["fullDocument"]

	// Get updateDescription for modifier updates ($set, $inc, etc.)
	updateDescription := event["updateDescription"]

	// Debug log for worker events
	w.log.Debugf("Worker %d received event: type=%s, namespace=%s, docID=%v, hasFullDoc=%v, hasUpdateDesc=%v",
		w.id, opType, namespace, docID, fullDocument != nil, updateDescription != nil)

	// Create write operation
	op := WriteOperation{
		DocumentID:        docID,
		Document:          fullDocument,
		UpdateDescription: updateDescription,
		Namespace:         namespace,
		OpType:            opType,
	}

	// Check if we need to create a new group
	needNewGroup := w.currentGroup == nil ||
		w.currentGroup.OpType != opType ||
		w.currentGroup.Namespace != namespace ||
		len(w.currentGroup.Operations) >= w.incrementalWriteBatchSize

	if needNewGroup && w.currentGroup != nil {
		// Add current group to processing queue
		w.processingQueue = append(w.processingQueue, w.currentGroup)
		w.currentGroup = nil

		// Start processing if not already in progress
		if !w.processing {
			w.processing = true
			go w.processGroups()
		}
	}

	// Create a new group if needed
	if w.currentGroup == nil {
		w.currentGroup = &OperationGroup{
			Namespace:  namespace,
			OpType:     opType,
			Operations: []WriteOperation{op},
			CreatedAt:  time.Now(), // Set creation timestamp
		}
	} else {
		// Add to current group
		w.currentGroup.Operations = append(w.currentGroup.Operations, op)
	}

	// If current group has reached max size, add it to the queue
	if len(w.currentGroup.Operations) >= w.incrementalWriteBatchSize {
		w.processingQueue = append(w.processingQueue, w.currentGroup)
		w.currentGroup = nil

		// Start processing if not already in progress
		if !w.processing {
			w.processing = true
			go w.processGroups()
		}
	}
}

// processGroups processes groups in the queue sequentially
func (w *Worker) processGroups() {
	w.wg.Add(1)
	defer w.wg.Done()

	for {
		// Get the next group to process
		w.mu.Lock()
		if len(w.processingQueue) == 0 {
			// No more groups to process
			w.processing = false
			w.mu.Unlock()

			// If shutdown is in progress, log completion
			if w.shutdownInProgress {
				w.log.Debugf("Worker %d: Completed processing all groups during shutdown", w.id)
			}
			return
		}

		// Get the first group from the queue
		group := w.processingQueue[0]
		w.processingQueue = w.processingQueue[1:]
		w.mu.Unlock()

		// Process the group
		w.processGroup(*group)
	}
}

// processGroup processes a single operation group
func (w *Worker) processGroup(group OperationGroup) {
	w.log.Debugf("Worker %d: Processing group: %s.%s with %d operations",
		w.id, group.Namespace, group.OpType, len(group.Operations))

	// Get target collection
	parts := strings.Split(group.Namespace, ".")
	if len(parts) != 2 {
		w.log.Errorf("Invalid namespace format: %s", group.Namespace)
		return
	}
	dbName, collName := parts[0], parts[1]

	// Get mapped collection name
	targetCollName := w.getTargetCollectionName(dbName, collName)
	targetCollection := w.targetDB.GetCollection(targetCollName)

	// Determine if we should use ordered operations
	useOrdered := group.OpType == "update" || group.OpType == "replace" || w.forceOrderedOperations

	// Process based on operation type
	switch group.OpType {
	case "insert":
		var docs []interface{}
		for _, op := range group.Operations {
			// Transform __*__ field names to _*_ for Firestore compatibility
			docs = append(docs, TransformFieldNames(op.Document, w.log, dbName, collName, op.DocumentID))
		}

		if _, err := targetCollection.InsertMany(w.ctx, docs, options.InsertMany().SetOrdered(useOrdered)); err != nil {
			bulkWriteException, ok := err.(mongo.BulkWriteException)
			if ok {
				w.log.Debugf("Bulk insert partially failed: %d failed", len(bulkWriteException.WriteErrors))

				// Process individual errors
				for _, writeErr := range bulkWriteException.WriteErrors {
					w.log.Debugf("Insert error at index %d: %v", writeErr.Index, writeErr.Message)

					// Check if it's a duplicate key error (code 11000)
					if writeErr.Code == 11000 {
						// Use upsert for this document
						if writeErr.Index < len(group.Operations) {
							op := group.Operations[writeErr.Index]
							filter := bson.M{"_id": op.DocumentID}
							if _, err := targetCollection.ReplaceOne(w.ctx, filter, op.Document, options.Replace().SetUpsert(true)); err != nil {
								w.log.Debugf("Upsert fallback failed for document %v: %v", op.DocumentID, err)
							} else {
								w.log.Debugf("Successfully upserted document %v after duplicate key error", op.DocumentID)
							}
						}
					} else {
						// For non-duplicate key errors, retry with regular insert
						if writeErr.Index < len(docs) {
							if _, err := targetCollection.InsertOne(w.ctx, docs[writeErr.Index]); err != nil {
								w.log.Errorf("Retry insert failed: %v", err)
							}
						}
					}
				}
			} else {
				// Handle non-bulk write errors
				if err == context.Canceled {
					w.log.Debugf("Bulk insert canceled due to context cancellation")
				} else {
					w.log.Errorf("Error performing bulk insert: %v", err)
				}

				// Fall back to individual operations with upsert for all documents
				for _, op := range group.Operations {
					// Try insert first
					if _, err := targetCollection.InsertOne(w.ctx, op.Document); err != nil {
						// If insert fails, try upsert
						filter := bson.M{"_id": op.DocumentID}
						if _, err := targetCollection.ReplaceOne(w.ctx, filter, op.Document, options.Replace().SetUpsert(true)); err != nil {
							if err == context.Canceled {
								w.log.Debugf("Upserting document %v canceled due to context cancellation", op.DocumentID)
							} else {
								w.log.Errorf("Error upserting document %v: %v", op.DocumentID, err)
							}
						} else {
							w.log.Debugf("Successfully upserted document %v after insert failed", op.DocumentID)
						}
					}
				}
			}
		} else {
			w.log.Debugf("Bulk inserted %d documents", len(docs))
		}

	case "update":
		var models []mongo.WriteModel
		for _, op := range group.Operations {
			// Check if this is a modifier update (has updateDescription) or full replacement (has fullDocument)
			if op.UpdateDescription != nil {
				// Modifier update - use UpdateOne with update operators ($set, $inc, etc.)
				// Transform __*__ field names in update description for Firestore compatibility
				model := mongo.NewUpdateOneModel().
					SetFilter(bson.M{"_id": op.DocumentID}).
					SetUpdate(TransformFieldNames(op.UpdateDescription, w.log, dbName, collName, op.DocumentID)).
					SetUpsert(true)
				models = append(models, model)
			} else if op.Document != nil {
				// Full document replacement - use ReplaceOne
				// Transform __*__ field names to _*_ for Firestore compatibility
				model := mongo.NewReplaceOneModel().
					SetFilter(bson.M{"_id": op.DocumentID}).
					SetReplacement(TransformFieldNames(op.Document, w.log, dbName, collName, op.DocumentID)).
					SetUpsert(true)
				models = append(models, model)
			} else {
				w.log.Errorf("Update operation has neither updateDescription nor fullDocument for doc %v", op.DocumentID)
				continue
			}
		}

		if _, err := targetCollection.BulkWrite(w.ctx, models, options.BulkWrite().SetOrdered(useOrdered)); err != nil {
			bulkWriteException, ok := err.(mongo.BulkWriteException)
			if ok {
				w.log.Errorf("Bulk update partially failed: %d failed", len(bulkWriteException.WriteErrors))

				// Process individual errors
				for _, writeErr := range bulkWriteException.WriteErrors {
					w.log.Errorf("Update error at index %d: %v", writeErr.Index, writeErr.Message)

					// Retry the failed operation
					if writeErr.Index < len(group.Operations) {
						op := group.Operations[writeErr.Index]
						filter := bson.M{"_id": op.DocumentID}

						// Retry with the appropriate method based on operation type
						if op.UpdateDescription != nil {
							if _, err := targetCollection.UpdateOne(w.ctx, filter, op.UpdateDescription, options.Update().SetUpsert(true)); err != nil {
								w.log.Errorf("Retry modifier update failed: %v", err)
							}
						} else if op.Document != nil {
							if _, err := targetCollection.ReplaceOne(w.ctx, filter, op.Document, options.Replace().SetUpsert(true)); err != nil {
								w.log.Errorf("Retry replace update failed: %v", err)
							}
						}
					}
				}
			} else {
				// Handle non-bulk write errors
				if err == context.Canceled {
					w.log.Debugf("Bulk update canceled due to context cancellation")
				} else {
					w.log.Errorf("Error performing bulk update: %v", err)
				}

				// Fall back to individual updates
				for _, op := range group.Operations {
					filter := bson.M{"_id": op.DocumentID}

					// Use the appropriate method based on operation type
					if op.UpdateDescription != nil {
						if _, err := targetCollection.UpdateOne(w.ctx, filter, op.UpdateDescription, options.Update().SetUpsert(true)); err != nil {
							if err == context.Canceled {
								w.log.Debugf("Updating document %v (modifier) canceled due to context cancellation", op.DocumentID)
							} else {
								w.log.Errorf("Error updating document %v (modifier): %v", op.DocumentID, err)
							}
						}
					} else if op.Document != nil {
						if _, err := targetCollection.ReplaceOne(w.ctx, filter, op.Document, options.Replace().SetUpsert(true)); err != nil {
							if err == context.Canceled {
								w.log.Debugf("Replacing document %v canceled due to context cancellation", op.DocumentID)
							} else {
								w.log.Errorf("Error replacing document %v: %v", op.DocumentID, err)
							}
						}
					}
				}
			}
		} else {
			w.log.Debugf("Bulk updated %d documents", len(models))
		}

	case "delete":
		var models []mongo.WriteModel
		for _, op := range group.Operations {
			model := mongo.NewDeleteOneModel().
				SetFilter(bson.M{"_id": op.DocumentID})
			models = append(models, model)
		}

		if _, err := targetCollection.BulkWrite(w.ctx, models, options.BulkWrite().SetOrdered(useOrdered)); err != nil {
			bulkWriteException, ok := err.(mongo.BulkWriteException)
			if ok {
				w.log.Errorf("Bulk delete partially failed: %d failed", len(bulkWriteException.WriteErrors))

				// Process individual errors
				for _, writeErr := range bulkWriteException.WriteErrors {
					w.log.Errorf("Delete error at index %d: %v", writeErr.Index, writeErr.Message)

					// Retry the failed operation
					if writeErr.Index < len(group.Operations) {
						op := group.Operations[writeErr.Index]
						filter := bson.M{"_id": op.DocumentID}
						if _, err := targetCollection.DeleteOne(w.ctx, filter); err != nil {
							w.log.Errorf("Retry delete failed: %v", err)
						}
					}
				}
			} else {
				// Handle non-bulk write errors
				if err == context.Canceled {
					w.log.Debugf("Bulk delete canceled due to context cancellation")
				} else {
					w.log.Errorf("Error performing bulk delete: %v", err)
				}

				// Fall back to individual deletes
				for _, op := range group.Operations {
					filter := bson.M{"_id": op.DocumentID}
					if _, err := targetCollection.DeleteOne(w.ctx, filter); err != nil {
						if err == context.Canceled {
							w.log.Debugf("Deleting document %v canceled due to context cancellation", op.DocumentID)
						} else {
							w.log.Errorf("Error deleting document %v: %v", op.DocumentID, err)
						}
					}
				}
			}
		} else {
			w.log.Debugf("Bulk deleted %d documents", len(models))
		}

	case "replace":
		var models []mongo.WriteModel
		for _, op := range group.Operations {
			// Transform __*__ field names to _*_ for Firestore compatibility
			model := mongo.NewReplaceOneModel().
				SetFilter(bson.M{"_id": op.DocumentID}).
				SetReplacement(TransformFieldNames(op.Document, w.log, dbName, collName, op.DocumentID)).
				SetUpsert(true)
			models = append(models, model)
		}

		if _, err := targetCollection.BulkWrite(w.ctx, models, options.BulkWrite().SetOrdered(useOrdered)); err != nil {
			bulkWriteException, ok := err.(mongo.BulkWriteException)
			if ok {
				w.log.Errorf("Bulk replace partially failed: %d failed", len(bulkWriteException.WriteErrors))

				// Process individual errors
				for _, writeErr := range bulkWriteException.WriteErrors {
					w.log.Errorf("Replace error at index %d: %v", writeErr.Index, writeErr.Message)

					// Retry the failed operation
					if writeErr.Index < len(group.Operations) {
						op := group.Operations[writeErr.Index]
						filter := bson.M{"_id": op.DocumentID}
						if _, err := targetCollection.ReplaceOne(w.ctx, filter, op.Document, options.Replace().SetUpsert(true)); err != nil {
							w.log.Errorf("Retry replace failed: %v", err)
						}
					}
				}
			} else {
				// Handle non-bulk write errors
				if err == context.Canceled {
					w.log.Debugf("Bulk replace canceled due to context cancellation")
				} else {
					w.log.Errorf("Error performing bulk replace: %v", err)
				}

				// Fall back to individual replaces
				for _, op := range group.Operations {
					filter := bson.M{"_id": op.DocumentID}
					if _, err := targetCollection.ReplaceOne(w.ctx, filter, op.Document, options.Replace().SetUpsert(true)); err != nil {
						if err == context.Canceled {
							w.log.Debugf("Replacing document %v canceled due to context cancellation", op.DocumentID)
						} else {
							w.log.Errorf("Error replacing document %v: %v", op.DocumentID, err)
						}
					}
				}
			}
		} else {
			w.log.Debugf("Bulk replaced %d documents", len(models))
		}
	}
}

// getTargetCollectionName gets the target collection name for a source collection
func (w *Worker) getTargetCollectionName(dbName, collName string) string {
	// Check if we have a mapping for this collection
	if w.collectionMap[dbName] != nil {
		if targetColl, exists := w.collectionMap[dbName][collName]; exists {
			return targetColl
		}
	}

	// If no mapping exists, use the same name
	return collName
}

// WaitForCompletion waits for all processing to complete
func (w *Worker) WaitForCompletion() {
	w.wg.Wait()
	w.log.Debugf("Worker %d: All operations processed successfully", w.id)
}

// Shutdown ensures any pending operations are processed
func (w *Worker) Shutdown() {
	w.mu.Lock()
	w.shutdownInProgress = true

	// Add current group to processing queue if it exists
	if w.currentGroup != nil && len(w.currentGroup.Operations) > 0 {
		w.log.Debugf("Worker %d: Flushing current group during shutdown: %s.%s with %d operations",
			w.id, w.currentGroup.Namespace, w.currentGroup.OpType, len(w.currentGroup.Operations))

		w.flushCurrentGroup()
	}

	w.mu.Unlock()

	// Wait for any ongoing processing to complete
	w.WaitForCompletion()
}
