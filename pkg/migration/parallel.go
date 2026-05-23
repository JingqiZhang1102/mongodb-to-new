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
	dlq                       DLQ           // Dead Letter Queue for failed documents
	retryManager              *RetryManager // Retry manager for transient errors

	// Statistics tracking
	statsManager *StatsManager // Manager for statistics and replication lag
	cfg          *config.Config
}

// NewEventDistributor creates a new event distributor
func NewEventDistributor(ctx context.Context, sourceDB, targetDB *db.MongoDB,
	collectionMap map[string]map[string]string,
	changeStream *mongo.ChangeStream, log *logger.Logger,
	resumeTokenPath string, checkpointInterval time.Duration,
	saveThreshold, incrementalWorkerCount, incrementalWriteBatchSize int,
	forceOrderedOperations bool, flushInterval time.Duration, cfg *config.Config, dlq DLQ) *EventDistributor {

	// Use stats interval from config
	statsInterval := time.Duration(cfg.StatsIntervalMinutes) * time.Minute

	// Create retry manager from config
	retryMgr := NewRetryManagerFromConfig(cfg, log)

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
		dlq:                       dlq,
		retryManager:              retryMgr,
		cfg:                       cfg,

		// Initialize statistics tracking
		statsManager: NewStatsManager(log, statsInterval, cfg.GroupOpsByDistinctId),
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
	d.log.Infof("Starting event distributor with %d workers (GroupOpsByDistinctId: %t)", d.incrementalWorkerCount, d.cfg.GroupOpsByDistinctId)

	// Start the statistics manager periodic reporting
	if d.statsManager != nil {
		d.statsManager.Start(d.ctx)
	}

	// Initialize workers with retry manager and stats manager
	for i := 0; i < d.incrementalWorkerCount; i++ {
		d.workers[i] = NewWorker(i, d.ctx, d.log, d.targetDB, d.collectionMap, d.incrementalWriteBatchSize, d.forceOrderedOperations, d.dlq, d.retryManager, d.statsManager, d.cfg.GroupOpsByDistinctId, d.flushInterval, d.cfg.IncrementalIncomingQueueSize, d.cfg.IncrementalProcessingQueueSize)
	}

	// Set up context cancellation handling
	go func() {
		<-d.ctx.Done()
		d.log.Info("Context canceled. Shutting down workers...")
		for _, worker := range d.workers {
			worker.Shutdown()
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

		// Get raw change event bytes
		rawEvent := d.changeStream.Current

		// Extract operationType via fast binary lookup
		opTypeVal, err := rawEvent.LookupErr("operationType")
		if err != nil {
			d.log.Errorf("Invalid raw change event: missing operationType")
			continue
		}
		opType := opTypeVal.StringValue()

		if d.statsManager != nil {
			d.statsManager.IncrementEventsReceived(opType)
		}

		// Extract documentKey._id via fast binary lookup
		docKeyVal, err := rawEvent.LookupErr("documentKey")
		if err != nil {
			d.log.Errorf("Invalid raw change event: missing documentKey")
			continue
		}
		docKeyRaw := docKeyVal.Document()
		docIDVal, err := docKeyRaw.LookupErr("_id")
		if err != nil {
			d.log.Errorf("Invalid raw change event: missing documentKey._id")
			continue
		}

		var docID interface{}
		if err := docIDVal.Unmarshal(&docID); err != nil {
			d.log.Errorf("Error unmarshaling documentKey._id from raw BSON: %v", err)
			continue
		}

		// Determine worker based on hash modulo worker count
		workerIndex := d.getWorkerIndex(docID)

		// Safe concurrent deep copy of raw BSON bytes to prevent mutation races
		rawCopy := make(bson.Raw, len(rawEvent))
		copy(rawCopy, rawEvent)

		// Send raw event copy to appropriate worker concurrently
		select {
		case d.workers[workerIndex].incomingQueue <- rawCopy:
		case <-d.ctx.Done():
			return nil
		}

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
	EventTime         time.Time
	ReceiveTime       time.Time
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
	statsManager  *StatsManager

	// Queue of raw change events waiting to be partitioned and batched concurrently
	incomingQueue chan interface{}

	// Current group being built
	currentGroup *OperationGroup

	// Queue of groups waiting to be processed
	processingQueue chan *OperationGroup

	// Maximum group size
	incrementalWriteBatchSize int

	// Force ordered operations for all types
	forceOrderedOperations bool

	// Dead Letter Queue for failed documents
	dlq DLQ

	// Retry manager for transient error handling
	retryManager *RetryManager

	// For shutdown coordination
	wg sync.WaitGroup

	// Mutex to protect concurrent access
	mu sync.RWMutex

	// Flag to indicate if shutdown is in progress
	shutdownInProgress bool

	// Dynamic grouping configurations
	groupOpsByDistinctId bool
	currentGroupIDs     map[int]bool
	flushInterval       time.Duration
}

// flushCurrentGroup moves the current group to the processing queue if it exists
func (w *Worker) flushCurrentGroup() bool {
	// Must be called with lock held
	if w.currentGroup != nil && len(w.currentGroup.Operations) > 0 {
		w.log.Debugf("Worker %d: Flushing group: %s.%s with %d operations",
			w.id, w.currentGroup.Namespace, w.currentGroup.OpType,
			len(w.currentGroup.Operations))

		w.processingQueue <- w.currentGroup
		w.currentGroup = nil
		return true
	}
	return false
}

type statsTrackingDLQ struct {
	underlyingDlq DLQ
	statsManager  *StatsManager
}

func (s *statsTrackingDLQ) WriteFailed(sourceDB, sourceCollection string, documentID interface{}, err error, phase, opType string, document interface{}) {
	s.underlyingDlq.WriteFailed(sourceDB, sourceCollection, documentID, err, phase, opType, document)
	if s.statsManager != nil {
		s.statsManager.IncrementEventsFailed(opType)
	}
}

func (s *statsTrackingDLQ) Count() int64 {
	return s.underlyingDlq.Count()
}

func (s *statsTrackingDLQ) Close() {
	s.underlyingDlq.Close()
}

// NewWorker creates a new worker
func NewWorker(id int, ctx context.Context, log *logger.Logger,
	targetDB *db.MongoDB, collectionMap map[string]map[string]string,
	incrementalWriteBatchSize int, forceOrderedOperations bool, dlq DLQ, retryManager *RetryManager, statsManager *StatsManager, groupOpsByDistinctId bool, flushInterval time.Duration,
	incomingQueueSize int, processingQueueSize int) *Worker {

	var workerDLQ DLQ = dlq
	if dlq != nil && statsManager != nil {
		workerDLQ = &statsTrackingDLQ{
			underlyingDlq: dlq,
			statsManager:  statsManager,
		}
	}

	w := &Worker{
		id:                        id,
		ctx:                       ctx,
		log:                       log,
		targetDB:                  targetDB,
		collectionMap:             collectionMap,
		incomingQueue:             make(chan interface{}, incomingQueueSize),
		processingQueue:           make(chan *OperationGroup, processingQueueSize),
		incrementalWriteBatchSize: incrementalWriteBatchSize,
		forceOrderedOperations:    forceOrderedOperations,
		dlq:                       workerDLQ,
		retryManager:              retryManager,
		statsManager:              statsManager,
		groupOpsByDistinctId:      groupOpsByDistinctId,
		currentGroupIDs:           make(map[int]bool),
		flushInterval:             flushInterval,
	}

	// Statically spawn the worker and eventLoop threads at startup
	go w.processGroups()
	go w.eventLoop()

	return w
}

// eventLoop concurrently drains raw change events from the incomingQueue, partitioning and batching them lock-freely
func (w *Worker) eventLoop() {
	w.wg.Add(1)
	defer w.wg.Done()

	// Local worker-level ticker to flush groups that time out
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case event, ok := <-w.incomingQueue:
			if !ok {
				// Queue closed, flush any remaining operations
				w.mu.Lock()
				w.flushCurrentGroup()
				close(w.processingQueue) // Signal processGroups to drain and exit
				w.mu.Unlock()
				return
			}
			w.ProcessEvent(event)

		case <-ticker.C:
			w.mu.Lock()
			if w.currentGroup != nil && len(w.currentGroup.Operations) > 0 {
				if time.Since(w.currentGroup.CreatedAt) >= w.flushInterval {
					if w.statsManager != nil {
						w.statsManager.IncrementTimeoutFlushes()
					}
					w.flushCurrentGroup()
				}
			}
			w.mu.Unlock()

		case <-w.ctx.Done():
			// Context canceled, flush remaining and shut down
			w.mu.Lock()
			w.flushCurrentGroup()
			close(w.processingQueue)
			w.mu.Unlock()
			return
		}
	}
}

// ProcessEvent handles a single change event by decoding raw BSON concurrently in the worker thread
func (w *Worker) ProcessEvent(eventArg interface{}) {
	w.mu.Lock()
	defer w.mu.Unlock()

	var event bson.M
	switch e := eventArg.(type) {
	case bson.M:
		event = e
	case bson.Raw:
		if err := bson.Unmarshal(e, &event); err != nil {
			w.log.Errorf("Worker %d: Failed to unmarshal raw BSON change event: %v", w.id, err)
			return
		}
	default:
		w.log.Errorf("Worker %d: Invalid event type in ProcessEvent: %T", w.id, eventArg)
		return
	}

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

	// If this is an update operation in the change stream path and fullDocument is nil, skip it and record metric
	if opType == "update" && fullDocument == nil && w.statsManager != nil {
		w.statsManager.IncrementUpdatedThenDeleted(w.id)
		return
	}

	// Get updateDescription for modifier updates ($set, $inc, etc.)
	var updateDescription interface{}

	// Extract clusterTime or wallTime for lag tracking
	eventTime := ExtractEventTime(event)

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
		EventTime:         eventTime,
		ReceiveTime:       time.Now(),
	}

	docHash := hashDocumentID(docID)

	// Check if we need to create a new group
	var needNewGroup bool
	if w.groupOpsByDistinctId {
		needNewGroup = w.currentGroup == nil ||
			w.currentGroup.Namespace != namespace ||
			len(w.currentGroup.Operations) >= w.incrementalWriteBatchSize ||
			w.currentGroupIDs[docHash]
	} else {
		needNewGroup = w.currentGroup == nil ||
			w.currentGroup.OpType != opType ||
			w.currentGroup.Namespace != namespace ||
			len(w.currentGroup.Operations) >= w.incrementalWriteBatchSize
	}

	if needNewGroup && w.currentGroup != nil {
		if w.statsManager != nil {
			// Determine the reason the group had to be flushed
			if w.groupOpsByDistinctId && w.currentGroupIDs[docHash] {
				w.statsManager.IncrementGroupFlushReason("collision")
			} else if w.currentGroup.Namespace != namespace {
				w.statsManager.IncrementGroupFlushReason("namespace")
			} else if len(w.currentGroup.Operations) >= w.incrementalWriteBatchSize {
				w.statsManager.IncrementGroupFlushReason("batchfull")
			} else if w.currentGroup.OpType != opType {
				w.statsManager.IncrementGroupFlushReason("optype")
			}
		}

		// Add current group to processing queue
		w.processingQueue <- w.currentGroup
		w.currentGroup = nil
		w.currentGroupIDs = make(map[int]bool)
	}

	// Create a new group if needed
	if w.currentGroup == nil {
		groupOpType := opType
		if w.groupOpsByDistinctId {
			groupOpType = "mixed"
		}
		w.currentGroup = &OperationGroup{
			Namespace:  namespace,
			OpType:     groupOpType,
			Operations: []WriteOperation{op},
			CreatedAt:  time.Now(), // Set creation timestamp
		}
		w.currentGroupIDs = make(map[int]bool)
		w.currentGroupIDs[docHash] = true
	} else {
		// Add to current group
		w.currentGroup.Operations = append(w.currentGroup.Operations, op)
		w.currentGroupIDs[docHash] = true
	}

	// If current group has reached max size, add it to the queue
	if len(w.currentGroup.Operations) >= w.incrementalWriteBatchSize {
		if w.statsManager != nil {
			w.statsManager.IncrementGroupFlushReason("batchfull")
		}
		w.processingQueue <- w.currentGroup
		w.currentGroup = nil
	}
}

// processGroups processes groups in the queue sequentially
func (w *Worker) processGroups() {
	w.wg.Add(1)
	defer w.wg.Done()

	for {
		select {
		case group, ok := <-w.processingQueue:
			if !ok {
				// Channel closed, shutdown worker
				if w.shutdownInProgress {
					w.log.Debugf("Worker %d: Completed processing all groups during shutdown", w.id)
				}
				return
			}
			// Process the group
			w.processGroup(*group)
		case <-w.ctx.Done():
			return
		}
	}
}

// processGroup processes a single operation group
func (w *Worker) processGroup(group OperationGroup) {
	w.log.Debugf("Worker %d: Processing group: %s.%s with %d operations",
		w.id, group.Namespace, group.OpType, len(group.Operations))

	// Get target collection
	parts := strings.SplitN(group.Namespace, ".", 2)
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

	if w.statsManager != nil {
		w.statsManager.RecordBulkWrite(len(group.Operations), useOrdered)
	}

	// Process based on operation type
	switch group.OpType {
	case "mixed":
		var models []mongo.WriteModel
		for _, op := range group.Operations {
			switch op.OpType {
			case "insert":
				transformed, err := TransformFieldNames(op.Document, w.log, dbName, collName, op.DocumentID)
				if err != nil {
					w.log.Errorf("[%s.%s] Field name transformation failed for mixed insert, document _id=%v: %v", dbName, collName, op.DocumentID, err)
					if w.dlq != nil {
						w.dlq.WriteFailed(dbName, collName, op.DocumentID, err, "incremental", "insert", op.Document)
					}
					continue
				}
				model := mongo.NewInsertOneModel().SetDocument(transformed)
				models = append(models, model)

			case "update":
				if op.Document != nil {
					transformed, err := TransformFieldNames(op.Document, w.log, dbName, collName, op.DocumentID)
					if err != nil {
						w.log.Errorf("[%s.%s] Field name transformation failed for mixed update replacement, document _id=%v: %v", dbName, collName, op.DocumentID, err)
						if w.dlq != nil {
							w.dlq.WriteFailed(dbName, collName, op.DocumentID, err, "incremental", "update", op.Document)
						}
						continue
					}
					model := mongo.NewReplaceOneModel().
						SetFilter(bson.M{"_id": op.DocumentID}).
						SetReplacement(transformed).
						SetUpsert(true)
					models = append(models, model)
				} else {
					w.log.Errorf("[%s.%s] Mixed update failed: document payload is nil for _id=%v", dbName, collName, op.DocumentID)
				}

			case "replace":
				transformed, err := TransformFieldNames(op.Document, w.log, dbName, collName, op.DocumentID)
				if err != nil {
					w.log.Errorf("[%s.%s] Field name transformation failed for mixed replace, document _id=%v: %v", dbName, collName, op.DocumentID, err)
					if w.dlq != nil {
						w.dlq.WriteFailed(dbName, collName, op.DocumentID, err, "incremental", "replace", op.Document)
					}
					continue
				}
				model := mongo.NewReplaceOneModel().
					SetFilter(bson.M{"_id": op.DocumentID}).
					SetReplacement(transformed).
					SetUpsert(true)
				models = append(models, model)

			case "delete":
				model := mongo.NewDeleteOneModel().SetFilter(bson.M{"_id": op.DocumentID})
				models = append(models, model)
			}
		}

		if len(models) > 0 {
			issueTime := time.Now()
			_, err := targetCollection.BulkWrite(w.ctx, models, options.BulkWrite().SetOrdered(true))
			if w.statsManager != nil {
				w.statsManager.RecordLatency("mixed", group.Operations, issueTime, time.Since(issueTime), w.id)
			}
			if err != nil {
				bulkWriteException, ok := err.(mongo.BulkWriteException)
				if ok && w.ctx.Err() != context.Canceled {
					w.log.Errorf("[%s.%s] Bulk mixed operations partially failed: %d failed", dbName, collName, len(bulkWriteException.WriteErrors))

					// Handle individual failures by fallback retries
					for _, writeErr := range bulkWriteException.WriteErrors {
						var failedDocID interface{}
						var op WriteOperation
						if writeErr.Index < len(group.Operations) {
							op = group.Operations[writeErr.Index]
							failedDocID = op.DocumentID
						}

						isDupInsert := op.OpType == "insert" && isDuplicateKeyError(writeErr.Code, writeErr.Message)
						if isDupInsert {
							w.log.Debugf("[%s.%s] Mixed insert had duplicate key occurrence for _id=%v, gracefully falling back to upsert", dbName, collName, failedDocID)
						} else {
							w.log.Errorf("[%s.%s] Mixed write error at index %d (opType=%s), _id=%v: %v", dbName, collName, writeErr.Index, op.OpType, failedDocID, writeErr.Message)
						}

						// Fallback retry for the single failed operation
						if writeErr.Index < len(group.Operations) {
							w.retryIndividualOperation(targetCollection, op, dbName, collName, writeErr.Message)
						}
					}
				} else {
					if err == context.Canceled || w.ctx.Err() == context.Canceled {
						w.log.Debugf("[%s.%s] Bulk mixed operations canceled due to context cancellation", dbName, collName)
					} else {
						w.log.Errorf("[%s.%s] Error performing bulk mixed operations: %v", dbName, collName, err)

						// For transient errors, retry the bulk operation with backoff before falling back to DLQ
						bulkRetrySucceeded := false
						if w.retryManager != nil {
							errType := w.retryManager.ClassifyError(err)
							if errType == ErrorTypeConnection || errType == ErrorTypeContention {
								w.log.Infof("[%s.%s] Transient error detected. Retrying bulk mixed operations with backoff...", dbName, collName)
								retryErr := w.retryManager.RetryWithBackoff(w.ctx, func() error {
									_, retryBulkErr := targetCollection.BulkWrite(w.ctx, models, options.BulkWrite().SetOrdered(true))
									return retryBulkErr
								})
								if retryErr == nil {
									w.log.Infof("[%s.%s] Bulk mixed operations succeeded after retry", dbName, collName)
									bulkRetrySucceeded = true
								} else {
									w.log.Warnf("[%s.%s] Bulk mixed operations still failed after retries: %v. Routing to DLQ.", dbName, collName, retryErr)
									err = retryErr
								}
							}
						}

						if !bulkRetrySucceeded {
							// Fall back to individual operations one-by-one to isolate errors and prevent DLQ victim colocation!
							w.log.Warnf("[%s.%s] Bulk mixed operations failed: %v. Falling back to individual operations one-by-one.", dbName, collName, err)
							for _, op := range group.Operations {
								w.retryIndividualOperation(targetCollection, op, dbName, collName, err.Error())
							}
						}
					}
				}
			}
		}

	case "insert":
		var docs []interface{}
		for _, op := range group.Operations {
			// Transform __*__ field names to _*_ for Firestore compatibility
			transformed, err := TransformFieldNames(op.Document, w.log, dbName, collName, op.DocumentID)
			if err != nil {
				w.log.Errorf("[%s.%s] Field name transformation failed for insert operation, document _id=%v: %v", dbName, collName, op.DocumentID, err)
				if w.dlq != nil {
					w.dlq.WriteFailed(dbName, collName, op.DocumentID, err, "incremental", "insert", op.Document)
				}
				continue
			}
			docs = append(docs, transformed)
		}

		issueTime := time.Now()
		_, err := targetCollection.InsertMany(w.ctx, docs, options.InsertMany().SetOrdered(useOrdered))
		if w.statsManager != nil {
			w.statsManager.RecordLatency("insert", group.Operations, issueTime, time.Since(issueTime), w.id)
		}
		if err != nil {
			bulkWriteException, ok := err.(mongo.BulkWriteException)
			if ok && w.ctx.Err() != context.Canceled {
				// Check if all write errors are simply duplicate key errors (gracefully handled by upsert fallback)
				hasRealErrors := false
				// Check if it's a duplicate key error
				for _, writeErr := range bulkWriteException.WriteErrors {
					if !isDuplicateKeyError(writeErr.Code, writeErr.Message) {
						hasRealErrors = true
						break
					}
				}
				if hasRealErrors {
					w.log.Errorf("[%s.%s] Bulk insert partially failed: %d failed", dbName, collName, len(bulkWriteException.WriteErrors))
				} else {
					w.log.Debugf("[%s.%s] Bulk insert had %d duplicate key occurrences (gracefully falling back to upserts)", dbName, collName, len(bulkWriteException.WriteErrors))
				}

				// Process individual errors
				for _, writeErr := range bulkWriteException.WriteErrors {
					var failedDocID interface{}
					var op WriteOperation
					if writeErr.Index < len(group.Operations) {
						op = group.Operations[writeErr.Index]
						failedDocID = op.DocumentID
					}
					w.log.Debugf("[%s.%s] Insert error at index %d, _id=%v: %v", dbName, collName, writeErr.Index, failedDocID, writeErr.Message)

					if writeErr.Index < len(group.Operations) {
						w.retryIndividualOperation(targetCollection, op, dbName, collName, writeErr.Message)
					}
				}
			} else {
				// Handle non-bulk write errors (e.g., broken pipe, connection reset, deadline exceeded)
				if err == context.Canceled || w.ctx.Err() == context.Canceled {
					w.log.Debugf("[%s.%s] Bulk insert canceled due to context cancellation", dbName, collName)
				} else {
					w.log.Errorf("[%s.%s] Error performing bulk insert: %v", dbName, collName, err)
				}

				// For transient errors, retry the bulk operation with backoff before falling back
				bulkRetrySucceeded := false
				if w.retryManager != nil && err != context.Canceled {
					errType := w.retryManager.ClassifyError(err)
					if errType == ErrorTypeConnection || errType == ErrorTypeContention {
						w.log.Infof("[%s.%s] Transient error detected. Retrying bulk insert with backoff...", dbName, collName)
						retryErr := w.retryManager.RetryWithBackoff(w.ctx, func() error {
							_, retryInsertErr := targetCollection.InsertMany(w.ctx, docs, options.InsertMany().SetOrdered(useOrdered))
							return retryInsertErr
						})
						if retryErr == nil {
							w.log.Infof("[%s.%s] Bulk insert succeeded after retry", dbName, collName)
							bulkRetrySucceeded = true
						} else {
							w.log.Warnf("[%s.%s] Bulk insert still failed after retries: %v. Falling back to individual operations.", dbName, collName, retryErr)
						}
					}
				}

				if !bulkRetrySucceeded {
					// Fall back to individual operations one-by-one to isolate errors and prevent DLQ victim colocation!
					w.log.Warnf("[%s.%s] Bulk insert failed: %v. Falling back to individual operations one-by-one.", dbName, collName, err)
					for _, op := range group.Operations {
						w.retryIndividualOperation(targetCollection, op, dbName, collName, err.Error())
					}
				}
			}
		} else {
			w.log.Debugf("[%s.%s] Bulk inserted %d documents", dbName, collName, len(docs))
		}

	case "update":
		var models []mongo.WriteModel
		for _, op := range group.Operations {
			// Unify all incremental updates into full ReplaceOne replacement upserts using op.Document
			if op.Document != nil {
				// Full document replacement - use ReplaceOne
				// Transform __*__ field names to _*_ for Firestore compatibility
				transformed, err := TransformFieldNames(op.Document, w.log, dbName, collName, op.DocumentID)
				if err != nil {
					w.log.Errorf("[%s.%s] Field name transformation failed for update replacement operation, document _id=%v: %v", dbName, collName, op.DocumentID, err)
					if w.dlq != nil {
						w.dlq.WriteFailed(dbName, collName, op.DocumentID, err, "incremental", "update", op.Document)
					}
					continue
				}
				model := mongo.NewReplaceOneModel().
					SetFilter(bson.M{"_id": op.DocumentID}).
					SetReplacement(transformed).
					SetUpsert(true)
				models = append(models, model)
			} else {
				w.log.Errorf("[%s.%s] Update operation failed: document payload is nil for _id=%v", dbName, collName, op.DocumentID)
				continue
			}
		}

		issueTime := time.Now()
		_, err := targetCollection.BulkWrite(w.ctx, models, options.BulkWrite().SetOrdered(useOrdered))
		if w.statsManager != nil {
			w.statsManager.RecordLatency("update", group.Operations, issueTime, time.Since(issueTime), w.id)
		}
		if err != nil {
			bulkWriteException, ok := err.(mongo.BulkWriteException)
			if ok && w.ctx.Err() != context.Canceled {
				w.log.Errorf("[%s.%s] Bulk update partially failed: %d failed", dbName, collName, len(bulkWriteException.WriteErrors))

				// Process individual errors
				for _, writeErr := range bulkWriteException.WriteErrors {
					var failedDocID interface{}
					var op WriteOperation
					if writeErr.Index < len(group.Operations) {
						op = group.Operations[writeErr.Index]
						failedDocID = op.DocumentID
					}
					w.log.Errorf("[%s.%s] Update error at index %d, _id=%v: %v", dbName, collName, writeErr.Index, failedDocID, writeErr.Message)

					if writeErr.Index < len(group.Operations) {
						w.retryIndividualOperation(targetCollection, op, dbName, collName, writeErr.Message)
					}
				}
			} else {
				// Handle non-bulk write errors
				if err == context.Canceled || w.ctx.Err() == context.Canceled {
					w.log.Debugf("[%s.%s] Bulk update canceled due to context cancellation", dbName, collName)
				} else {
					w.log.Errorf("[%s.%s] Error performing bulk update: %v", dbName, collName, err)
				}

				// For transient errors, retry the bulk operation with backoff before falling back
				bulkRetrySucceeded := false
				if w.retryManager != nil && err != context.Canceled {
					errType := w.retryManager.ClassifyError(err)
					if errType == ErrorTypeConnection || errType == ErrorTypeContention {
						w.log.Infof("[%s.%s] Transient error detected. Retrying bulk update with backoff...", dbName, collName)
						retryErr := w.retryManager.RetryWithBackoff(w.ctx, func() error {
							_, retryBulkErr := targetCollection.BulkWrite(w.ctx, models, options.BulkWrite().SetOrdered(useOrdered))
							return retryBulkErr
						})
						if retryErr == nil {
							w.log.Infof("[%s.%s] Bulk update succeeded after retry", dbName, collName)
							bulkRetrySucceeded = true
						} else {
							w.log.Warnf("[%s.%s] Bulk update still failed after retries: %v. Falling back to individual operations.", dbName, collName, retryErr)
						}
					}
				}

				if bulkRetrySucceeded {
					break
				}

				// Fall back to individual updates
				for _, op := range group.Operations {
					w.retryIndividualOperation(targetCollection, op, dbName, collName, err.Error())
				}
			}
		} else {
			w.log.Debugf("[%s.%s] Bulk updated %d documents", dbName, collName, len(models))
		}

	case "delete":
		var models []mongo.WriteModel
		for _, op := range group.Operations {
			model := mongo.NewDeleteOneModel().
				SetFilter(bson.M{"_id": op.DocumentID})
			models = append(models, model)
		}

		issueTime := time.Now()
		_, err := targetCollection.BulkWrite(w.ctx, models, options.BulkWrite().SetOrdered(useOrdered))
		if w.statsManager != nil {
			w.statsManager.RecordLatency("delete", group.Operations, issueTime, time.Since(issueTime), w.id)
		}
		if err != nil {
			bulkWriteException, ok := err.(mongo.BulkWriteException)
			if ok && w.ctx.Err() != context.Canceled {
				w.log.Errorf("[%s.%s] Bulk delete partially failed: %d failed", dbName, collName, len(bulkWriteException.WriteErrors))

				// Process individual errors
				for _, writeErr := range bulkWriteException.WriteErrors {
					var failedDocID interface{}
					var op WriteOperation
					if writeErr.Index < len(group.Operations) {
						op = group.Operations[writeErr.Index]
						failedDocID = op.DocumentID
					}
					w.log.Errorf("[%s.%s] Delete error at index %d, _id=%v: %v", dbName, collName, writeErr.Index, failedDocID, writeErr.Message)

					if writeErr.Index < len(group.Operations) {
						w.retryIndividualOperation(targetCollection, op, dbName, collName, writeErr.Message)
					}
				}
			} else {
				// Handle non-bulk write errors
				if err == context.Canceled || w.ctx.Err() == context.Canceled {
					w.log.Debugf("[%s.%s] Bulk delete canceled due to context cancellation", dbName, collName)
				} else {
					w.log.Errorf("[%s.%s] Error performing bulk delete: %v", dbName, collName, err)
				}

				// For transient errors, retry the bulk operation with backoff before falling back
				bulkRetrySucceeded := false
				if w.retryManager != nil && err != context.Canceled {
					errType := w.retryManager.ClassifyError(err)
					if errType == ErrorTypeConnection || errType == ErrorTypeContention {
						w.log.Infof("[%s.%s] Transient error detected. Retrying bulk delete with backoff...", dbName, collName)
						retryErr := w.retryManager.RetryWithBackoff(w.ctx, func() error {
							_, retryBulkErr := targetCollection.BulkWrite(w.ctx, models, options.BulkWrite().SetOrdered(useOrdered))
							return retryBulkErr
						})
						if retryErr == nil {
							w.log.Infof("[%s.%s] Bulk delete succeeded after retry", dbName, collName)
							bulkRetrySucceeded = true
						} else {
							w.log.Warnf("[%s.%s] Bulk delete still failed after retries: %v. Falling back to individual operations.", dbName, collName, retryErr)
						}
					}
				}

				if bulkRetrySucceeded {
					break
				}

				// Fall back to individual deletes
				for _, op := range group.Operations {
					w.retryIndividualOperation(targetCollection, op, dbName, collName, err.Error())
				}
			}
		} else {
			w.log.Debugf("[%s.%s] Bulk deleted %d documents", dbName, collName, len(models))
		}

	case "replace":
		var models []mongo.WriteModel
		for _, op := range group.Operations {
			// Transform __*__ field names to _*_ for Firestore compatibility
			transformed, err := TransformFieldNames(op.Document, w.log, dbName, collName, op.DocumentID)
			if err != nil {
				w.log.Errorf("[%s.%s] Field name transformation failed for replace operation, document _id=%v: %v", dbName, collName, op.DocumentID, err)
				if w.dlq != nil {
					w.dlq.WriteFailed(dbName, collName, op.DocumentID, err, "incremental", "replace", op.Document)
				}
				continue
			}
			model := mongo.NewReplaceOneModel().
				SetFilter(bson.M{"_id": op.DocumentID}).
				SetReplacement(transformed).
				SetUpsert(true)
			models = append(models, model)
		}

		issueTime := time.Now()
		_, err := targetCollection.BulkWrite(w.ctx, models, options.BulkWrite().SetOrdered(useOrdered))
		if w.statsManager != nil {
			w.statsManager.RecordLatency("replace", group.Operations, issueTime, time.Since(issueTime), w.id)
		}
		if err != nil {
			bulkWriteException, ok := err.(mongo.BulkWriteException)
			if ok {
				w.log.Errorf("[%s.%s] Bulk replace partially failed: %d failed", dbName, collName, len(bulkWriteException.WriteErrors))

				// Process individual errors
				for _, writeErr := range bulkWriteException.WriteErrors {
					var failedDocID interface{}
					var op WriteOperation
					if writeErr.Index < len(group.Operations) {
						op = group.Operations[writeErr.Index]
						failedDocID = op.DocumentID
					}
					w.log.Errorf("[%s.%s] Replace error at index %d, _id=%v: %v", dbName, collName, writeErr.Index, failedDocID, writeErr.Message)

					if writeErr.Index < len(group.Operations) {
						w.retryIndividualOperation(targetCollection, op, dbName, collName, writeErr.Message)
					}
				}
			} else {
				// Handle non-bulk write errors
				if err == context.Canceled {
					w.log.Debugf("[%s.%s] Bulk replace canceled due to context cancellation", dbName, collName)
				} else {
					w.log.Errorf("[%s.%s] Error performing bulk replace: %v", dbName, collName, err)
				}

				// For transient errors, retry the bulk operation with backoff before falling back
				bulkRetrySucceeded := false
				if w.retryManager != nil && err != context.Canceled {
					errType := w.retryManager.ClassifyError(err)
					if errType == ErrorTypeConnection || errType == ErrorTypeContention {
						w.log.Infof("[%s.%s] Transient error detected. Retrying bulk replace with backoff...", dbName, collName)
						retryErr := w.retryManager.RetryWithBackoff(w.ctx, func() error {
							_, retryBulkErr := targetCollection.BulkWrite(w.ctx, models, options.BulkWrite().SetOrdered(useOrdered))
							return retryBulkErr
						})
						if retryErr == nil {
							w.log.Infof("[%s.%s] Bulk replace succeeded after retry", dbName, collName)
							bulkRetrySucceeded = true
						} else {
							w.log.Warnf("[%s.%s] Bulk replace still failed after retries: %v. Falling back to individual operations.", dbName, collName, retryErr)
						}
					}
				}

				if bulkRetrySucceeded {
					break
				}

				// Fall back to individual replaces
				for _, op := range group.Operations {
					w.retryIndividualOperation(targetCollection, op, dbName, collName, err.Error())
				}
			}
		} else {
			w.log.Debugf("[%s.%s] Bulk replaced %d documents", dbName, collName, len(models))
		}
	}

	// Record replication lag for successfully processed operations
	if w.statsManager != nil {
		w.statsManager.RecordLags(group.Operations, time.Now())
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
	w.mu.Unlock()

	// Close raw incoming queue to signal eventLoop to drain and close processingQueue
	close(w.incomingQueue)

	// Wait for both eventLoop and processGroups goroutines to complete
	w.WaitForCompletion()
}



// retryIndividualOperation retries a single failed write operation of a batch using fallback execution.
func (w *Worker) retryIndividualOperation(targetCollection *mongo.Collection, op WriteOperation, dbName, collName string, originalErrorMsg string) {
	filter := bson.M{"_id": op.DocumentID}
	if w.statsManager != nil {
		w.statsManager.IncrementSequentialRetries(op.OpType, 1)
	}

	// Check if this error represents a duplicate key/already exists constraint failure
	isDup := isDuplicateKeyError(0, originalErrorMsg)

	switch op.OpType {
	case "insert":
		transformed, err := TransformFieldNames(op.Document, w.log, dbName, collName, op.DocumentID)
		if err != nil {
			w.log.Errorf("[%s.%s] Field name transformation failed for fallback insert, document _id=%v: %v", dbName, collName, op.DocumentID, err)
			if w.dlq != nil {
				w.dlq.WriteFailed(dbName, collName, op.DocumentID, err, "incremental", "insert", op.Document)
			}
			return
		}

		if isDup {
			// If duplicate key, retry using ReplaceOne with upsert=true to overwrite
			w.log.Debugf("[%s.%s] Fallback upserting duplicate insert document _id=%v", dbName, collName, op.DocumentID)
			if _, err := targetCollection.ReplaceOne(w.ctx, filter, transformed, options.Replace().SetUpsert(true)); err != nil {
				w.log.Debugf("[%s.%s] Fallback upsert failed for insert document _id=%v: %v", dbName, collName, op.DocumentID, err)
				if w.dlq != nil {
					w.dlq.WriteFailed(dbName, collName, op.DocumentID, err, "incremental", "insert", op.Document)
				}
			} else {
				w.log.Debugf("[%s.%s] Successfully upserted document _id=%v after duplicate key error", dbName, collName, op.DocumentID)
			}
		} else {
			// For non-duplicate key errors, retry with standard InsertOne
			if _, err := targetCollection.InsertOne(w.ctx, transformed); err != nil {
				w.log.Errorf("[%s.%s] Fallback insert failed for document _id=%v: %v", dbName, collName, op.DocumentID, err)
				if w.dlq != nil {
					w.dlq.WriteFailed(dbName, collName, op.DocumentID, err, "incremental", "insert", op.Document)
				}
			}
		}

	case "update":
		if op.Document != nil {
			transformed, err := TransformFieldNames(op.Document, w.log, dbName, collName, op.DocumentID)
			if err != nil {
				w.log.Errorf("[%s.%s] Field name transformation failed for fallback replace update, document _id=%v: %v", dbName, collName, op.DocumentID, err)
				if w.dlq != nil {
					w.dlq.WriteFailed(dbName, collName, op.DocumentID, err, "incremental", "update", op.Document)
				}
				return
			}
			if _, err := targetCollection.ReplaceOne(w.ctx, filter, transformed, options.Replace().SetUpsert(true)); err != nil {
				if err == context.Canceled || w.ctx.Err() == context.Canceled {
					w.log.Debugf("[%s.%s] Fallback replace update for document _id=%v canceled", dbName, collName, op.DocumentID)
					return
				}

				// If it failed because the document already exists (concurrent upsert collision), retry with upsert=false
				if isDuplicateKeyError(0, err.Error()) {
					w.log.Debugf("[%s.%s] Concurrent upsert collision detected for replace document _id=%v, retrying without upsert", dbName, collName, op.DocumentID)
					if _, retryErr := targetCollection.ReplaceOne(w.ctx, filter, transformed, options.Replace().SetUpsert(false)); retryErr != nil {
						w.log.Errorf("[%s.%s] Fallback replace update without upsert failed for document _id=%v: %v", dbName, collName, op.DocumentID, retryErr)
						if w.dlq != nil {
							w.dlq.WriteFailed(dbName, collName, op.DocumentID, retryErr, "incremental", "update", op.Document)
						}
					} else {
						w.log.Debugf("[%s.%s] Successfully completed replace update for document _id=%v after concurrent upsert resolution", dbName, collName, op.DocumentID)
					}
				} else {
					w.log.Errorf("[%s.%s] Fallback replace update failed for document _id=%v: %v", dbName, collName, op.DocumentID, err)
					if w.dlq != nil {
						w.dlq.WriteFailed(dbName, collName, op.DocumentID, err, "incremental", "update", op.Document)
					}
				}
			}
		} else {
			w.log.Errorf("[%s.%s] Fallback update failed: document payload is nil for _id=%v", dbName, collName, op.DocumentID)
		}

	case "replace":
		transformed, err := TransformFieldNames(op.Document, w.log, dbName, collName, op.DocumentID)
		if err != nil {
			w.log.Errorf("[%s.%s] Field name transformation failed for fallback replace, document _id=%v: %v", dbName, collName, op.DocumentID, err)
			if w.dlq != nil {
				w.dlq.WriteFailed(dbName, collName, op.DocumentID, err, "incremental", "replace", op.Document)
			}
			return
		}
		if _, err := targetCollection.ReplaceOne(w.ctx, filter, transformed, options.Replace().SetUpsert(true)); err != nil {
			if err == context.Canceled || w.ctx.Err() == context.Canceled {
				w.log.Debugf("[%s.%s] Fallback replace for document _id=%v canceled due to context cancellation", dbName, collName, op.DocumentID)
				return
			}

			// If it failed because the document already exists (concurrent upsert collision), retry with upsert=false
			if isDuplicateKeyError(0, err.Error()) {
				w.log.Debugf("[%s.%s] Concurrent upsert collision detected for replace document _id=%v, retrying without upsert", dbName, collName, op.DocumentID)
				if _, retryErr := targetCollection.ReplaceOne(w.ctx, filter, transformed, options.Replace().SetUpsert(false)); retryErr != nil {
					w.log.Errorf("[%s.%s] Fallback replace without upsert failed for document _id=%v: %v", dbName, collName, op.DocumentID, retryErr)
					if w.dlq != nil {
						w.dlq.WriteFailed(dbName, collName, op.DocumentID, retryErr, "incremental", "replace", op.Document)
					}
				} else {
					w.log.Debugf("[%s.%s] Successfully completed replace for document _id=%v after concurrent upsert resolution", dbName, collName, op.DocumentID)
				}
			} else {
				w.log.Errorf("[%s.%s] Fallback replace failed for document _id=%v: %v", dbName, collName, op.DocumentID, err)
				if w.dlq != nil {
					w.dlq.WriteFailed(dbName, collName, op.DocumentID, err, "incremental", "replace", op.Document)
				}
			}
		}

	case "delete":
		if _, err := targetCollection.DeleteOne(w.ctx, filter); err != nil {
			w.log.Errorf("[%s.%s] Fallback delete failed for document _id=%v: %v", dbName, collName, op.DocumentID, err)
			if w.dlq != nil {
				w.dlq.WriteFailed(dbName, collName, op.DocumentID, err, "incremental", "delete", nil)
			}
		}
	}
}

// isDuplicateKeyError checks if a write error code or message represents a duplicate key/document already exists constraint failure.
func isDuplicateKeyError(code int, msg string) bool {
	return code == 11000 || 
		strings.Contains(msg, "Document already exists") || 
		strings.Contains(msg, "E11000")
}
