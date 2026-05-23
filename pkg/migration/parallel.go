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
	forceOrderedOperations bool, flushInterval time.Duration, cfg *config.Config, dlq DLQ, statsManager *StatsManager) *EventDistributor {

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

		// Use the shared statistics tracking
		statsManager: statsManager,
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
		bytes = id[:]
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

	// Ensure all workers are shut down sequentially and safely upon exit
	defer func() {
		d.log.Info("Shutting down workers...")
		for _, worker := range d.workers {
			if worker != nil {
				worker.Shutdown()
			}
		}
	}()

	// Main loop to read from change stream and distribute events
	var changeCount int
	lastCheckpointTime := time.Now()

	for {
		// Try to get next change event
		ok := d.changeStream.Next(d.ctx)
		readTime := time.Now()
		if !ok {
			// Check if this is due to an error or end of stream
			if err := d.changeStream.Err(); err != nil {
				// Check if the error is due to context cancellation
				if err == context.Canceled {
					d.log.Info("Change stream interrupted due to context cancellation")
					return nil
				}
				d.log.Errorf("Change stream error: %v", err)
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

		// Determine worker lock-freely and allocation-freely by hashing raw BSON bytes directly.
		// By hashing the raw `_id` BSON value slice directly, we completely avoid calling Unmarshal
		// on the single-threaded distributor loop. This shifts 100% of the CPU-heavy BSON decoding
		// load into parallel worker goroutines, enabling linear CPU scale-out and 0 heap allocations.
		hash := hashBytes(docIDVal.Value)
		workerIndex := ((hash % d.incrementalWorkerCount) + d.incrementalWorkerCount) % d.incrementalWorkerCount

		// Safe concurrent deep copy of raw BSON bytes to prevent mutation races
		rawCopy := make(bson.Raw, len(rawEvent))
		copy(rawCopy, rawEvent)

		// Send raw event copy to appropriate worker concurrently
		select {
		case d.workers[workerIndex].incomingQueue <- QueueEvent{Event: rawCopy, ReadTime: readTime}:
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

// QueueEvent wraps the raw change stream or oplog event with metadata like read time
type QueueEvent struct {
	Event    interface{} // bson.M or bson.Raw
	ReadTime time.Time
}

// WriteOperation represents a single write operation
type WriteOperation struct {
	DocumentID        interface{}
	Document          interface{}
	UpdateDescription interface{} // For modifier updates ($set, $inc, etc.)
	Namespace         string
	OpType            string
	// Stats
	EventTime         time.Time // Time when the change event occurred on the source database (clusterTime or wallTime)
	ReadTime          time.Time // Time when the event was read from the change stream/oplog by the distributor
	WorkerReceiveTime time.Time // Time when the event was received by the worker thread goroutine
	SuccessTime       time.Time // Set when successfully written to target DB
	SuccessAfterRetry bool      // Set to true if the operation succeeded after a retry
	DLQed             bool      // Track if the operation was routed to the DLQ
	Error             error     // Track the exact write failure error
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
	log           *workerLogger
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
	currentGroupIDs      map[int]bool
	flushInterval        time.Duration
}

// flushCurrentGroup moves the current group to the processing queue if it exists
func (w *Worker) flushCurrentGroup() bool {
	// Must be called with lock held
	if w.currentGroup != nil && len(w.currentGroup.Operations) > 0 {
		w.log.Debugf("Flushing group: %s.%s with %d operations",
			w.currentGroup.Namespace, w.currentGroup.OpType,
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
	// Incremented in statsManager.RecordLags to prevent double counting and maintain context
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
		log:                       &workerLogger{workerID: id, logger: log},
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
	var readTime time.Time

	switch e := eventArg.(type) {
	case QueueEvent:
		readTime = e.ReadTime
		switch inner := e.Event.(type) {
		case bson.M:
			event = inner
		case bson.Raw:
			if err := bson.Unmarshal(inner, &event); err != nil {
				w.log.Errorf("Failed to unmarshal raw BSON change event: %v", err)
				return
			}
		default:
			w.log.Errorf("Invalid inner event type: %T", e.Event)
			return
		}
	case bson.M:
		event = e
		if rt, exists := event["readTime"]; exists {
			if t, ok := rt.(time.Time); ok {
				readTime = t
			}
		}
	case bson.Raw:
		if err := bson.Unmarshal(e, &event); err != nil {
			w.log.Errorf("Failed to unmarshal raw BSON change event: %v", err)
			return
		}
	default:
		w.log.Errorf("Invalid event type in ProcessEvent: %T", eventArg)
		return
	}

	if readTime.IsZero() {
		readTime = time.Now()
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
	w.log.Debugf("received event: type=%s, namespace=%s, docID=%v, hasFullDoc=%v, hasUpdateDesc=%v",
		opType, namespace, docID, fullDocument != nil, updateDescription != nil)

	// Create write operation
	op := WriteOperation{
		DocumentID:        docID,
		Document:          fullDocument,
		UpdateDescription: updateDescription,
		Namespace:         namespace,
		OpType:            opType,
		EventTime:         eventTime,
		ReadTime:          readTime,
		WorkerReceiveTime: time.Now(),
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
					w.log.Debugf("Completed processing all groups during shutdown")
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
	w.log.Debugf("Processing group: %s.%s with %d operations",
		group.Namespace, group.OpType, len(group.Operations))

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

	// Use a graceful shutdown context with timeout if the main context was canceled.
	// This ensures the final queues flushed during Ctrl+C / shutdown can actually write to MongoDB.
	writeCtx := w.ctx
	if w.ctx.Err() != nil {
		var cancel context.CancelFunc
		writeCtx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
	}

	// Determine if we should use ordered operations
	useOrdered := group.OpType == "update" || group.OpType == "replace" || w.forceOrderedOperations

	if w.statsManager != nil {
		w.statsManager.RecordBulkWrite(len(group.Operations), useOrdered)
	}

	// Process based on operation type
	switch group.OpType {
	case "mixed":
		var models []mongo.WriteModel
		for idx := range group.Operations {
			op := &group.Operations[idx]
			model, err := w.buildWriteModel(*op, dbName, collName)
			if err != nil {
				w.handleTransformationFailure(op, dbName, collName, err)
				continue
			}
			models = append(models, model)
		}

		if len(models) > 0 {
			startTime := time.Now()
			_, err := targetCollection.BulkWrite(writeCtx, models, options.BulkWrite().SetOrdered(useOrdered))
			if w.statsManager != nil {
				w.statsManager.RecordLatency("mixed", group.Operations, time.Since(startTime), w.id, err == nil)
			}
			w.handleBulkWriteResult(writeCtx, &group, err, targetCollection, dbName, collName, func() error {
				_, retryBulkErr := targetCollection.BulkWrite(writeCtx, models, options.BulkWrite().SetOrdered(useOrdered))
				return retryBulkErr
			})
		}

	case "insert":
		var docs []interface{}
		for idx := range group.Operations {
			op := &group.Operations[idx]
			// Transform __*__ field names to _*_ for Firestore compatibility
			transformed, err := TransformFieldNames(op.Document, w.log.logger, dbName, collName, op.DocumentID)
			if err != nil {
				w.handleTransformationFailure(op, dbName, collName, err)
				continue
			}
			docs = append(docs, transformed)
		}

		startTime := time.Now()
		_, err := targetCollection.InsertMany(writeCtx, docs, options.InsertMany().SetOrdered(useOrdered))
		if w.statsManager != nil {
			w.statsManager.RecordLatency("insert", group.Operations, time.Since(startTime), w.id, err == nil)
		}
		w.handleBulkWriteResult(writeCtx, &group, err, targetCollection, dbName, collName, func() error {
			_, retryInsertErr := targetCollection.InsertMany(writeCtx, docs, options.InsertMany().SetOrdered(useOrdered))
			return retryInsertErr
		})

	case "update":
		var models []mongo.WriteModel
		for idx := range group.Operations {
			op := &group.Operations[idx]
			model, err := w.buildWriteModel(*op, dbName, collName)
			if err != nil {
				w.handleTransformationFailure(op, dbName, collName, err)
				continue
			}
			models = append(models, model)
		}

		startTime := time.Now()
		_, err := targetCollection.BulkWrite(writeCtx, models, options.BulkWrite().SetOrdered(useOrdered))
		if w.statsManager != nil {
			w.statsManager.RecordLatency("update", group.Operations, time.Since(startTime), w.id, err == nil)
		}
		w.handleBulkWriteResult(writeCtx, &group, err, targetCollection, dbName, collName, func() error {
			_, retryBulkErr := targetCollection.BulkWrite(writeCtx, models, options.BulkWrite().SetOrdered(useOrdered))
			return retryBulkErr
		})

	case "delete":
		var models []mongo.WriteModel
		for _, op := range group.Operations {
			model, err := w.buildWriteModel(op, dbName, collName)
			if err != nil {
				continue
			}
			models = append(models, model)
		}

		startTime := time.Now()
		_, err := targetCollection.BulkWrite(writeCtx, models, options.BulkWrite().SetOrdered(useOrdered))
		if w.statsManager != nil {
			w.statsManager.RecordLatency("delete", group.Operations, time.Since(startTime), w.id, err == nil)
		}
		w.handleBulkWriteResult(writeCtx, &group, err, targetCollection, dbName, collName, func() error {
			_, retryBulkErr := targetCollection.BulkWrite(writeCtx, models, options.BulkWrite().SetOrdered(useOrdered))
			return retryBulkErr
		})

	case "replace":
		var models []mongo.WriteModel
		for idx := range group.Operations {
			op := &group.Operations[idx]
			model, err := w.buildWriteModel(*op, dbName, collName)
			if err != nil {
				w.handleTransformationFailure(op, dbName, collName, err)
				continue
			}
			models = append(models, model)
		}

		startTime := time.Now()
		_, err := targetCollection.BulkWrite(writeCtx, models, options.BulkWrite().SetOrdered(useOrdered))
		if w.statsManager != nil {
			w.statsManager.RecordLatency("replace", group.Operations, time.Since(startTime), w.id, err == nil)
		}
		w.handleBulkWriteResult(writeCtx, &group, err, targetCollection, dbName, collName, func() error {
			_, retryBulkErr := targetCollection.BulkWrite(writeCtx, models, options.BulkWrite().SetOrdered(useOrdered))
			return retryBulkErr
		})
	}

	// Record replication lag for successfully processed operations
	if w.statsManager != nil {
		w.statsManager.RecordLags(group.Operations)
	}
}

// handleBulkWriteResult processes the outcome of a bulk write operation, setting SuccessTime for succeeded operations and executing fallbacks for failed ones.
func (w *Worker) handleBulkWriteResult(ctx context.Context, group *OperationGroup, err error, targetCollection *mongo.Collection, dbName, collName string, transientRetryFunc func() error) {
	if err == nil {
		now := time.Now()
		for i := range group.Operations {
			group.Operations[i].SuccessTime = now
		}
		w.log.Debugf("[%s.%s] Bulk %s operations processed successfully: %d documents", dbName, collName, group.OpType, len(group.Operations))
		return
	}

	bulkWriteException, ok := err.(mongo.BulkWriteException)
	if ok && ctx.Err() != context.Canceled {
		nonDupCount := 0
		for _, writeErr := range bulkWriteException.WriteErrors {
			if !isDuplicateKeyError(writeErr.Code, writeErr.Message) {
				nonDupCount++
			}
		}

		if nonDupCount > 0 {
			w.log.Errorf("[%s.%s] Bulk %s operations partially failed: %d failed", dbName, collName, group.OpType, len(bulkWriteException.WriteErrors))
		} else {
			w.log.Debugf("[%s.%s] Bulk %s operations partially failed: %d failed (all duplicate key errors)", dbName, collName, group.OpType, len(bulkWriteException.WriteErrors))
		}

		failedIndices := make(map[int]bool)
		for _, writeErr := range bulkWriteException.WriteErrors {
			failedIndices[writeErr.Index] = true
		}
		now := time.Now()
		for i := range group.Operations {
			if !failedIndices[i] {
				group.Operations[i].SuccessTime = now
			}
		}

		// Process individual errors
		for _, writeErr := range bulkWriteException.WriteErrors {
			var failedDocID interface{}
			var op WriteOperation
			if writeErr.Index < len(group.Operations) {
				op = group.Operations[writeErr.Index]
				failedDocID = op.DocumentID
			}

			isDup := isDuplicateKeyError(writeErr.Code, writeErr.Message)
			if isDup {
				if w.statsManager != nil {
					w.statsManager.IncrementDuplicateKeys(1)
				}
				w.log.Debugf("[%s.%s] Duplicate key occurrence at index %d (opType=%s), _id=%v: %v. Gracefully falling back.", dbName, collName, writeErr.Index, op.OpType, failedDocID, writeErr.Message)
			} else {
				w.log.Errorf("[%s.%s] Write error at index %d (opType=%s), _id=%v: %v", dbName, collName, writeErr.Index, op.OpType, failedDocID, writeErr.Message)
			}

			if writeErr.Index < len(group.Operations) {
				w.retryIndividualOperation(ctx, targetCollection, &group.Operations[writeErr.Index], dbName, collName, writeErr.Message)
			}
		}
	} else {
		// Handle non-bulk write errors
		if err == context.Canceled || ctx.Err() == context.Canceled {
			w.log.Debugf("[%s.%s] Bulk %s operations canceled due to context cancellation", dbName, collName, group.OpType)
			return
		}

		w.log.Errorf("[%s.%s] Error performing bulk %s operations: %v", dbName, collName, group.OpType, err)

		// For transient errors, retry the bulk operation with backoff before falling back
		bulkRetrySucceeded := false
		if w.retryManager != nil && transientRetryFunc != nil {
			errType := w.retryManager.ClassifyError(err)
			if errType == ErrorTypeConnection || errType == ErrorTypeContention {
				w.log.Infof("[%s.%s] Transient error detected. Retrying bulk %s operations with backoff...", dbName, collName, group.OpType)
				retryErr := w.retryManager.RetryWithBackoff(ctx, transientRetryFunc)
				if retryErr == nil {
					w.log.Infof("[%s.%s] Bulk %s operations succeeded after retry", dbName, collName, group.OpType)
					bulkRetrySucceeded = true
					now := time.Now()
					for i := range group.Operations {
						group.Operations[i].SuccessTime = now
						group.Operations[i].SuccessAfterRetry = true
					}
				} else {
					w.log.Warnf("[%s.%s] Bulk %s operations still failed after retries: %v. Falling back to individual operations.", dbName, collName, group.OpType, retryErr)
				}
			}
		}

		if !bulkRetrySucceeded {
			// Fall back to individual updates/deletes/replaces
			for i := range group.Operations {
				w.retryIndividualOperation(ctx, targetCollection, &group.Operations[i], dbName, collName, err.Error())
			}
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
	w.log.Debugf("All operations processed successfully")
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
func (w *Worker) retryIndividualOperation(ctx context.Context, targetCollection *mongo.Collection, op *WriteOperation, dbName, collName string, originalErrorMsg string) {
	filter := bson.M{"_id": op.DocumentID}
	if w.statsManager != nil {
		w.statsManager.IncrementSequentialRetries(op.OpType, 1)
	}

	// Check if this error represents a duplicate key/already exists constraint failure
	isDup := isDuplicateKeyError(0, originalErrorMsg)

	switch op.OpType {
	case "insert":
		transformed, err := TransformFieldNames(op.Document, w.log.logger, dbName, collName, op.DocumentID)
		if err != nil {
			w.log.Errorf("[%s.%s] Field name transformation failed for fallback insert, document _id=%v: %v", dbName, collName, op.DocumentID, err)
			w.markDLQ(op, dbName, collName, err)
			return
		}

		if isDup {
			// If duplicate key, retry using ReplaceOne with upsert=true to overwrite
			w.log.Debugf("[%s.%s] Fallback upserting duplicate insert document _id=%v", dbName, collName, op.DocumentID)
			if _, err := targetCollection.ReplaceOne(ctx, filter, transformed, options.Replace().SetUpsert(true)); err != nil {
				w.log.Debugf("[%s.%s] Fallback upsert failed for insert document _id=%v: %v", dbName, collName, op.DocumentID, err)
				w.markDLQ(op, dbName, collName, err)
			} else {
				w.log.Debugf("[%s.%s] Successfully upserted document _id=%v after duplicate key error", dbName, collName, op.DocumentID)
				op.SuccessTime = time.Now()
				op.SuccessAfterRetry = true
			}
		} else {
			// For non-duplicate key errors, retry with standard InsertOne
			if _, err := targetCollection.InsertOne(ctx, transformed); err != nil {
				// If it actually already exists (e.g. concurrent write or socket retry pre-exist), fallback to ReplaceOne upsert
				if isDuplicateKeyError(0, err.Error()) {
					if w.statsManager != nil {
						w.statsManager.IncrementDuplicateKeys(1)
					}
					w.log.Debugf("[%s.%s] Fallback InsertOne got duplicate key, retrying with upsert overwrite for _id=%v", dbName, collName, op.DocumentID)
					if _, replaceErr := targetCollection.ReplaceOne(ctx, filter, transformed, options.Replace().SetUpsert(true)); replaceErr != nil {
						w.log.Errorf("[%s.%s] Fallback upsert replace after duplicate insert failed for document _id=%v: %v", dbName, collName, op.DocumentID, replaceErr)
						w.markDLQ(op, dbName, collName, replaceErr)
					} else {
						op.SuccessTime = time.Now()
						op.SuccessAfterRetry = true
					}
				} else {
					w.log.Errorf("[%s.%s] Fallback insert failed for document _id=%v: %v", dbName, collName, op.DocumentID, err)
					w.markDLQ(op, dbName, collName, err)
				}
			} else {
				op.SuccessTime = time.Now()
				op.SuccessAfterRetry = true
			}
		}

	case "update":
		if op.Document != nil {
			transformed, err := TransformFieldNames(op.Document, w.log.logger, dbName, collName, op.DocumentID)
			if err != nil {
				w.log.Errorf("[%s.%s] Field name transformation failed for fallback replace update, document _id=%v: %v", dbName, collName, op.DocumentID, err)
				w.markDLQ(op, dbName, collName, err)
				return
			}
			if _, err := targetCollection.ReplaceOne(ctx, filter, transformed, options.Replace().SetUpsert(true)); err != nil {
				if err == context.Canceled || ctx.Err() == context.Canceled {
					w.log.Debugf("[%s.%s] Fallback replace update for document _id=%v canceled", dbName, collName, op.DocumentID)
					return
				}

				// If it failed because the document already exists (concurrent upsert collision), retry with upsert=false
				if isDuplicateKeyError(0, err.Error()) {
					if w.statsManager != nil {
						w.statsManager.IncrementDuplicateKeys(1)
					}
					w.log.Debugf("[%s.%s] Concurrent upsert collision detected for replace document _id=%v, retrying without upsert", dbName, collName, op.DocumentID)
					if _, retryErr := targetCollection.ReplaceOne(ctx, filter, transformed, options.Replace().SetUpsert(false)); retryErr != nil {
						w.log.Errorf("[%s.%s] Fallback replace update without upsert failed for document _id=%v: %v", dbName, collName, op.DocumentID, retryErr)
						w.markDLQ(op, dbName, collName, retryErr)
					} else {
						w.log.Debugf("[%s.%s] Successfully completed replace update for document _id=%v after concurrent upsert resolution", dbName, collName, op.DocumentID)
						op.SuccessTime = time.Now()
						op.SuccessAfterRetry = true
					}
				} else {
					w.log.Errorf("[%s.%s] Fallback replace update failed for document _id=%v: %v", dbName, collName, op.DocumentID, err)
					w.markDLQ(op, dbName, collName, err)
				}
			} else {
				op.SuccessTime = time.Now()
				op.SuccessAfterRetry = true
			}
		} else {
			w.log.Errorf("[%s.%s] Fallback update failed: document payload is nil for _id=%v", dbName, collName, op.DocumentID)
		}

	case "replace":
		transformed, err := TransformFieldNames(op.Document, w.log.logger, dbName, collName, op.DocumentID)
		if err != nil {
			w.log.Errorf("[%s.%s] Field name transformation failed for fallback replace, document _id=%v: %v", dbName, collName, op.DocumentID, err)
			w.markDLQ(op, dbName, collName, err)
			return
		}
		if _, err := targetCollection.ReplaceOne(ctx, filter, transformed, options.Replace().SetUpsert(true)); err != nil {
			if err == context.Canceled || ctx.Err() == context.Canceled {
				w.log.Debugf("[%s.%s] Fallback replace for document _id=%v canceled due to context cancellation", dbName, collName, op.DocumentID)
				return
			}

			// If it failed because the document already exists (concurrent upsert collision), retry with upsert=false
			if isDuplicateKeyError(0, err.Error()) {
				if w.statsManager != nil {
					w.statsManager.IncrementDuplicateKeys(1)
				}
				w.log.Debugf("[%s.%s] Concurrent upsert collision detected for replace document _id=%v, retrying without upsert", dbName, collName, op.DocumentID)
				if _, retryErr := targetCollection.ReplaceOne(ctx, filter, transformed, options.Replace().SetUpsert(false)); retryErr != nil {
					w.log.Errorf("[%s.%s] Fallback replace without upsert failed for document _id=%v: %v", dbName, collName, op.DocumentID, retryErr)
					w.markDLQ(op, dbName, collName, retryErr)
				} else {
					w.log.Debugf("[%s.%s] Successfully completed replace for document _id=%v after concurrent upsert resolution", dbName, collName, op.DocumentID)
					op.SuccessTime = time.Now()
					op.SuccessAfterRetry = true
				}
			} else {
				w.log.Errorf("[%s.%s] Fallback replace failed for document _id=%v: %v", dbName, collName, op.DocumentID, err)
				w.markDLQ(op, dbName, collName, err)
			}
		} else {
			op.SuccessTime = time.Now()
			op.SuccessAfterRetry = true
		}

	case "delete":
		if _, err := targetCollection.DeleteOne(ctx, filter); err != nil {
			w.log.Errorf("[%s.%s] Fallback delete failed for document _id=%v: %v", dbName, collName, op.DocumentID, err)
			w.markDLQ(op, dbName, collName, err)
		} else {
			op.SuccessTime = time.Now()
			op.SuccessAfterRetry = true
		}
	}
}

func (w *Worker) markDLQ(op *WriteOperation, dbName, collName string, err error) {
	if w.dlq != nil {
		w.dlq.WriteFailed(dbName, collName, op.DocumentID, err, "incremental", op.OpType, op.Document)
	}
	op.DLQed = true
	op.Error = err
}

func (w *Worker) handleTransformationFailure(op *WriteOperation, dbName, collName string, err error) {
	w.log.Errorf("[%s.%s] Field name transformation failed for %s, document _id=%v: %v", dbName, collName, op.OpType, op.DocumentID, err)
	w.markDLQ(op, dbName, collName, err)
}

// isDuplicateKeyError checks if a write error code or message represents a duplicate key/document already exists constraint failure.
func isDuplicateKeyError(code int, msg string) bool {
	return code == 11000 ||
		strings.Contains(msg, "Document already exists") ||
		strings.Contains(msg, "E11000")
}

// buildWriteModel builds a single mongo.WriteModel from a WriteOperation, transforming Firestore-incompatible keys.
func (w *Worker) buildWriteModel(op WriteOperation, dbName, collName string) (mongo.WriteModel, error) {
	switch op.OpType {
	case "insert":
		transformed, err := TransformFieldNames(op.Document, w.log.logger, dbName, collName, op.DocumentID)
		if err != nil {
			return nil, err
		}
		return mongo.NewInsertOneModel().SetDocument(transformed), nil

	case "update", "replace":
		if op.Document == nil {
			return nil, fmt.Errorf("document payload is nil for _id=%v", op.DocumentID)
		}
		transformed, err := TransformFieldNames(op.Document, w.log.logger, dbName, collName, op.DocumentID)
		if err != nil {
			return nil, err
		}
		return mongo.NewReplaceOneModel().
			SetFilter(bson.M{"_id": op.DocumentID}).
			SetReplacement(transformed).
			SetUpsert(true), nil

	case "delete":
		return mongo.NewDeleteOneModel().SetFilter(bson.M{"_id": op.DocumentID}), nil
	}
	return nil, fmt.Errorf("unsupported operation type: %s", op.OpType)
}

type workerLogger struct {
	workerID int
	logger   *logger.Logger
}

func (wl *workerLogger) Debug(args ...interface{}) {
	newArgs := append([]interface{}{fmt.Sprintf("Worker %d: ", wl.workerID)}, args...)
	wl.logger.Debug(newArgs...)
}

func (wl *workerLogger) Debugf(format string, args ...interface{}) {
	newArgs := make([]interface{}, 1+len(args))
	newArgs[0] = wl.workerID
	copy(newArgs[1:], args)
	wl.logger.Debugf("Worker %d: "+format, newArgs...)
}

func (wl *workerLogger) Info(args ...interface{}) {
	newArgs := append([]interface{}{fmt.Sprintf("Worker %d: ", wl.workerID)}, args...)
	wl.logger.Info(newArgs...)
}

func (wl *workerLogger) Infof(format string, args ...interface{}) {
	newArgs := make([]interface{}, 1+len(args))
	newArgs[0] = wl.workerID
	copy(newArgs[1:], args)
	wl.logger.Infof("Worker %d: "+format, newArgs...)
}

func (wl *workerLogger) Warn(args ...interface{}) {
	newArgs := append([]interface{}{fmt.Sprintf("Worker %d: ", wl.workerID)}, args...)
	wl.logger.Warn(newArgs...)
}

func (wl *workerLogger) Warnf(format string, args ...interface{}) {
	newArgs := make([]interface{}, 1+len(args))
	newArgs[0] = wl.workerID
	copy(newArgs[1:], args)
	wl.logger.Warnf("Worker %d: "+format, newArgs...)
}

func (wl *workerLogger) Error(args ...interface{}) {
	newArgs := append([]interface{}{fmt.Sprintf("Worker %d: ", wl.workerID)}, args...)
	wl.logger.Error(newArgs...)
}

func (wl *workerLogger) Errorf(format string, args ...interface{}) {
	newArgs := make([]interface{}, 1+len(args))
	newArgs[0] = wl.workerID
	copy(newArgs[1:], args)
	wl.logger.Errorf("Worker %d: "+format, newArgs...)
}

const fnvOffset32 = 2166136261
const fnvPrime32 = 16777619

func hashBytes(data []byte) int {
	hash := uint32(fnvOffset32)
	for _, b := range data {
		hash ^= uint32(b)
		hash *= fnvPrime32
	}
	return int(hash)
}
