package migration

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gsbingo17/mongodb-migration/pkg/config"
	"github.com/gsbingo17/mongodb-migration/pkg/db"
	"github.com/gsbingo17/mongodb-migration/pkg/logger"
	"github.com/rwynn/gtm/v2"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// OplogReplicator handles replication using oplog tailing via GTM library
type OplogReplicator struct {
	sourceDB          *db.MongoDB
	targetDB          *db.MongoDB
	config            *config.Config
	log               *logger.Logger
	collectionMap     map[string]map[string]string // Map of database -> source collection -> target collection
	collectionConfigs map[string]map[string]config.CollectionConfig // Map of database -> source collection -> full config
	mu                sync.Mutex                   // Mutex for thread-safe operations
	dlq               DLQ                          // Dead Letter Queue for failed documents
	retryManager      *RetryManager                // Retry manager for transient errors
	DryRun            bool                         // Dry run flag
}

// NewOplogReplicator creates a new oplog-based replicator
func NewOplogReplicator(sourceDB, targetDB *db.MongoDB, cfg *config.Config, log *logger.Logger) *OplogReplicator {
	return &OplogReplicator{
		sourceDB:          sourceDB,
		targetDB:          targetDB,
		config:            cfg,
		log:               log,
		collectionMap:     make(map[string]map[string]string),
		collectionConfigs: make(map[string]map[string]config.CollectionConfig),
	}
}

// SetDLQ sets the Dead Letter Queue writer for this replicator
func (r *OplogReplicator) SetDLQ(dlq DLQ) {
	r.dlq = dlq
}

// AddCollection adds a collection to be watched
func (r *OplogReplicator) AddCollection(sourceDB, targetDB string, collConfig config.CollectionConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Initialize maps if needed
	if r.collectionMap[sourceDB] == nil {
		r.collectionMap[sourceDB] = make(map[string]string)
	}
	if r.collectionConfigs == nil {
		r.collectionConfigs = make(map[string]map[string]config.CollectionConfig)
	}
	if r.collectionConfigs[sourceDB] == nil {
		r.collectionConfigs[sourceDB] = make(map[string]config.CollectionConfig)
	}

	// Add collection mapping
	r.collectionMap[sourceDB][collConfig.SourceCollection] = collConfig.TargetCollection
	r.collectionConfigs[sourceDB][collConfig.SourceCollection] = collConfig

	r.log.Infof("Added collection mapping: %s.%s -> %s.%s (UpsertMode: %t)", 
		sourceDB, collConfig.SourceCollection, targetDB, collConfig.TargetCollection, collConfig.UpsertMode)
}

// StartReplication starts the oplog-based replication
func (r *OplogReplicator) StartReplication(ctx context.Context, globalTimestamp interface{}, timestampPath string, initialMigrationState *InitialMigrationState, initialMigrationStatePath string, pair config.DatabasePair, liveOnly bool, liveStartTime *primitive.Timestamp, migrator *Migrator) error {
	// Abort if the initial migration state was completed with failures, or if DLQ has entries
	if initialMigrationState != nil && initialMigrationState.Status == StatusCompletedWithFailures {
		return fmt.Errorf("cannot start replication: initial migration completed with failures in a previous run")
	}
	if r.dlq != nil {
		if _, isNop := r.dlq.(*NopDLQWriter); !isNop {
			if r.dlq.Count() > 0 {
				return fmt.Errorf("cannot start replication: DLQ contains failed documents")
			}
		}
	}

	// Enforce safety invariants between Initial Migration State and Oplog Timestamp Checkpoint
	if liveStartTime != nil && globalTimestamp != nil {
		return fmt.Errorf("safety violation: a custom live-start-timestamp is specified, but a global oplog timestamp checkpoint already exists. Clean up checkpoint file or omit live-start-timestamp to resume from the last checkpoint")
	}

	if liveStartTime == nil {
		if initialMigrationState == nil {
			if globalTimestamp != nil {
				return fmt.Errorf("safety violation: initial migration state file does not exist, but a global oplog timestamp checkpoint exists. Clean up checkpoint file or ensure state is in sync before proceeding")
			}
		} else if initialMigrationState.Status == StatusCompleted || initialMigrationState.Status == StatusSkipped {
			if globalTimestamp == nil {
				return fmt.Errorf("safety violation: initial migration state is marked as %s, but no global oplog timestamp checkpoint was found. Clean up state file or restore checkpoint before proceeding", initialMigrationState.Status)
			}
		}
	}

	var needsInitialMigration bool
	var afterTimestamp primitive.Timestamp

	// We need to run initial migration if no state file exists OR if it is not marked completed
	if initialMigrationState == nil || !initialMigrationState.IsCompleted() {
		if liveOnly {
			r.log.Info("Live-only mode enabled. Skipping initial migration phase.")
			if err := SaveInitialMigrationState(initialMigrationStatePath, StatusSkipped, 0); err != nil {
				r.log.Errorf("Error saving initial migration state as skipped: %v", err)
			}
			needsInitialMigration = false
			initialMigrationState = &InitialMigrationState{
				Status: StatusSkipped,
			}
		} else {
			needsInitialMigration = true
		}
	}

	// Convert globalTimestamp to OplogTimestamp if it exists
	var savedTimestamp *OplogTimestamp
	if globalTimestamp != nil {
		// Try to convert to OplogTimestamp
		if ts, ok := globalTimestamp.(*OplogTimestamp); ok {
			savedTimestamp = ts
		} else if tsMap, ok := globalTimestamp.(map[string]interface{}); ok {
			// Handle case where it's loaded from JSON as map
			if ts, ok := tsMap["ts"].(primitive.Timestamp); ok {
				savedTimestamp = &OplogTimestamp{Timestamp: ts}
				if term, ok := tsMap["t"].(int64); ok {
					savedTimestamp.Term = term
				}
			}
		}
	}

	// If no saved timestamp is found in the checkpoint files, determine the starting point:
	// - If the user passed a custom historical liveStartTime, use that as our starting tailing position.
	// - Otherwise, fetch the current cluster time from the primary to replicate starting from "now".
	if savedTimestamp == nil {
		if liveStartTime != nil {
			r.log.Infof("Using user-provided liveStartTime: %s", time.Unix(int64(liveStartTime.T), 0).UTC().Format(time.RFC3339))
			afterTimestamp = *liveStartTime
			savedTimestamp = &OplogTimestamp{Timestamp: *liveStartTime}
		} else {
			if liveOnly {
				r.log.Info("No oplog timestamp found in live-only mode. Obtaining current cluster time to start incremental replication.")
			} else {
				r.log.Info("No oplog timestamp found. Capturing current cluster time.")
			}

			// Get current cluster time as starting point
			currentTime, err := r.getCurrentClusterTime(ctx)
			if err != nil {
				return fmt.Errorf("failed to get current cluster time: %w", err)
			}

			afterTimestamp = currentTime
			savedTimestamp = &OplogTimestamp{Timestamp: currentTime}
		}

		// Save this timestamp
		if err := SaveOplogTimestamp(timestampPath, *savedTimestamp); err != nil {
			r.log.Errorf("Error saving oplog timestamp: %v", err)
		} else {
			r.log.Infof("Saved oplog timestamp: %v", savedTimestamp)
		}

	} else {
		r.log.Info("Oplog timestamp available.")
		afterTimestamp = savedTimestamp.Timestamp
	}

	// Create retry manager from config
	r.retryManager = NewRetryManagerFromConfig(r.config, r.log)

	// Perform initial migration if needed (same logic as change stream replicator)
	if needsInitialMigration {
		// Mark initial migration state as incomplete before starting
		if err := SaveInitialMigrationState(initialMigrationStatePath, StatusInProgress, 0); err != nil {
			r.log.Errorf("Error saving initial migration state as incomplete: %v", err)
		}

		initialMigrator := NewInitialMigrator(r.sourceDB, r.targetDB, r.config, r.log, r.collectionConfigs, r.dlq, r.retryManager)
		initialMigrator.DryRun = r.DryRun

		_, totalFailedCount, err := initialMigrator.Run(ctx, pair, migrator)
		if err != nil {
			return fmt.Errorf("initial migration failed: %w", err)
		}

		if pair.Target.IndexOnly {
			return nil
		}

		// Determine if initial migration completed with failures
		status := StatusCompleted
		if totalFailedCount > 0 {
			status = StatusCompletedWithFailures
		}
		if r.dlq != nil {
			if _, isNop := r.dlq.(*NopDLQWriter); !isNop {
				if r.dlq.Count() > 0 {
					status = StatusCompletedWithFailures
				}
			}
		}

		// Mark initial migration state as complete
		if err := SaveInitialMigrationState(initialMigrationStatePath, status, totalFailedCount); err != nil {
			r.log.Errorf("Error saving initial migration state as complete: %v", err)
		}

		if status == StatusCompletedWithFailures {
			return fmt.Errorf("initial migration completed with %d failures and/or DLQ entries. Aborting replication", totalFailedCount)
		}
		r.log.Info("Initial migration completed. Starting incremental replication.")
	} else {
		r.log.Info("Initial migration already marked as completed. Skipping.")
	}

	// Index-Only mode: sync indexes (if not already done during initial migration) and exit
	if pair.Target.IndexOnly {
		if !needsInitialMigration && (pair.Target.SyncAllIndexes || len(pair.Target.Indexes) > 0) {
			// Checkpoint exists, so performInitialMigration was skipped — sync indexes now
			r.log.Info("IndexOnly mode: checkpoint exists, performing index sync directly")
			// Build collections list from collectionConfigs
			var collections []config.CollectionConfig
			for _, colls := range r.collectionConfigs {
				for _, collConfig := range colls {
					collections = append(collections, collConfig)
				}
			}
			if err := migrator.syncIndexes(ctx, r.sourceDB, r.targetDB, pair, collections); err != nil {
				r.log.Warnf("Index sync encountered issues: %v", err)
			}
			r.log.Info("IndexOnly mode: waiting for all async index creation to complete...")
			r.targetDB.WaitForIndexCreation()
		}
		r.log.Info("IndexOnly mode: skipping oplog tailing. Index replication complete.")
		return nil
	}

	// Start oplog tailing using GTM
	return r.tailOplog(ctx, afterTimestamp, timestampPath)
}

// getCurrentClusterTime gets the current cluster time from MongoDB
func (r *OplogReplicator) getCurrentClusterTime(ctx context.Context) (primitive.Timestamp, error) {
	// Run a simple command to get cluster time
	var result bson.M
	err := r.sourceDB.GetClient().Database("admin").RunCommand(ctx, bson.D{{Key: "isMaster", Value: 1}}).Decode(&result)
	if err != nil {
		return primitive.Timestamp{}, fmt.Errorf("failed to get cluster time: %w", err)
	}

	// Extract cluster time
	if clusterTime, ok := result["$clusterTime"].(bson.M); ok {
		if ts, ok := clusterTime["clusterTime"].(primitive.Timestamp); ok {
			return ts, nil
		}
	}

	// Fallback: get latest oplog timestamp
	oplogCollection := r.sourceDB.GetClient().Database("local").Collection("oplog.rs")
	opts := options.FindOne().SetSort(bson.D{{Key: "$natural", Value: -1}})
	var lastEntry bson.M
	err = oplogCollection.FindOne(ctx, bson.D{}, opts).Decode(&lastEntry)
	if err != nil {
		return primitive.Timestamp{}, fmt.Errorf("failed to get last oplog entry: %w", err)
	}

	if ts, ok := lastEntry["ts"].(primitive.Timestamp); ok {
		return ts, nil
	}

	return primitive.Timestamp{}, fmt.Errorf("failed to extract timestamp from oplog")
}



// tailOplog starts tailing the oplog using GTM with parallel processing
func (r *OplogReplicator) tailOplog(ctx context.Context, afterTimestamp primitive.Timestamp, timestampPath string) error {
	r.log.Infof("Starting oplog tailing from timestamp: %v", afterTimestamp)

	// Build allowed namespaces map for filtering
	allowedNamespaces := make(map[string]bool)
	for sourceDB, collections := range r.collectionMap {
		for sourceCollection := range collections {
			namespace := fmt.Sprintf("%s.%s", sourceDB, sourceCollection)
			allowedNamespaces[namespace] = true
		}
	}

	// Configure GTM options
	gtmOpts := &gtm.Options{
		Filter: func(op *gtm.Op) bool {
			// Filter by timestamp and namespace
			if op.Timestamp.T < afterTimestamp.T || (op.Timestamp.T == afterTimestamp.T && op.Timestamp.I <= afterTimestamp.I) {
				return false
			}
			return ShouldProcessGTMOp(op, allowedNamespaces)
		},
		OpLogDatabaseName:   "local",
		OpLogCollectionName: "oplog.rs",
		ChannelSize:         r.config.IncrementalReadBatchSize,
		BufferDuration:      time.Duration(r.config.FlushIntervalMs) * time.Millisecond,
	}

	// Start GTM
	gtmCtx := gtm.Start(r.sourceDB.GetClient(), gtmOpts)
	defer gtmCtx.Stop()

	r.log.Info("GTM oplog tailing started successfully")

	// Initialize parallel workers
	r.log.Infof("Starting parallel oplog processing with %d workers", r.config.IncrementalWorkerCount)
	enableTransform := r.config.EnableFieldTransformations != nil && *r.config.EnableFieldTransformations
	transformer := NewFieldTransformer(enableTransform, r.log)
	workers := make([]*Worker, r.config.IncrementalWorkerCount)
	for i := 0; i < r.config.IncrementalWorkerCount; i++ {
		workers[i] = NewWorker(i, ctx, r.log, r.targetDB, r.collectionConfigs, r.config.IncrementalWriteBatchSize, r.config.ForceOrderedOperations, r.dlq, r.retryManager, nil, r.config.GroupOpsByDistinctId, time.Duration(r.config.FlushIntervalMs)*time.Millisecond, r.config.IncrementalIncomingQueueSize, r.config.IncrementalProcessingQueueSize, transformer)
	}

	// Set up context cancellation handling for workers
	go func() {
		<-ctx.Done()
		r.log.Info("Context canceled. Shutting down workers...")
		for _, worker := range workers {
			worker.Shutdown()
		}
	}()

	// Set up periodic flushing (matching EventDistributor pattern)
	flushInterval := time.Duration(r.config.FlushIntervalMs) * time.Millisecond
	StartPeriodicFlushLoop(ctx, workers, flushInterval, r.log)

	// Statistics tracking
	var processedCount int
	var lastCheckpoint time.Time = time.Now()
	var eventsSinceLastStats int
	var lastStatsTime time.Time = time.Now()
	statsInterval := time.Duration(r.config.StatsIntervalMinutes) * time.Minute

	// Track latest oplog timestamp for checkpoint saving
	var latestOplogTimestamp primitive.Timestamp

	// Set up periodic statistics reporting
	statsTicker := time.NewTicker(statsInterval)
	defer statsTicker.Stop()

	go func() {
		for {
			select {
			case <-statsTicker.C:
				// Calculate and log statistics
				r.mu.Lock()
				eventCount := eventsSinceLastStats
				duration := time.Since(lastStatsTime)
				eventsSinceLastStats = 0
				lastStatsTime = time.Now()
				r.mu.Unlock()

				if duration > 0 && eventCount > 0 {
					rate := float64(eventCount) / duration.Seconds()
					r.log.Infof("Oplog replication statistics: Processed %d events in the last %v (%.2f events/second)",
						eventCount, duration.Round(time.Second), rate)
				} else if eventCount > 0 {
					r.log.Infof("Oplog replication statistics: Processed %d events since last report", eventCount)
				} else {
					r.log.Info("Oplog replication statistics: No events processed since last report")
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Start processing GTM operations with parallel workers
	for {
		select {
		case op := <-gtmCtx.OpC:
			if op == nil {
				continue
			}

			// Update latest oplog timestamp
			latestOplogTimestamp = op.Timestamp

			if r.DryRun {
				continue
			}

			// Convert oplog event to worker event format and distribute to workers
			r.distributeOplogEvent(ctx, op, workers)

			r.mu.Lock()
			processedCount++
			eventsSinceLastStats++
			r.mu.Unlock()

			// Periodic checkpoint
			r.mu.Lock()
			shouldCheckpoint := processedCount >= r.config.SaveThreshold || time.Since(lastCheckpoint) >= time.Duration(r.config.CheckpointIntervalMinutes)*time.Minute
			currentProcessedCount := processedCount
			r.mu.Unlock()

			if shouldCheckpoint {
				timestamp := OplogTimestamp{
					Timestamp: latestOplogTimestamp,
				}
				if err := SaveOplogTimestamp(timestampPath, timestamp); err != nil {
					r.log.Errorf("Failed to save oplog timestamp: %v", err)
				} else {
					r.log.Infof("Checkpoint saved (%d operations processed, timestamp T=%d I=%d)",
						currentProcessedCount, latestOplogTimestamp.T, latestOplogTimestamp.I)
				}
				r.mu.Lock()
				processedCount = 0
				lastCheckpoint = time.Now()
				r.mu.Unlock()
			}

		case err := <-gtmCtx.ErrC:
			if err != nil {
				r.log.Errorf("GTM error: %v", err)
				// GTM will attempt to reconnect automatically
			}

		case <-ctx.Done():
			r.log.Info("Oplog replication stopped due to context cancellation")

			// Wait for all workers to finish processing
			for _, worker := range workers {
				worker.WaitForCompletion()
			}

			// Save final oplog timestamp before exiting
			if latestOplogTimestamp.T > 0 || latestOplogTimestamp.I > 0 {
				finalTimestamp := OplogTimestamp{
					Timestamp: latestOplogTimestamp,
				}
				if err := SaveOplogTimestamp(timestampPath, finalTimestamp); err != nil {
					r.log.Errorf("Failed to save final oplog timestamp on shutdown: %v", err)
				} else {
					r.log.Infof("Saved final oplog timestamp on shutdown: T=%d, I=%d",
						latestOplogTimestamp.T, latestOplogTimestamp.I)
				}
			}

			return nil
		}
	}
}

// distributeOplogEvent converts oplog event to worker format and distributes to appropriate worker
func (r *OplogReplicator) distributeOplogEvent(ctx context.Context, op *gtm.Op, workers []*Worker) {
	// Extract namespace parts
	parts := strings.SplitN(op.Namespace, ".", 2)
	if len(parts) != 2 {
		r.log.Warnf("Invalid namespace: %s", op.Namespace)
		return
	}

	sourceDB := parts[0]
	sourceCollection := parts[1]

	// Convert GTM operation to change event format expected by workers
	var changeEvent bson.M

	switch op.Operation {
	case "i": // insert
		changeEvent = bson.M{
			"operationType": "insert",
			"ns": bson.M{
				"db":   sourceDB,
				"coll": sourceCollection,
			},
			"documentKey": bson.M{
				"_id": op.Id,
			},
			"fullDocument": op.Data,
		}

	case "u": // update
		// Check if this is a modifier update or full document replacement
		var hasModifiers bool
		for k := range op.Data {
			if strings.HasPrefix(k, "$") {
				hasModifiers = true
				break
			}
		}

		if hasModifiers {
			// Modifier update - convert to update event with updateDescription
			changeEvent = bson.M{
				"operationType": "update",
				"ns": bson.M{
					"db":   sourceDB,
					"coll": sourceCollection,
				},
				"documentKey": bson.M{
					"_id": op.Id,
				},
				"updateDescription": op.Data,
			}
		} else {
			// Full document replacement
			changeEvent = bson.M{
				"operationType": "replace",
				"ns": bson.M{
					"db":   sourceDB,
					"coll": sourceCollection,
				},
				"documentKey": bson.M{
					"_id": op.Id,
				},
				"fullDocument": op.Data,
			}
		}

	case "d": // delete
		changeEvent = bson.M{
			"operationType": "delete",
			"ns": bson.M{
				"db":   sourceDB,
				"coll": sourceCollection,
			},
			"documentKey": bson.M{
				"_id": op.Id,
			},
		}

	default:
		r.log.Warnf("Unknown operation type: %s", op.Operation)
		return
	}

	// Determine worker based on document ID hash
	workerIndex := hashDocumentID(op.Id) % len(workers)
	if workerIndex < 0 {
		workerIndex = -workerIndex
	}

	changeEvent["readTime"] = time.Now()

	// Send raw event to appropriate worker concurrently
	workers[workerIndex].batchingQueue <- changeEvent
}
