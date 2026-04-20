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
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// OplogReplicator handles replication using oplog tailing via GTM library
type OplogReplicator struct {
	sourceDB      *db.MongoDB
	targetDB      *db.MongoDB
	config        *config.Config
	log           *logger.Logger
	collectionMap map[string]map[string]string // Map of database -> source collection -> target collection
	mu            sync.Mutex                   // Mutex for thread-safe operations
}

// NewOplogReplicator creates a new oplog-based replicator
func NewOplogReplicator(sourceDB, targetDB *db.MongoDB, cfg *config.Config, log *logger.Logger) *OplogReplicator {
	return &OplogReplicator{
		sourceDB:      sourceDB,
		targetDB:      targetDB,
		config:        cfg,
		log:           log,
		collectionMap: make(map[string]map[string]string),
	}
}

// AddCollection adds a collection to be watched
func (r *OplogReplicator) AddCollection(sourceDB, targetDB, sourceCollection, targetCollection string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Initialize maps if needed
	if r.collectionMap[sourceDB] == nil {
		r.collectionMap[sourceDB] = make(map[string]string)
	}

	// Add collection mapping
	r.collectionMap[sourceDB][sourceCollection] = targetCollection

	r.log.Infof("Added collection mapping: %s.%s -> %s.%s", sourceDB, sourceCollection, targetDB, targetCollection)
}

// StartReplication starts the oplog-based replication
func (r *OplogReplicator) StartReplication(ctx context.Context, globalTimestamp interface{}, timestampPath string, pair config.DatabasePair, migrator *Migrator) error {
	var needsInitialMigration bool
	var afterTimestamp primitive.Timestamp

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

	// If no timestamp is available, get current cluster time and perform initial migration
	if savedTimestamp == nil {
		r.log.Info("No oplog timestamp found. Capturing current cluster time and will perform initial migration.")

		// Get current cluster time as starting point
		currentTime, err := r.getCurrentClusterTime(ctx)
		if err != nil {
			return fmt.Errorf("failed to get current cluster time: %w", err)
		}

		afterTimestamp = currentTime
		savedTimestamp = &OplogTimestamp{Timestamp: currentTime}

		// Save this timestamp
		if err := SaveOplogTimestamp(timestampPath, *savedTimestamp); err != nil {
			r.log.Errorf("Error saving oplog timestamp: %v", err)
		} else {
			r.log.Infof("Saved oplog timestamp: %v", savedTimestamp)
		}

		needsInitialMigration = true
	} else {
		r.log.Info("Oplog timestamp available. Starting incremental replication.")
		afterTimestamp = savedTimestamp.Timestamp
	}

	// Perform initial migration if needed (same logic as change stream replicator)
	if needsInitialMigration {
		if err := r.performInitialMigration(ctx, pair, migrator); err != nil {
			return fmt.Errorf("initial migration failed: %w", err)
		}
		r.log.Info("Initial migration completed. Starting incremental replication.")
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

// performInitialMigration performs the initial migration (code reused from client_stream.go)
func (r *OplogReplicator) performInitialMigration(ctx context.Context, pair config.DatabasePair, migrator *Migrator) error {
	initialMigrationStart := time.Now()
	r.log.Info("Performing initial migration for all collections")

	// Sync indexes before migrating data if configured
	if pair.Target.SyncAllIndexes || len(pair.Target.Indexes) > 0 {
		r.log.Info("Syncing indexes before initial migration")
		// Build collections list from collectionMap
		var collections []config.CollectionConfig
		for _, colls := range r.collectionMap {
			for src, tgt := range colls {
				collections = append(collections, config.CollectionConfig{
					SourceCollection: src,
					TargetCollection: tgt,
				})
			}
		}
		if err := migrator.syncIndexes(ctx, r.sourceDB, r.targetDB, pair, collections); err != nil {
			r.log.Warnf("Index sync encountered issues: %v (continuing with migration)", err)
		}
	}

	// Use a semaphore to limit the number of concurrent collection migrations
	semaphore := make(chan struct{}, r.config.InitialMigrationWorkers)
	var wg sync.WaitGroup

	// Track overall statistics
	var totalMigratedCount int64
	var totalCollections int
	var mu sync.Mutex

	// Iterate through all collections in the map
	for sourceDB, collections := range r.collectionMap {
		for sourceCollection, targetCollection := range collections {
			wg.Add(1)
			totalCollections++

			// Acquire semaphore
			semaphore <- struct{}{}

			// Start migration in a goroutine
			go func(sourceDB, sourceCollection, targetCollection string) {
				defer wg.Done()
				defer func() { <-semaphore }()

				r.log.Infof("Starting initial migration for %s.%s to %s", sourceDB, sourceCollection, targetCollection)

				// Get source and target collections
				sourceDBCollection := r.sourceDB.GetCollection(sourceCollection)
				targetDBCollection := r.targetDB.GetCollection(targetCollection)

				// Count documents
				count, err := sourceDBCollection.CountDocuments(ctx, bson.D{})
				if err != nil {
					r.log.Errorf("Error counting documents in %s.%s: %v", sourceDB, sourceCollection, err)
					return
				}

				r.log.Infof("Found %d documents to migrate in %s.%s", count, sourceDB, sourceCollection)

				// Skip if no documents
				if count == 0 {
					r.log.Infof("No documents to migrate for %s.%s", sourceDB, sourceCollection)
					return
				}

				// Perform migration using existing parallel migration logic
				migratedCount := r.migrateCollection(ctx, sourceDBCollection, targetDBCollection, count, sourceDB, sourceCollection)

				// Update overall statistics
				mu.Lock()
				totalMigratedCount += migratedCount
				mu.Unlock()
			}(sourceDB, sourceCollection, targetCollection)
		}
	}

	// Wait for all collection migrations to complete
	wg.Wait()

	initialMigrationDuration := time.Since(initialMigrationStart)
	r.log.Infof("Initial migration completed in %.2f seconds. Total collections: %d, Total documents: %d",
		initialMigrationDuration.Seconds(), totalCollections, totalMigratedCount)

	return nil
}

// migrateCollection migrates a single collection with sophisticated error handling
func (r *OplogReplicator) migrateCollection(ctx context.Context, sourceCol, targetCol *mongo.Collection, count int64, sourceDB, sourceCollection string) int64 {
	readBatchSize := r.config.InitialReadBatchSize
	writeBatchSize := r.config.InitialWriteBatchSize

	cursor, err := sourceCol.Find(ctx, bson.D{}, options.Find().SetBatchSize(int32(readBatchSize)))
	if err != nil {
		r.log.Errorf("Error creating cursor for %s.%s: %v", sourceDB, sourceCollection, err)
		return 0
	}
	defer cursor.Close(ctx)

	var batch []interface{}
	var migratedCount int64

	for cursor.Next(ctx) {
		var doc bson.D
		if err := cursor.Decode(&doc); err != nil {
			r.log.Errorf("[%s.%s] Error decoding document: %v", sourceDB, sourceCollection, err)
			continue
		}

		batch = append(batch, doc)

		if len(batch) >= writeBatchSize {
			migratedCount += r.insertBatchWithRetry(ctx, targetCol, batch, sourceDB, sourceCollection)
			batch = nil
		}
	}

	// Insert remaining documents
	if len(batch) > 0 {
		migratedCount += r.insertBatchWithRetry(ctx, targetCol, batch, sourceDB, sourceCollection)
	}

	r.log.Infof("Migration for %s.%s completed: %d documents", sourceDB, sourceCollection, migratedCount)
	return migratedCount
}

// insertBatchWithRetry inserts a batch of documents with sophisticated error handling
// Returns the count of successfully inserted documents
func (r *OplogReplicator) insertBatchWithRetry(ctx context.Context, targetCol *mongo.Collection, batch []interface{}, sourceDB, sourceCollection string) int64 {
	// Transform __*__ field names to _*_ for Firestore compatibility
	batch = TransformBatch(batch, r.log, sourceDB, sourceCollection)

	var successCount int64

	if _, err := targetCol.InsertMany(ctx, batch, options.InsertMany().SetOrdered(false)); err != nil {
		bulkWriteException, ok := err.(mongo.BulkWriteException)
		if ok {
			// Calculate successful inserts
			successCount = int64(len(batch) - len(bulkWriteException.WriteErrors))

			if len(bulkWriteException.WriteErrors) > 0 {
				r.log.Debugf("Bulk insert partially failed for %s.%s: %d succeeded, %d failed",
					sourceDB, sourceCollection, successCount, len(bulkWriteException.WriteErrors))
			}

			// Process individual errors
			for _, writeErr := range bulkWriteException.WriteErrors {
				// Check if it's a duplicate key error (code 11000)
				if writeErr.Code == 11000 {
					// Use upsert for duplicate key errors
					if writeErr.Index < len(batch) {
						doc := batch[writeErr.Index]

						// Extract document ID for filter
						var docID interface{}
						switch d := doc.(type) {
						case bson.D:
							for _, elem := range d {
								if elem.Key == "_id" {
									docID = elem.Value
									break
								}
							}
						case bson.M:
							docID = d["_id"]
						}

						if docID != nil {
							filter := bson.M{"_id": docID}
							if _, err := targetCol.ReplaceOne(ctx, filter, doc, options.Replace().SetUpsert(true)); err != nil {
								r.log.Debugf("Upsert fallback failed for document %v in %s.%s: %v",
									docID, sourceDB, sourceCollection, err)
							} else {
								r.log.Debugf("Successfully upserted document %v in %s.%s after duplicate key error",
									docID, sourceDB, sourceCollection)
								successCount++
							}
						}
					}
				} else {
					// For non-duplicate key errors, log and retry with individual insert
					r.log.Debugf("Insert error at index %d in %s.%s: %v",
						writeErr.Index, sourceDB, sourceCollection, writeErr.Message)

					if writeErr.Index < len(batch) {
						// Extract document ID for logging
						var retryDocID interface{}
						switch d := batch[writeErr.Index].(type) {
						case bson.D:
							for _, elem := range d {
								if elem.Key == "_id" {
									retryDocID = elem.Value
									break
								}
							}
						case bson.M:
							retryDocID = d["_id"]
						}

						if _, err := targetCol.InsertOne(ctx, batch[writeErr.Index]); err != nil {
							r.log.Errorf("[%s.%s] Retry insert failed for document _id=%v: %v",
								sourceDB, sourceCollection, retryDocID, err)
						} else {
							successCount++
						}
					}
				}
			}
		} else {
			// Handle non-bulk write errors
			if err == context.Canceled {
				r.log.Debugf("Bulk insert canceled for %s.%s due to context cancellation", sourceDB, sourceCollection)
			} else {
				r.log.Errorf("Error performing bulk insert for %s.%s: %v", sourceDB, sourceCollection, err)
			}

			// Fall back to individual operations with upsert for all documents
			for _, doc := range batch {
				// Try insert first
				if _, err := targetCol.InsertOne(ctx, doc); err != nil {
					// If insert fails, try upsert
					var docID interface{}
					switch d := doc.(type) {
					case bson.D:
						for _, elem := range d {
							if elem.Key == "_id" {
								docID = elem.Value
								break
							}
						}
					case bson.M:
						docID = d["_id"]
					}

					if docID != nil {
						filter := bson.M{"_id": docID}
						if _, err := targetCol.ReplaceOne(ctx, filter, doc, options.Replace().SetUpsert(true)); err != nil {
							if err == context.Canceled {
								r.log.Debugf("Upserting document %v in %s.%s canceled due to context cancellation",
									docID, sourceDB, sourceCollection)
							} else {
								r.log.Errorf("Error upserting document %v in %s.%s: %v",
									docID, sourceDB, sourceCollection, err)
							}
						} else {
							r.log.Debugf("Successfully upserted document %v in %s.%s after insert failed",
								docID, sourceDB, sourceCollection)
							successCount++
						}
					}
				} else {
					successCount++
				}
			}
		}
	} else {
		// All documents inserted successfully
		successCount = int64(len(batch))
		r.log.Debugf("Bulk inserted %d documents successfully in %s.%s", len(batch), sourceDB, sourceCollection)
	}

	return successCount
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
	workers := make([]*Worker, r.config.IncrementalWorkerCount)
	for i := 0; i < r.config.IncrementalWorkerCount; i++ {
		workers[i] = NewWorker(i, ctx, r.log, r.targetDB, r.collectionMap, r.config.IncrementalWriteBatchSize, r.config.ForceOrderedOperations)
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
	flushTicker := time.NewTicker(flushInterval)
	defer flushTicker.Stop()

	go func() {
		for {
			select {
			case <-flushTicker.C:
				// Check all workers for groups that need flushing
				for _, worker := range workers {
					worker.mu.Lock()
					if worker.currentGroup != nil && len(worker.currentGroup.Operations) > 0 {
						// If the group has been waiting for more than the flush interval
						if time.Since(worker.currentGroup.CreatedAt) >= flushInterval {
							r.log.Debugf("Flushing group in worker %d due to timeout: %s.%s with %d operations",
								worker.id, worker.currentGroup.Namespace, worker.currentGroup.OpType,
								len(worker.currentGroup.Operations))

							worker.flushCurrentGroup()
						}
					}
					worker.mu.Unlock()
				}
			case <-ctx.Done():
				return
			}
		}
	}()

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

			// Convert oplog event to worker event format and distribute to workers
			r.distributeOplogEvent(ctx, op, workers)

			r.mu.Lock()
			processedCount++
			eventsSinceLastStats++
			r.mu.Unlock()

			// Periodic checkpoint
			r.mu.Lock()
			shouldCheckpoint := processedCount >= r.config.SaveThreshold || time.Since(lastCheckpoint) >= time.Duration(r.config.CheckpointInterval)*time.Minute
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

	// Send event to appropriate worker
	workers[workerIndex].ProcessEvent(changeEvent)
}
