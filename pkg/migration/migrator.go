package migration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gsbingo17/mongodb-migration/pkg/config"
	"github.com/gsbingo17/mongodb-migration/pkg/db"
	"github.com/gsbingo17/mongodb-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Migrator handles the migration and replication process
type Migrator struct {
	config *config.Config
	log    *logger.Logger
}

// NewMigrator creates a new migrator
func NewMigrator(config *config.Config, log *logger.Logger) *Migrator {
	return &Migrator{
		config: config,
		log:    log,
	}
}

// Start starts the migration or replication process
func (m *Migrator) Start(ctx context.Context, mode string) error {
	// Validate mode
	if mode != "migrate" && mode != "live" {
		return fmt.Errorf("invalid mode: %s, must be 'migrate' or 'live'", mode)
	}

	m.log.Infof("Starting MongoDB to MongoDB %s process", mode)

	if mode == "migrate" {
		// Migrate mode: process each database pair sequentially
		for i, pair := range m.config.DatabasePairs {
			m.log.Infof("Processing database pair %d/%d", i+1, len(m.config.DatabasePairs))
			if err := m.processDatabasePair(ctx, pair, i, mode); err != nil {
				if err == context.Canceled {
					m.log.Info("Processing stopped due to user interrupt (Ctrl+C)")
					break
				}
				m.log.Errorf("Error processing database pair %d: %v", i+1, err)
			}
		}
		m.log.Info("Migration completed successfully")
		return nil
	}

	// Live mode: process all database pairs concurrently
	m.log.Infof("Starting live replication for %d database pair(s) concurrently", len(m.config.DatabasePairs))

	var wg sync.WaitGroup
	for i, pair := range m.config.DatabasePairs {
		wg.Add(1)
		go func(index int, dbPair config.DatabasePair) {
			defer wg.Done()
			m.log.Infof("Starting database pair %d/%d", index+1, len(m.config.DatabasePairs))
			if err := m.processDatabasePair(ctx, dbPair, index, mode); err != nil {
				if err == context.Canceled {
					m.log.Infof("Database pair %d stopped due to context cancellation", index+1)
				} else {
					m.log.Errorf("Error processing database pair %d: %v", index+1, err)
				}
			}
		}(i, pair)
	}

	// Wait for interrupt signal
	m.log.Info("Live replication active. Press Ctrl+C to stop.")

	shutdownCtx, cancelFunc := context.WithCancel(ctx)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		m.log.Infof("Received %s signal. Initiating graceful shutdown...", sig)
		cancelFunc()
	}()

	<-shutdownCtx.Done()

	// Wait for all database pairs to finish shutting down
	wg.Wait()

	m.log.Info("Shutdown complete.")
	return nil
}

// getCheckpointPath generates a per-pair checkpoint file path
// For single database pair configs, it uses the legacy "global" naming for backward compatibility
func (m *Migrator) getCheckpointPath(prefix string, pairIndex int) string {
	if len(m.config.DatabasePairs) == 1 {
		return fmt.Sprintf("%s-global.json", prefix)
	}
	return fmt.Sprintf("%s-pair%d.json", prefix, pairIndex)
}

// processDatabasePair processes a single database pair
func (m *Migrator) processDatabasePair(ctx context.Context, pair config.DatabasePair, pairIndex int, mode string) error {
	// Check if this is legacy mode - if so, handle it separately
	if mode == "live" && pair.Source.ReplicationMethod == "oplog-legacy" {
		// For legacy mode, don't connect here - let startOplogReplicationLegacy handle it
		collections := pair.Target.Collections
		if len(collections) == 0 {
			// Auto-detect collections using legacy mgo driver
			m.log.Info("No collections specified for oplog-legacy mode. Auto-detecting all collections...")
			legacyDB, err := db.NewMongoDBLegacy(pair.Source.ConnectionString, pair.Source.Database)
			if err != nil {
				return fmt.Errorf("failed to connect to source MongoDB (legacy) for collection detection: %w", err)
			}
			sourceCollections, err := legacyDB.ListCollections()
			legacyDB.Close()
			if err != nil {
				return fmt.Errorf("failed to list collections from source: %w", err)
			}
			m.log.Infof("Auto-detected %d collections in source database: %v", len(sourceCollections), sourceCollections)
			for _, collName := range sourceCollections {
				collections = append(collections, config.CollectionConfig{
					SourceCollection: collName,
					TargetCollection: collName,
				})
			}
			if len(collections) == 0 {
				m.log.Warn("No collections found in source database")
				return nil
			}
		}
		return m.startOplogReplicationLegacy(ctx, pair.Source.Database, pair.Target.Database, collections, pair, pairIndex)
	}

	// Connect to source MongoDB (modern driver)
	m.log.Infof("Connecting to source MongoDB at %s", pair.Source.ConnectionString)
	sourceDB, err := db.NewMongoDB(pair.Source.ConnectionString, pair.Source.Database, m.log)
	if err != nil {
		return fmt.Errorf("failed to connect to source MongoDB: %w", err)
	}

	// Connect to target MongoDB
	m.log.Infof("Connecting to target MongoDB at %s", pair.Target.ConnectionString)
	targetDB, err := db.NewMongoDB(pair.Target.ConnectionString, pair.Target.Database, m.log)
	if err != nil {
		return fmt.Errorf("failed to connect to target MongoDB: %w", err)
	}

	// Determine collections to process
	collections, err := m.getCollectionsToProcess(ctx, sourceDB, pair.Target.Collections)
	if err != nil {
		return fmt.Errorf("failed to determine collections to process: %w", err)
	}

	// Sync indexes before data migration (if configured)
	if len(pair.Target.Indexes) > 0 {
		if err := m.syncIndexes(ctx, sourceDB, targetDB, pair); err != nil {
			m.log.Warnf("Index sync encountered issues: %v (continuing with migration)", err)
			// Continue with migration even if index sync has issues
		}
	}

	// Process each collection
	if mode == "migrate" {
		// For migrate mode, use a wait group to process collections in parallel
		var wg sync.WaitGroup
		// Create a semaphore to limit concurrency
		// Use the dedicated parameter for concurrent collections
		concurrentCollections := m.config.ConcurrentCollections
		m.log.Infof("Processing up to %d collections concurrently", concurrentCollections)
		semaphore := make(chan struct{}, concurrentCollections)

		for _, collConfig := range collections {
			wg.Add(1)
			// Acquire semaphore
			semaphore <- struct{}{}

			// Start migration in a goroutine
			go func(collConfig config.CollectionConfig) {
				defer wg.Done()
				defer func() { <-semaphore }() // Release semaphore when done

				if err := m.migrateCollection(ctx, sourceDB, targetDB, collConfig); err != nil {
					if err == context.Canceled {
						m.log.Infof("Migration of collection %s interrupted due to user interrupt (Ctrl+C)", collConfig.SourceCollection)
						// Don't report as an error
					} else {
						m.log.Errorf("Error migrating collection %s: %v", collConfig.SourceCollection, err)
					}
					// Continue with other collections even if one fails
				}
			}(collConfig)
		}

		// Wait for all migrations to complete
		wg.Wait()
	} else if mode == "live" {
		// Use client-level change stream for live replication
		if err := m.startClientLevelReplication(ctx, sourceDB, targetDB, pair.Source.Database, pair.Target.Database, collections, pair, pairIndex); err != nil {
			// We don't need to check for context.Canceled here anymore as it's handled in the lower layers
			return fmt.Errorf("error starting client-level replication: %w", err)
		}
	}

	// If in migrate mode, close connections
	if mode == "migrate" {
		if err := sourceDB.Close(ctx); err != nil {
			m.log.Errorf("Error closing source MongoDB connection: %v", err)
		}
		if err := targetDB.Close(ctx); err != nil {
			m.log.Errorf("Error closing target MongoDB connection: %v", err)
		}
	}

	return nil
}

// startClientLevelReplication starts replication using either change streams or oplog
func (m *Migrator) startClientLevelReplication(ctx context.Context, sourceDB, targetDB *db.MongoDB, sourceDBName, targetDBName string, collections []config.CollectionConfig, pair config.DatabasePair, pairIndex int) error {
	// Determine replication method
	replicationMethod := pair.Source.ReplicationMethod
	if replicationMethod == "" {
		replicationMethod = "changestream" // Default to change stream
	}

	m.log.Infof("Starting replication using method: %s", replicationMethod)

	if replicationMethod == "oplog" {
		// Use oplog-based replication
		return m.startOplogReplication(ctx, sourceDB, targetDB, sourceDBName, targetDBName, collections, pair, pairIndex)
	} else {
		// Use change stream-based replication (default)
		return m.startChangeStreamReplication(ctx, sourceDB, targetDB, sourceDBName, targetDBName, collections, pair, pairIndex)
	}
}

// startChangeStreamReplication starts replication using change streams
func (m *Migrator) startChangeStreamReplication(ctx context.Context, sourceDB, targetDB *db.MongoDB, sourceDBName, targetDBName string, collections []config.CollectionConfig, pair config.DatabasePair, pairIndex int) error {
	m.log.Info("Starting change stream-based replication for all collections")

	// Create client-level replicator
	replicator := NewClientLevelReplicator(sourceDB, targetDB, m.config, m.log)

	// Add all collections to the replicator
	for _, collConfig := range collections {
		// Add collection to replicator
		replicator.AddCollection(sourceDBName, targetDBName, collConfig.SourceCollection, collConfig.TargetCollection)
	}

	// Load global resume token if it exists (per-pair path)
	globalResumeTokenPath := m.getCheckpointPath("resumeToken", pairIndex)
	m.log.Infof("Using checkpoint file: %s", globalResumeTokenPath)
	globalResumeToken, err := LoadResumeToken(globalResumeTokenPath)
	if err != nil {
		m.log.Warnf("Error loading global resume token: %v. Will start from the beginning.", err)
		globalResumeToken = nil
	}

	// Start client-level replication (which will handle index sync during initial migration)
	return replicator.StartReplication(ctx, globalResumeToken, globalResumeTokenPath, pair, m)
}

// startOplogReplication starts replication using oplog tailing
func (m *Migrator) startOplogReplication(ctx context.Context, sourceDB, targetDB *db.MongoDB, sourceDBName, targetDBName string, collections []config.CollectionConfig, pair config.DatabasePair, pairIndex int) error {
	m.log.Info("Starting oplog-based replication for all collections")

	// Use modern oplog replicator
	replicator := NewOplogReplicator(sourceDB, targetDB, m.config, m.log)

	// Add all collections to the replicator
	for _, collConfig := range collections {
		replicator.AddCollection(sourceDBName, targetDBName, collConfig.SourceCollection, collConfig.TargetCollection)
	}

	// Load oplog timestamp if it exists (per-pair path)
	oplogTimestampPath := m.getCheckpointPath("oplogTimestamp", pairIndex)
	m.log.Infof("Using checkpoint file: %s", oplogTimestampPath)
	oplogTimestamp, err := LoadOplogTimestamp(oplogTimestampPath)
	if err != nil {
		m.log.Warnf("Error loading oplog timestamp: %v. Will start from the beginning.", err)
		oplogTimestamp = nil
	}

	// Convert to interface{} for compatibility with StartReplication signature
	var globalTimestamp interface{}
	if oplogTimestamp != nil {
		globalTimestamp = oplogTimestamp
	}

	// Start oplog replication (which will handle index sync during initial migration)
	return replicator.StartReplication(ctx, globalTimestamp, oplogTimestampPath, pair, m)
}

// startOplogReplicationLegacy starts replication using legacy GTM + mgo for old MongoDB versions
func (m *Migrator) startOplogReplicationLegacy(ctx context.Context, sourceDBName, targetDBName string, collections []config.CollectionConfig, pair config.DatabasePair, pairIndex int) error {
	m.log.Info("Starting legacy oplog-based replication (using mgo driver for MongoDB 3.0/3.2)")

	// Connect to source MongoDB using legacy driver (mgo)
	m.log.Infof("Connecting to source MongoDB (legacy) at %s", pair.Source.ConnectionString)
	sourceDBLegacy, err := db.NewMongoDBLegacy(pair.Source.ConnectionString, pair.Source.Database)
	if err != nil {
		return fmt.Errorf("failed to connect to source MongoDB (legacy): %w", err)
	}
	defer sourceDBLegacy.Close()

	// Connect to target MongoDB using modern driver
	m.log.Infof("Connecting to target MongoDB (modern) at %s", pair.Target.ConnectionString)
	targetDB, err := db.NewMongoDB(pair.Target.ConnectionString, pair.Target.Database, m.log)
	if err != nil {
		return fmt.Errorf("failed to connect to target MongoDB: %w", err)
	}
	defer targetDB.Close(ctx)

	// Create legacy oplog replicator
	replicator := NewOplogReplicatorLegacy(sourceDBLegacy, targetDB, m.config, m.log)

	// Add all collections to the replicator
	for _, collConfig := range collections {
		replicator.AddCollection(sourceDBName, targetDBName, collConfig.SourceCollection, collConfig.TargetCollection)
	}

	// Load oplog timestamp if it exists (per-pair path)
	oplogTimestampPath := m.getCheckpointPath("oplogTimestamp", pairIndex)
	m.log.Infof("Using checkpoint file: %s", oplogTimestampPath)
	oplogTimestamp, err := LoadOplogTimestamp(oplogTimestampPath)
	if err != nil {
		m.log.Warnf("Error loading oplog timestamp: %v. Will start from the beginning.", err)
		oplogTimestamp = nil
	}

	// Convert to interface{} for compatibility with StartReplication signature
	var globalTimestamp interface{}
	if oplogTimestamp != nil {
		globalTimestamp = oplogTimestamp
	}

	// Start legacy oplog replication
	return replicator.StartReplication(ctx, globalTimestamp, oplogTimestampPath, pair, m)
}

// getCollectionsToProcess determines which collections to process
func (m *Migrator) getCollectionsToProcess(ctx context.Context, sourceDB *db.MongoDB, configCollections []config.CollectionConfig) ([]config.CollectionConfig, error) {
	// If collections are specified in config, use them
	if len(configCollections) > 0 {
		m.log.Infof("Using %d collections specified in config", len(configCollections))
		return configCollections, nil
	}

	// Otherwise, auto-detect all collections in the source database
	m.log.Info("No collections specified in config. Auto-detecting all collections...")
	sourceCollections, err := sourceDB.ListCollections(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list collections: %w", err)
	}

	m.log.Infof("Found %d collections in source database: %v", len(sourceCollections), sourceCollections)

	// Create collection configs with same name for source and target
	var collections []config.CollectionConfig
	for _, collName := range sourceCollections {
		collections = append(collections, config.CollectionConfig{
			SourceCollection: collName,
			TargetCollection: collName,
		})
	}

	return collections, nil
}

// migrateCollection performs a one-time migration of a collection with parallel batch processing
func (m *Migrator) migrateCollection(ctx context.Context, sourceDB, targetDB *db.MongoDB, collConfig config.CollectionConfig) error {
	// Get source and target collections
	sourceCollection := sourceDB.GetCollection(collConfig.SourceCollection)
	targetCollection := targetDB.GetCollection(collConfig.TargetCollection)

	m.log.Infof("Migrating collection: %s.%s to %s.%s", sourceDB.GetDatabaseName(), collConfig.SourceCollection, targetDB.GetDatabaseName(), collConfig.TargetCollection)

	// Get total count for progress reporting
	totalCount, err := sourceCollection.CountDocuments(ctx, bson.D{})
	if err != nil {
		return fmt.Errorf("failed to count documents: %w", err)
	}

	m.log.Infof("Found %d documents to migrate", totalCount)

	// If no documents, we're done
	if totalCount == 0 {
		m.log.Infof("No documents to migrate for collection %s", collConfig.SourceCollection)
		return nil
	}

	// Check if parallel reads are enabled and collection is large enough
	if m.config.ParallelReadsEnabled && totalCount >= int64(m.config.MinDocsForParallelReads) {
		m.log.Infof("Using parallel reads for large collection: %s (%d documents)", collConfig.SourceCollection, totalCount)
		return m.migrateCollectionParallel(ctx, sourceDB, targetDB, collConfig, totalCount)
	}

	// Set up batch processing using configuration parameters
	readBatchSize := m.config.InitialReadBatchSize
	writeBatchSize := m.config.InitialWriteBatchSize

	m.log.Infof("Using read batch size: %d, write batch size: %d", readBatchSize, writeBatchSize)

	// Create retry manager for batch processing
	retryManager := NewRetryManager(
		m.config.RetryConfig.MaxRetries,
		time.Duration(m.config.RetryConfig.BaseDelayMs)*time.Millisecond,
		time.Duration(m.config.RetryConfig.MaxDelayMs)*time.Millisecond,
		m.config.RetryConfig.EnableBatchSplitting,
		m.config.RetryConfig.MinBatchSize,
		m.config.RetryConfig.ConvertInvalidIds,
		m.log,
	)

	cursor, err := sourceCollection.Find(ctx, bson.D{}, options.Find().SetBatchSize(int32(readBatchSize)))
	if err != nil {
		return fmt.Errorf("failed to create cursor: %w", err)
	}
	defer cursor.Close(ctx)

	// Set up parallel batch processing
	var wg sync.WaitGroup
	channelBufferSize := m.config.InitialChannelBufferSize
	batchChan := make(chan []interface{}, channelBufferSize) // Buffer for batches
	errorChan := make(chan error, 1)                         // Channel for errors
	doneChan := make(chan struct{})                          // Channel to signal completion

	// Track progress
	var migratedCount int64
	var lastLoggedPercentage int = -1 // Start at -1 to ensure 0% is logged
	var mu sync.Mutex                 // Mutex for thread-safe updates to migratedCount and lastLoggedPercentage

	// Start worker pool for parallel batch processing
	workerCount := m.config.InitialMigrationWorkers
	m.log.Infof("Starting %d workers for parallel document batch processing", workerCount)

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for batch := range batchChan {
				// Use RetryManager to handle retries with batch splitting
				err := retryManager.RetryWithSplit(ctx, batch, collConfig.SourceCollection, func(b []interface{}) error {
					return processBatch(ctx, targetCollection, b, collConfig.UpsertMode, m.log, sourceDB.GetDatabaseName(), collConfig.SourceCollection)
				})
				if err != nil {
					select {
					case errorChan <- fmt.Errorf("worker %d failed to process batch: %w", workerID, err):
					default:
						// Error channel already has an error
					}
					return
				}

				// Update progress
				mu.Lock()
				migratedCount += int64(len(batch))
				currentCount := migratedCount // Copy for logging outside the lock

				// Calculate current percentage (0-10 for 0%-100%)
				currentPercentage := int(float64(currentCount) / float64(totalCount) * 10)

				// Only log when crossing a 10% threshold at the collection level
				// and update lastLoggedPercentage atomically to prevent multiple logs
				shouldLog := false
				if currentPercentage > lastLoggedPercentage {
					lastLoggedPercentage = currentPercentage
					shouldLog = true
				}
				mu.Unlock()

				// Log outside the mutex lock to reduce lock contention
				if shouldLog {
					m.log.Infof("Collection %s progress: %d/%d documents (%.0f%%)",
						collConfig.SourceCollection, currentCount, totalCount, float64(currentPercentage)*10)
				}
			}
		}(i)
	}

	// Start a goroutine to close channels when all batches are processed
	go func() {
		wg.Wait()
		close(doneChan)
	}()

	// Process documents and create batches
	var batch []interface{}
	var batchCount int

	for {
		// Check for errors from workers
		select {
		case err := <-errorChan:
			cursor.Close(ctx)
			close(batchChan)
			return err
		default:
			// No errors, continue processing
		}

		// Get next document
		if !cursor.Next(ctx) {
			break
		}

		// Decode document
		var doc bson.D
		if err := cursor.Decode(&doc); err != nil {
			close(batchChan)
			return fmt.Errorf("failed to decode document: %w", err)
		}

		// Add to batch
		batch = append(batch, doc)
		batchCount++

		// Send batch if it reaches the write batch size
		if batchCount >= writeBatchSize {
			select {
			case batchChan <- batch:
				// Batch sent to worker
			case err := <-errorChan:
				// Error from a worker
				cursor.Close(ctx)
				close(batchChan)
				return err
			case <-ctx.Done():
				// Context cancelled
				cursor.Close(ctx)
				close(batchChan)
				m.log.Info("Batch processing interrupted due to context cancellation")
				return context.Canceled // Return context.Canceled for consistent error handling
			}

			// Reset batch
			batch = nil
			batchCount = 0

			// Add a small delay between batches to reduce contention
			time.Sleep(5 * time.Millisecond)
		}
	}

	// Check for cursor errors
	if err := cursor.Err(); err != nil {
		close(batchChan)
		return fmt.Errorf("cursor error: %w", err)
	}

	// Process any remaining documents
	if len(batch) > 0 {
		select {
		case batchChan <- batch:
			// Final batch sent to worker
		case err := <-errorChan:
			// Error from a worker
			close(batchChan)
			return err
		case <-ctx.Done():
			// Context cancelled
			close(batchChan)
			m.log.Info("Final batch processing interrupted due to context cancellation")
			return context.Canceled // Return context.Canceled for consistent error handling
		}
	}

	// Close batch channel to signal workers to exit
	close(batchChan)

	// Wait for all workers to finish or for an error
	select {
	case <-doneChan:
		// All workers finished successfully
	case err := <-errorChan:
		// Error from a worker
		return err
	case <-ctx.Done():
		// Context cancelled
		m.log.Info("Migration interrupted due to context cancellation")
		return context.Canceled // Return context.Canceled for consistent error handling
	}

	m.log.Infof("Migration for %s completed successfully! Total documents: %d", collConfig.SourceCollection, migratedCount)
	return nil
}

// processBatch processes a batch of documents
func processBatch(ctx context.Context, collection *mongo.Collection, batch []interface{}, useUpsert bool, log *logger.Logger, dbName, collName string) error {
	if len(batch) == 0 {
		return nil
	}

	// Transform __*__ field names to _*_ for Firestore compatibility
	batch = TransformBatch(batch, log, dbName, collName)

	// If upsert mode is enabled, use upsert operations directly
	if useUpsert {
		var models []mongo.WriteModel
		for _, doc := range batch {
			// Extract the _id from the document
			var id interface{}
			switch d := doc.(type) {
			case bson.D:
				for _, elem := range d {
					if elem.Key == "_id" {
						id = elem.Value
						break
					}
				}
			case bson.M:
				id = d["_id"]
			}

			if id != nil {
				// Create a replace model with upsert
				model := mongo.NewReplaceOneModel().
					SetFilter(bson.M{"_id": id}).
					SetReplacement(doc).
					SetUpsert(true)
				models = append(models, model)
			}
		}

		// Execute the bulk write with the upsert models
		if len(models) > 0 {
			_, err := collection.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
			return err
		}
		return nil
	}

	// Try to insert the batch
	_, err := collection.InsertMany(ctx, batch, options.InsertMany().SetOrdered(false))
	if err == nil {
		return nil
	}

	// If there's an error, check if it's a bulk write error with duplicate key errors
	var bulkWriteErr mongo.BulkWriteException
	if errors.As(err, &bulkWriteErr) {
		// Check if all errors are duplicate key errors
		allDuplicateKeyErrors := true
		for _, writeErr := range bulkWriteErr.WriteErrors {
			if writeErr.Code != 11000 { // 11000 is the code for duplicate key error
				allDuplicateKeyErrors = false
				break
			}
		}

		if allDuplicateKeyErrors {
			// Use upsert for the failed documents
			var models []mongo.WriteModel
			failedIndices := make(map[int]bool)

			// Mark the failed indices
			for _, writeErr := range bulkWriteErr.WriteErrors {
				failedIndices[writeErr.Index] = true
			}

			// Create upsert models for the failed documents
			for i, doc := range batch {
				if failedIndices[i] {
					// Extract the _id from the document
					var id interface{}
					switch d := doc.(type) {
					case bson.D:
						for _, elem := range d {
							if elem.Key == "_id" {
								id = elem.Value
								break
							}
						}
					case bson.M:
						id = d["_id"]
					}

					if id != nil {
						// Create a replace model with upsert
						model := mongo.NewReplaceOneModel().
							SetFilter(bson.M{"_id": id}).
							SetReplacement(doc).
							SetUpsert(true)
						models = append(models, model)
					}
				}
			}

			// Execute the bulk write with the upsert models
			if len(models) > 0 {
				_, err := collection.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
				return err
			}

			// If no models were created, return nil
			return nil
		}
	}

	// For other errors, return the original error
	return err
}

// Note: The startLiveReplication function has been replaced by the client-level
// change stream approach in the startClientLevelReplication function.

// migrateCollectionParallel performs a one-time migration of a collection using parallel reads
func (m *Migrator) migrateCollectionParallel(ctx context.Context, sourceDB, targetDB *db.MongoDB, collConfig config.CollectionConfig, totalCount int64) error {
	sourceCollection := sourceDB.GetCollection(collConfig.SourceCollection)
	targetCollection := targetDB.GetCollection(collConfig.TargetCollection)

	// Create partitioner
	partitioner := NewCollectionPartitioner(
		sourceCollection,
		m.log,
		m.config.MaxReadPartitions,
		m.config.MinDocsPerPartition,
		m.config.SampleSize,
	)

	// Create partitions
	partitions, err := partitioner.Partition(ctx)
	if err != nil {
		return fmt.Errorf("failed to create partitions: %w", err)
	}

	m.log.Infof("Created %d partitions for collection %s", len(partitions), collConfig.SourceCollection)

	// Create retry manager for batch processing
	retryManager := NewRetryManager(
		m.config.RetryConfig.MaxRetries,
		time.Duration(m.config.RetryConfig.BaseDelayMs)*time.Millisecond,
		time.Duration(m.config.RetryConfig.MaxDelayMs)*time.Millisecond,
		m.config.RetryConfig.EnableBatchSplitting,
		m.config.RetryConfig.MinBatchSize,
		m.config.RetryConfig.ConvertInvalidIds,
		m.log,
	)

	// Process partitions in parallel
	var wg sync.WaitGroup
	errorChan := make(chan error, len(partitions))
	doneChan := make(chan struct{})

	// Track progress
	var migratedCount int64
	var mu sync.Mutex
	var lastLoggedPercentage int = -1 // Start at -1 to ensure 0% is logged

	// Start a goroutine to periodically report progress at the collection level
	progressCtx, cancelProgress := context.WithCancel(ctx)
	defer cancelProgress()

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				mu.Lock()
				currentCount := migratedCount
				currentPercentage := int(float64(currentCount) / float64(totalCount) * 10)

				// Only log when crossing a 10% threshold
				if currentPercentage > lastLoggedPercentage {
					m.log.Infof("Collection %s progress: %d/%d documents (%.0f%%)",
						collConfig.SourceCollection, currentCount, totalCount, float64(currentPercentage)*10)
					lastLoggedPercentage = currentPercentage
				}
				mu.Unlock()
			case <-progressCtx.Done():
				return
			case <-doneChan:
				// Log final progress
				mu.Lock()
				currentCount := migratedCount
				m.log.Infof("Collection %s completed: %d/%d documents (100%%)",
					collConfig.SourceCollection, currentCount, totalCount)
				mu.Unlock()
				return
			}
		}
	}()

	// Process each partition
	for i, partition := range partitions {
		wg.Add(1)

		go func(partitionIndex int, filter bson.D) {
			defer wg.Done()

			m.log.Debugf("Starting partition %d with filter: %v", partitionIndex, filter)

			// Create cursor for this partition
			cursor, err := sourceCollection.Find(ctx, filter, options.Find().SetBatchSize(int32(m.config.InitialReadBatchSize)))
			if err != nil {
				errorChan <- fmt.Errorf("failed to create cursor for partition %d: %w", partitionIndex, err)
				return
			}
			defer cursor.Close(ctx)

			// Set up parallel batch processing within this partition
			var partitionWg sync.WaitGroup
			partitionBatchChan := make(chan []interface{}, m.config.InitialChannelBufferSize) // Buffer for batches
			partitionErrorChan := make(chan error, 1)                                         // Channel for errors
			partitionDoneChan := make(chan struct{})                                          // Channel to signal completion

			// Track progress for this partition
			var partitionMigratedCount int64
			var partitionMu sync.Mutex // Mutex for thread-safe updates to partitionMigratedCount

			// Start worker pool for this partition
			workerCount := m.config.WorkersPerPartition
			if workerCount < 1 {
				workerCount = 1 // Ensure at least 1 worker per partition
			}

			m.log.Debugf("Starting %d workers for partition %d", workerCount, partitionIndex)

			for w := 0; w < workerCount; w++ {
				partitionWg.Add(1)
				go func(workerID int) {
					defer partitionWg.Done()

					for batch := range partitionBatchChan {
						// Use RetryManager to handle retries with batch splitting
						err := retryManager.RetryWithSplit(ctx, batch, collConfig.SourceCollection, func(b []interface{}) error {
							return processBatch(ctx, targetCollection, b, collConfig.UpsertMode, m.log, sourceDB.GetDatabaseName(), collConfig.SourceCollection)
						})

						if err != nil {
							select {
							case partitionErrorChan <- fmt.Errorf("worker %d in partition %d failed: %w", workerID, partitionIndex, err):
							default:
								// Error channel already has an error
							}
							return
						}

						// Update progress
						partitionMu.Lock()
						partitionMigratedCount += int64(len(batch))
						partitionMu.Unlock()

						// Update overall progress counter
						mu.Lock()
						migratedCount += int64(len(batch))
						mu.Unlock()
					}
				}(w)
			}

			// Start a goroutine to close channels when all batches are processed
			go func() {
				partitionWg.Wait()
				close(partitionDoneChan)
			}()

			// Process documents and create batches
			var batch []interface{}
			var batchCount int

			for {
				// Check for errors from workers
				select {
				case err := <-partitionErrorChan:
					cursor.Close(ctx)
					close(partitionBatchChan)
					errorChan <- err
					return
				default:
					// No errors, continue processing
				}

				// Get next document
				if !cursor.Next(ctx) {
					break
				}

				// Decode document
				var doc bson.D
				if err := cursor.Decode(&doc); err != nil {
					close(partitionBatchChan)
					errorChan <- fmt.Errorf("failed to decode document in partition %d: %w", partitionIndex, err)
					return
				}

				// Add to batch
				batch = append(batch, doc)
				batchCount++

				// Send batch if it reaches the write batch size
				if batchCount >= m.config.InitialWriteBatchSize {
					select {
					case partitionBatchChan <- batch:
						// Batch sent to worker
					case err := <-partitionErrorChan:
						// Error from a worker
						cursor.Close(ctx)
						close(partitionBatchChan)
						errorChan <- err
						return
					case <-ctx.Done():
						// Context cancelled
						cursor.Close(ctx)
						close(partitionBatchChan)
						errorChan <- ctx.Err()
						return
					}

					// Reset batch
					batch = nil
					batchCount = 0
				}
			}

			// Check for cursor errors
			if err := cursor.Err(); err != nil {
				close(partitionBatchChan)
				errorChan <- fmt.Errorf("cursor error in partition %d: %w", partitionIndex, err)
				return
			}

			// Process any remaining documents
			if len(batch) > 0 {
				select {
				case partitionBatchChan <- batch:
					// Final batch sent to worker
				case err := <-partitionErrorChan:
					// Error from a worker
					close(partitionBatchChan)
					errorChan <- err
					return
				case <-ctx.Done():
					// Context cancelled
					close(partitionBatchChan)
					errorChan <- ctx.Err()
					return
				}
			}

			// Close batch channel to signal workers to exit
			close(partitionBatchChan)

			// Wait for all workers to finish or for an error
			select {
			case <-partitionDoneChan:
				// All workers finished successfully
			case err := <-partitionErrorChan:
				// Error from a worker
				errorChan <- err
				return
			case <-ctx.Done():
				// Context cancelled
				errorChan <- ctx.Err()
				return
			}

			// Log partition completion at debug level only
			m.log.Debugf("Partition %d completed: %d documents",
				partitionIndex, partitionMigratedCount)
		}(i, partition)
	}

	// Wait for all partitions to complete or for an error
	go func() {
		wg.Wait()
		close(errorChan)
		close(doneChan) // Signal progress reporting goroutine to exit
	}()

	// Check for errors
	for err := range errorChan {
		if err != nil {
			return err
		}
	}

	m.log.Infof("Parallel migration for %s completed successfully! Total documents: %d",
		collConfig.SourceCollection, migratedCount)
	return nil
}

// Helper functions for min/max
// func min(a, b int) int {
// 	if a < b {
// 		return a
// 	}
// 	return b
// }

// syncIndexes synchronizes indexes from source to target collections
func (m *Migrator) syncIndexes(ctx context.Context, sourceDB, targetDB *db.MongoDB, pair config.DatabasePair) error {
	if len(pair.Target.Indexes) == 0 {
		m.log.Debug("No indexes configured for sync")
		return nil
	}

	m.log.Info("Starting index synchronization...")

	for _, indexConfig := range pair.Target.Indexes {
		m.log.Infof("Syncing indexes for collection: %s", indexConfig.SourceCollection)

		// Get all indexes from source collection
		sourceIndexes, err := sourceDB.ListIndexes(ctx, indexConfig.SourceCollection)
		if err != nil {
			m.log.Warnf("Failed to list indexes for %s: %v (continuing anyway)", indexConfig.SourceCollection, err)
			continue
		}

		// Get target collection name
		targetCollName := m.getTargetCollectionName(indexConfig.SourceCollection, pair)

		// Filter to only requested indexes
		for _, indexDef := range sourceIndexes {
			indexName, ok := indexDef["name"].(string)
			if !ok {
				m.log.Warnf("Index definition missing name field: %v", indexDef)
				continue
			}

			// Skip _id_ index (MongoDB creates this automatically)
			if indexName == "_id_" {
				continue
			}

			// Check if this index is in the requested list
			found := false
			for _, requestedName := range indexConfig.IndexNames {
				if indexName == requestedName {
					found = true
					break
				}
			}

			if !found {
				continue
			}

			// Create index on target
			m.log.Infof("Creating index '%s' on target collection '%s'", indexName, targetCollName)
			if err := targetDB.CreateIndexFromDefinition(ctx, targetCollName, indexDef); err != nil {
				// Log warning but continue - index creation failures are non-blocking
				m.log.Warnf("Failed to create index '%s': %v (continuing anyway)", indexName, err)
			} else {
				m.log.Infof("Successfully created index '%s'", indexName)
			}
		}
	}

	m.log.Info("Index synchronization completed")
	return nil
}

// getTargetCollectionName gets the target collection name for a source collection
func (m *Migrator) getTargetCollectionName(sourceCollName string, pair config.DatabasePair) string {
	// Search through collections configuration for explicit mapping
	for _, coll := range pair.Target.Collections {
		if coll.SourceCollection == sourceCollName {
			return coll.TargetCollection
		}
	}

	// If not found in explicit config, assume same name as source
	m.log.Debugf("No explicit mapping for %s, using same collection name on target", sourceCollName)
	return sourceCollName
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
