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
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Migrator handles the migration and replication process
type Migrator struct {
	config       *config.Config
	log          *logger.Logger
	CdcStartTime *primitive.Timestamp
	DontApply    bool
	DryRun       bool
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
	if mode != "migrate" && mode != "live" && mode != "live-only" {
		return fmt.Errorf("invalid mode: %s, must be 'migrate', 'live', or 'live-only'", mode)
	}

	// Validate dont-apply constraint: dont-apply is only supported for 'live-only' mode
	if m.DontApply && mode != "live-only" {
		return fmt.Errorf("dont-apply mode is only supported for 'live-only' migrations")
	}

	// Validate dry run constraint: dry run is only supported for 'live-only' mode
	if m.DryRun && mode != "live-only" {
		return fmt.Errorf("dry-run mode is only supported for 'live-only' migrations")
	}

	// Validate mutual exclusivity of dont-apply and dry-run
	if m.DontApply && m.DryRun {
		return fmt.Errorf("dont-apply and dry-run modes are mutually exclusive")
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

	// Wait for interrupt signal or all pairs to complete
	m.log.Info("Live replication active. Press Ctrl+C to stop.")

	shutdownCtx, cancelFunc := context.WithCancel(ctx)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		m.log.Infof("Received %s signal. Initiating graceful shutdown...", sig)
		cancelFunc()
	}()

	// Also cancel when all database pairs complete (e.g., all indexOnly pairs finish)
	go func() {
		wg.Wait()
		m.log.Info("All database pairs completed. Shutting down.")
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

// getDLQPath generates a per-pair DLQ file path
func (m *Migrator) getDLQPath(pairIndex int) string {
	if len(m.config.DatabasePairs) == 1 {
		return "dlq-global.jsonl"
	}
	return fmt.Sprintf("dlq-pair%d.jsonl", pairIndex)
}

// getInitialMigrationStatePath generates a per-pair initial migration state file path
func (m *Migrator) getInitialMigrationStatePath(pairIndex int) string {
	if len(m.config.DatabasePairs) == 1 {
		return "initialMigrationState-global.json"
	}
	return fmt.Sprintf("initialMigrationState-pair%d.json", pairIndex)
}

// processDatabasePair processes a single database pair
func (m *Migrator) processDatabasePair(ctx context.Context, pair config.DatabasePair, pairIndex int, mode string) error {
	liveOnly := mode == "live-only"

	// Initialize shared stats tracking for this database pair
	statsInterval := time.Duration(m.config.StatsIntervalMinutes) * time.Minute
	statsManager := NewStatsManager(m.log, statsInterval, m.config.GroupOpsByDistinctId)
	statsManager.DontApply = m.DontApply
	statsManager.DryRun = m.DryRun

	// Check if this is legacy mode - if so, handle it separately
	if (mode == "live" || mode == "live-only") && pair.Source.ReplicationMethod == "oplog-legacy" {
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
		return m.startOplogReplicationLegacy(ctx, pair.Source.Database, pair.Target.Database, collections, pair, pairIndex, liveOnly)
	}

	// Connect to source MongoDB (modern driver)
	m.log.Infof("Connecting to source MongoDB at %s (MinPoolSize: 128, MaxPoolSize: 256)", pair.Source.ConnectionString)
	sourceDB, err := db.NewMongoDB(pair.Source.ConnectionString, pair.Source.Database, 128, 256, 0, statsManager.GetSourcePoolMonitor(), m.log) // Source uses static pool size (min 128, max 256)
	if err != nil {
		return fmt.Errorf("failed to connect to source MongoDB: %w", err)
	}

	// Get maximum connection idle timeout for target
	maxConnIdleTimeTarget := time.Duration(m.config.TargetMaxConnIdleSeconds) * time.Second

	// Connect to target MongoDB
	m.log.Infof("Connecting to target MongoDB at %s (MinPoolSize: %d, MaxPoolSize: %d, MaxIdleTime: %v)", pair.Target.ConnectionString, m.config.TargetMinPoolSize, m.config.TargetMaxPoolSize, maxConnIdleTimeTarget)
	targetDB, err := db.NewMongoDB(pair.Target.ConnectionString, pair.Target.Database, uint64(m.config.TargetMinPoolSize), uint64(m.config.TargetMaxPoolSize), maxConnIdleTimeTarget, statsManager.GetTargetPoolMonitor(), m.log)
	if err != nil {
		return fmt.Errorf("failed to connect to target MongoDB: %w", err)
	}

	// Determine collections to process
	collections, err := m.getCollectionsToProcess(ctx, sourceDB, pair.Target.Collections)
	if err != nil {
		return fmt.Errorf("failed to determine collections to process: %w", err)
	}

	// Apply database target-level default UpsertMode if active
	if pair.Target.UpsertMode {
		for i := range collections {
			collections[i].UpsertMode = true
		}
	}

	// Sync indexes before data migration (if configured)
	// For live mode, each replicator handles index sync during its own initial migration
	if mode == "migrate" && (pair.Target.SyncAllIndexes || len(pair.Target.Indexes) > 0) {
		if err := m.syncIndexes(ctx, sourceDB, targetDB, pair, collections); err != nil {
			m.log.Warnf("Index sync encountered issues: %v (continuing with migration)", err)
			// Continue with migration even if index sync has issues
		}

		// Index-Only mode: wait for all async index builds then return without migrating data
		if pair.Target.IndexOnly {
			m.log.Info("IndexOnly mode enabled. Waiting for all async index creation to complete...")
			targetDB.WaitForIndexCreation()
			m.log.Info("IndexOnly mode: all indexes synced successfully. Skipping data migration.")
			return nil
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

				opts := MigrateOptions{
					DLQ:          nil, // fail-fast
					StatsManager: nil,
					UpsertMode:   collConfig.UpsertMode,
				}
				if _, _, err := m.migrateCollection(ctx, sourceDB, targetDB, collConfig, opts); err != nil {
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
	} else if mode == "live" || mode == "live-only" {
		// Use client-level change stream for live replication
		if err := m.startClientLevelReplication(ctx, sourceDB, targetDB, pair.Source.Database, pair.Target.Database, collections, pair, pairIndex, liveOnly, statsManager); err != nil {
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
func (m *Migrator) startClientLevelReplication(ctx context.Context, sourceDB, targetDB *db.MongoDB, sourceDBName, targetDBName string, collections []config.CollectionConfig, pair config.DatabasePair, pairIndex int, liveOnly bool, statsManager *StatsManager) error {
	// Determine replication method
	replicationMethod := pair.Source.ReplicationMethod
	if replicationMethod == "" {
		replicationMethod = "changestream" // Default to change stream
	}

	m.log.Infof("Starting replication using method: %s", replicationMethod)

	if replicationMethod == "oplog" {
		// Use oplog-based replication
		return m.startOplogReplication(ctx, sourceDB, targetDB, sourceDBName, targetDBName, collections, pair, pairIndex, liveOnly)
	} else {
		// Use change stream-based replication (default)
		return m.startChangeStreamReplication(ctx, sourceDB, targetDB, sourceDBName, targetDBName, collections, pair, pairIndex, liveOnly, statsManager)
	}
}

// startChangeStreamReplication starts replication using change streams
func (m *Migrator) startChangeStreamReplication(ctx context.Context, sourceDB, targetDB *db.MongoDB, sourceDBName, targetDBName string, collections []config.CollectionConfig, pair config.DatabasePair, pairIndex int, liveOnly bool, statsManager *StatsManager) error {
	m.log.Info("Starting change stream-based replication for all collections")

	// Create client-level replicator
	replicator := NewClientLevelReplicator(sourceDB, targetDB, m.config, m.log)
	replicator.SetStatsManager(statsManager)
	replicator.DontApply = m.DontApply
	replicator.DryRun = m.DryRun

	// Add all collections to the replicator
	for _, collConfig := range collections {
		// Add collection to replicator
		replicator.AddCollection(sourceDBName, targetDBName, collConfig)
	}

	// Load global resume token if it exists (per-pair path)
	globalResumeTokenPath := m.getCheckpointPath("resumeToken", pairIndex)
	m.log.Infof("Using checkpoint file: %s", globalResumeTokenPath)
	globalResumeToken, err := LoadResumeToken(globalResumeTokenPath)
	if err != nil {
		m.log.Warnf("Error loading global resume token: %v. Will start from the beginning.", err)
		globalResumeToken = nil
	}

	// Load initial migration state
	initialMigrationStatePath := m.getInitialMigrationStatePath(pairIndex)
	m.log.Infof("Using initial migration state file: %s", initialMigrationStatePath)
	initialMigrationState, err := LoadInitialMigrationState(initialMigrationStatePath)
	if err != nil {
		return fmt.Errorf("failed to load initial migration state: %w", err)
	}

	// Create DLQ writer for this database pair
	dlqPath := m.getDLQPath(pairIndex)
	dlq, err := NewDLQWriter(dlqPath, m.log)
	if err != nil {
		m.log.Warnf("Failed to create DLQ writer at %s: %v (continuing without DLQ)", dlqPath, err)
		dlq = nil
	}
	var dlqInterface DLQ = &NopDLQWriter{}
	if dlq != nil {
		dlqInterface = dlq
		defer dlq.Close()
	}
	replicator.SetDLQ(dlqInterface)

	// Start client-level replication (which will handle index sync during initial migration)
	return replicator.StartReplication(ctx, globalResumeToken, globalResumeTokenPath, initialMigrationState, initialMigrationStatePath, pair, liveOnly, m.CdcStartTime, m)
}

// startOplogReplication starts replication using oplog tailing
func (m *Migrator) startOplogReplication(ctx context.Context, sourceDB, targetDB *db.MongoDB, sourceDBName, targetDBName string, collections []config.CollectionConfig, pair config.DatabasePair, pairIndex int, liveOnly bool) error {
	m.log.Info("Starting oplog-based replication for all collections")

	// Use modern oplog replicator
	replicator := NewOplogReplicator(sourceDB, targetDB, m.config, m.log)
	replicator.DontApply = m.DontApply
	replicator.DryRun = m.DryRun

	// Add all collections to the replicator
	for _, collConfig := range collections {
		replicator.AddCollection(sourceDBName, targetDBName, collConfig)
	}

	// Load oplog timestamp if it exists (per-pair path)
	oplogTimestampPath := m.getCheckpointPath("oplogTimestamp", pairIndex)
	m.log.Infof("Using checkpoint file: %s", oplogTimestampPath)
	oplogTimestamp, err := LoadOplogTimestamp(oplogTimestampPath)
	if err != nil {
		m.log.Warnf("Error loading oplog timestamp: %v. Will start from the beginning.", err)
		oplogTimestamp = nil
	}

	// Load initial migration state
	initialMigrationStatePath := m.getInitialMigrationStatePath(pairIndex)
	m.log.Infof("Using initial migration state file: %s", initialMigrationStatePath)
	initialMigrationState, err := LoadInitialMigrationState(initialMigrationStatePath)
	if err != nil {
		return fmt.Errorf("failed to load initial migration state: %w", err)
	}

	// Convert to interface{} for compatibility with StartReplication signature
	var globalTimestamp interface{}
	if oplogTimestamp != nil {
		globalTimestamp = oplogTimestamp
	}

	// Create DLQ writer for this database pair
	dlqPath := m.getDLQPath(pairIndex)
	dlq, err := NewDLQWriter(dlqPath, m.log)
	if err != nil {
		m.log.Warnf("Failed to create DLQ writer at %s: %v (continuing without DLQ)", dlqPath, err)
		dlq = nil
	}
	var dlqInterface DLQ = &NopDLQWriter{}
	if dlq != nil {
		dlqInterface = dlq
		defer dlq.Close()
	}
	replicator.SetDLQ(dlqInterface)

	// Start oplog replication (which will handle index sync during initial migration)
	return replicator.StartReplication(ctx, globalTimestamp, oplogTimestampPath, initialMigrationState, initialMigrationStatePath, pair, liveOnly, m.CdcStartTime, m)
}

// startOplogReplicationLegacy starts replication using legacy GTM + mgo for old MongoDB versions
func (m *Migrator) startOplogReplicationLegacy(ctx context.Context, sourceDBName, targetDBName string, collections []config.CollectionConfig, pair config.DatabasePair, pairIndex int, liveOnly bool) error {
	m.log.Info("Starting legacy oplog-based replication (using mgo driver for MongoDB 3.0/3.2)")

	// Connect to source MongoDB using legacy driver (mgo)
	m.log.Infof("Connecting to source MongoDB (legacy) at %s", pair.Source.ConnectionString)
	sourceDBLegacy, err := db.NewMongoDBLegacy(pair.Source.ConnectionString, pair.Source.Database)
	if err != nil {
		return fmt.Errorf("failed to connect to source MongoDB (legacy): %w", err)
	}
	defer sourceDBLegacy.Close()

	// Get maximum connection idle timeout
	maxConnIdleTime := time.Duration(m.config.TargetMaxConnIdleSeconds) * time.Second

	// Connect to target MongoDB using modern driver
	m.log.Infof("Connecting to target MongoDB (modern) at %s (MinPoolSize: %d, MaxPoolSize: %d, MaxIdleTime: %v)", pair.Target.ConnectionString, m.config.TargetMinPoolSize, m.config.TargetMaxPoolSize, maxConnIdleTime)
	targetDB, err := db.NewMongoDB(pair.Target.ConnectionString, pair.Target.Database, uint64(m.config.TargetMinPoolSize), uint64(m.config.TargetMaxPoolSize), maxConnIdleTime, nil, m.log)
	if err != nil {
		return fmt.Errorf("failed to connect to target MongoDB: %w", err)
	}
	defer targetDB.Close(ctx)

	// Create legacy oplog replicator
	replicator := NewOplogReplicatorLegacy(sourceDBLegacy, targetDB, m.config, m.log)
	replicator.DontApply = m.DontApply
	replicator.DryRun = m.DryRun

	// Add all collections to the replicator
	for _, collConfig := range collections {
		replicator.AddCollection(sourceDBName, targetDBName, collConfig)
	}

	// Load oplog timestamp if it exists (per-pair path)
	oplogTimestampPath := m.getCheckpointPath("oplogTimestamp", pairIndex)
	m.log.Infof("Using checkpoint file: %s", oplogTimestampPath)
	oplogTimestamp, err := LoadOplogTimestamp(oplogTimestampPath)
	if err != nil {
		m.log.Warnf("Error loading oplog timestamp: %v. Will start from the beginning.", err)
		oplogTimestamp = nil
	}

	// Load initial migration state
	initialMigrationStatePath := m.getInitialMigrationStatePath(pairIndex)
	m.log.Infof("Using initial migration state file: %s", initialMigrationStatePath)
	initialMigrationState, err := LoadInitialMigrationState(initialMigrationStatePath)
	if err != nil {
		return fmt.Errorf("failed to load initial migration state: %w", err)
	}

	// Convert to interface{} for compatibility with StartReplication signature
	var globalTimestamp interface{}
	if oplogTimestamp != nil {
		globalTimestamp = oplogTimestamp
	}

	// Create DLQ writer for this database pair
	dlqPath := m.getDLQPath(pairIndex)
	dlq, err := NewDLQWriter(dlqPath, m.log)
	if err != nil {
		m.log.Warnf("Failed to create DLQ writer at %s: %v (continuing without DLQ)", dlqPath, err)
		dlq = nil
	}
	var dlqInterface DLQ = &NopDLQWriter{}
	if dlq != nil {
		dlqInterface = dlq
		defer dlq.Close()
	}
	replicator.SetDLQ(dlqInterface)

	// Start legacy oplog replication
	return replicator.StartReplication(ctx, globalTimestamp, oplogTimestampPath, initialMigrationState, initialMigrationStatePath, pair, liveOnly, m.CdcStartTime, m)
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

// MigrateOptions defines parameters to tune the backfill behavior dynamically
type MigrateOptions struct {
	DLQ          DLQ           // If provided, failures are routed here and migration continues (resilient mode)
	StatsManager *StatsManager // If provided, statistics are updated thread-safely (live mode stats)
	UpsertMode   bool          // Use upsert instead of insert (from CollectionConfig)
}

// migrateCollection performs a one-time migration of a collection with parallel batch processing
func (m *Migrator) migrateCollection(ctx context.Context, sourceDB, targetDB *db.MongoDB, collConfig config.CollectionConfig, opts MigrateOptions) (int64, int64, error) {
	// Get source and target collections
	sourceCollection := sourceDB.GetCollection(collConfig.SourceCollection)
	targetCollection := targetDB.GetCollection(collConfig.TargetCollection)

	m.log.Infof("Migrating collection: %s.%s to %s.%s", sourceDB.GetDatabaseName(), collConfig.SourceCollection, targetDB.GetDatabaseName(), collConfig.TargetCollection)

	// Get total count for progress reporting
	totalCount, err := sourceCollection.EstimatedDocumentCount(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to count documents: %w", err)
	}

	m.log.Infof("Found %d documents to migrate", totalCount)

	// If no documents, we're done
	if totalCount == 0 {
		m.log.Infof("No documents to migrate for collection %s", collConfig.SourceCollection)
		return 0, 0, nil
	}

	// Check if parallel reads are enabled and collection is large enough
	if m.config.ParallelReadsEnabled && totalCount >= int64(m.config.MinDocsForParallelReads) {
		m.log.Infof("Using parallel reads for large collection: %s (%d documents)", collConfig.SourceCollection, totalCount)
		return m.migrateCollectionParallel(ctx, sourceDB, targetDB, collConfig, totalCount, opts)
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
		return 0, 0, fmt.Errorf("failed to create cursor: %w", err)
	}
	defer cursor.Close(ctx)

	// Set up parallel batch processing
	var wg sync.WaitGroup
	channelBufferSize := m.config.InitialChannelBufferSize
	batchChan := make(chan []interface{}, channelBufferSize) // Buffer for batches
	errorChan := make(chan error, 1)                         // Channel for errors
	doneChan := make(chan struct{})                          // Channel to signal completion

	// Track progress
	var successCount int64
	var failedCount int64
	var migratedCount int64
	var lastLoggedPercentage int = -1 // Start at -1 to ensure 0% is logged
	var mu sync.Mutex                 // Mutex for thread-safe updates to successCount, failedCount, migratedCount, and lastLoggedPercentage

	// Start worker pool for parallel batch processing
	workerCount := m.config.InitialMigrationWorkers
	m.log.Infof("Starting %d workers for parallel document batch processing", workerCount)

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for batch := range batchChan {
				succeeded, failed, err := m.writeBatch(ctx, targetCollection, batch, sourceDB.GetDatabaseName(), collConfig.SourceCollection, opts, retryManager)
				if err != nil {
					select {
					case errorChan <- fmt.Errorf("worker %d failed to process batch: %w", workerID, err):
					default:
					}
					return
				}

				// Update progress
				mu.Lock()
				successCount += succeeded
				failedCount += failed
				migratedCount += int64(len(batch))
				currentCount := migratedCount
				currentSuccess := successCount
				currentFailed := failedCount

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
					if failedCount > 0 {
						m.log.Infof("Collection %s progress: %d/%d documents (%.0f%%) - Successful: %d, Failed: %d",
							collConfig.SourceCollection, currentCount, totalCount, float64(currentPercentage)*10, currentSuccess, currentFailed)
					} else {
						m.log.Infof("Collection %s progress: %d/%d documents (%.0f%%)",
							collConfig.SourceCollection, currentCount, totalCount, float64(currentPercentage)*10)
					}
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
			return successCount, failedCount, err
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
			return successCount, failedCount, fmt.Errorf("failed to decode document: %w", err)
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
				return successCount, failedCount, err
			case <-ctx.Done():
				// Context cancelled
				cursor.Close(ctx)
				close(batchChan)
				m.log.Info("Batch processing interrupted due to context cancellation")
				return successCount, failedCount, context.Canceled // Return context.Canceled for consistent error handling
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
		return successCount, failedCount, fmt.Errorf("cursor error: %w", err)
	}

	// Process any remaining documents
	if len(batch) > 0 {
		select {
		case batchChan <- batch:
			// Final batch sent to worker
		case err := <-errorChan:
			// Error from a worker
			close(batchChan)
			return successCount, failedCount, err
		case <-ctx.Done():
			// Context cancelled
			close(batchChan)
			m.log.Info("Final batch processing interrupted due to context cancellation")
			return successCount, failedCount, context.Canceled // Return context.Canceled for consistent error handling
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
		return successCount, failedCount, err
	case <-ctx.Done():
		// Context cancelled
		m.log.Info("Migration interrupted due to context cancellation")
		return successCount, failedCount, context.Canceled // Return context.Canceled for consistent error handling
	}

	if failedCount > 0 {
		m.log.Warnf("Migration for %s completed with %d failures! Successful: %d, Failed: %d, Total: %d",
			collConfig.SourceCollection, failedCount, successCount, failedCount, migratedCount)
	} else {
		m.log.Infof("Migration for %s completed successfully! Total documents: %d",
			collConfig.SourceCollection, migratedCount)
	}
	return successCount, failedCount, nil
}

// processBatch processes a batch of documents
func processBatch(ctx context.Context, collection *mongo.Collection, batch []interface{}, useUpsert bool, log *logger.Logger, dbName, collName string) error {
	if len(batch) == 0 {
		return nil
	}

	// Transform __*__ field names to _*_ for Firestore compatibility
	transformedBatch, transErr := TransformBatch(batch, log, dbName, collName)
	if transErr != nil {
		return fmt.Errorf("failed to transform field names: %w", transErr)
	}
	batch = transformedBatch

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
func (m *Migrator) migrateCollectionParallel(ctx context.Context, sourceDB, targetDB *db.MongoDB, collConfig config.CollectionConfig, totalCount int64, opts MigrateOptions) (int64, int64, error) {
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
		return 0, 0, fmt.Errorf("failed to create partitions: %w", err)
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
	var successCount int64
	var failedCount int64
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
					if failedCount > 0 {
						m.log.Infof("Collection %s progress: %d/%d documents (%.0f%%) - Successful: %d, Failed: %d",
							collConfig.SourceCollection, currentCount, totalCount, float64(currentPercentage)*10, successCount, failedCount)
					} else {
						m.log.Infof("Collection %s progress: %d/%d documents (%.0f%%)",
							collConfig.SourceCollection, currentCount, totalCount, float64(currentPercentage)*10)
					}
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
						succeeded, failed, err := m.writeBatch(ctx, targetCollection, batch, sourceDB.GetDatabaseName(), collConfig.SourceCollection, opts, retryManager)
						if err != nil {
							select {
							case partitionErrorChan <- fmt.Errorf("worker %d in partition %d failed: %w", workerID, partitionIndex, err):
							default:
							}
							return
						}

						// Update progress
						partitionMu.Lock()
						partitionMigratedCount += int64(len(batch))
						partitionMu.Unlock()

						// Update overall progress counter
						mu.Lock()
						successCount += succeeded
						failedCount += failed
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
			return successCount, failedCount, err
		}
	}

	if failedCount > 0 {
		m.log.Warnf("Parallel migration for %s completed with %d failures! Successful: %d, Failed: %d, Total: %d",
			collConfig.SourceCollection, failedCount, successCount, failedCount, migratedCount)
	} else {
		m.log.Infof("Parallel migration for %s completed successfully! Total documents: %d",
			collConfig.SourceCollection, migratedCount)
	}
	return successCount, failedCount, nil
}

// Helper functions for min/max
// func min(a, b int) int {
// 	if a < b {
// 		return a
// 	}
// 	return b
// }

// syncIndexes synchronizes indexes from source to target collections.
// When pair.Target.SyncAllIndexes is true, it syncs ALL indexes (except _id_) for every
// collection in the collections list. Otherwise it uses the explicit pair.Target.Indexes config.
func (m *Migrator) syncIndexes(ctx context.Context, sourceDB, targetDB *db.MongoDB, pair config.DatabasePair, collections []config.CollectionConfig) error {
	m.log.Info("Starting async index synchronization (fire-and-forget)...")

	var indexCount int

	if pair.Target.SyncAllIndexes {
		// Auto-sync all indexes for every migrated collection
		m.log.Info("SyncAllIndexes enabled: syncing all indexes (excluding _id_) for all collections")

		for _, collConfig := range collections {
			// Get all indexes from source collection
			sourceIndexes, err := sourceDB.ListIndexes(ctx, collConfig.SourceCollection)
			if err != nil {
				m.log.Warnf("Failed to list indexes for %s: %v (continuing anyway)", collConfig.SourceCollection, err)
				continue
			}

			targetCollName := collConfig.TargetCollection

			// List existing indexes on the target to skip already-created ones
			existingIndexNames := make(map[string]bool)
			targetIndexes, err := targetDB.ListIndexes(ctx, targetCollName)
			if err != nil {
				m.log.Debugf("Could not list target indexes for %s: %v (will attempt all)", targetCollName, err)
			} else {
				for _, idx := range targetIndexes {
					if name, ok := idx["name"].(string); ok {
						existingIndexNames[name] = true
					}
				}
			}

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

				// Skip if index already exists on target
				if existingIndexNames[indexName] {
					m.log.Infof("Index '%s' already exists on target collection '%s', skipping", indexName, targetCollName)
					continue
				}

				// Fire-and-forget: launch async index creation with a dedicated client
				m.log.Infof("Launching async index creation: '%s' on target collection '%s'", indexName, targetCollName)
				targetDB.CreateIndexFromDefinitionAsync(pair.Target.ConnectionString, targetCollName, indexDef)
				indexCount++
			}
		}
	}

	// Also process explicit index configs if provided
	for _, indexConfig := range pair.Target.Indexes {
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

			// Fire-and-forget: launch async index creation with a dedicated client
			m.log.Infof("Launching async index creation: '%s' on target collection '%s'", indexName, targetCollName)
			targetDB.CreateIndexFromDefinitionAsync(pair.Target.ConnectionString, targetCollName, indexDef)
			indexCount++
		}
	}

	m.log.Infof("Launched %d async index creation tasks. Proceeding with data migration.", indexCount)
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

// writeBatch processes and writes a batch of documents.
// - If opts.DLQ is provided, it uses resilient writes (upsert fallback + DLQ routing) and returns counts.
// - If opts.DLQ is nil, it uses fail-fast writes (standard InsertMany + RetryManager) and aborts on errors.
func (m *Migrator) writeBatch(ctx context.Context, targetCol *mongo.Collection, batch []interface{}, sourceDB, sourceCollection string, opts MigrateOptions, retryManager *RetryManager) (int64, int64, error) {
	if opts.DLQ != nil {
		// Resilient Mode: Use DLQ Fallback (exactly identical to client_stream.go)
		transformedBatch, err := TransformBatch(batch, m.log, sourceDB, sourceCollection)
		if err != nil {
			m.log.Errorf("Field name transformation failed for batch in %s.%s: %v", sourceDB, sourceCollection, err)
			for _, doc := range batch {
				docID := extractDocID(doc)
				opts.DLQ.WriteFailed(sourceDB, sourceCollection, docID, err, "initial", "insert", doc)
			}
			return 0, int64(len(batch)), nil
		}
		batch = transformedBatch

		var batchFailed int64

		if _, err := targetCol.InsertMany(ctx, batch, options.InsertMany().SetOrdered(false)); err != nil {
			bulkWriteException, ok := err.(mongo.BulkWriteException)
			if ok {
				m.log.Debugf("Bulk insert partially failed for %s.%s: %d failed",
					sourceDB, sourceCollection, len(bulkWriteException.WriteErrors))

				for _, writeErr := range bulkWriteException.WriteErrors {
					var errDocID interface{}
					if writeErr.Index < len(batch) {
						errDocID = extractDocID(batch[writeErr.Index])
					}

					m.log.Debugf("[%s.%s] Insert error at index %d, _id=%v: %v", sourceDB, sourceCollection, writeErr.Index, errDocID, writeErr.Message)

					if writeErr.Code == 11000 && writeErr.Index < len(batch) {
						m.log.Debugf("[%s.%s] Skipping duplicate document _id=%v at index %d", sourceDB, sourceCollection, errDocID, writeErr.Index)
					} else if writeErr.Index < len(batch) {
						doc := batch[writeErr.Index]
						id := extractDocID(doc)

						if id != nil {
							filter := bson.M{"_id": id}
							if _, err := targetCol.ReplaceOne(ctx, filter, doc, options.Replace().SetUpsert(true)); err != nil {
								m.log.Errorf("[%s.%s] Retry upsert failed for document _id=%v: %v", sourceDB, sourceCollection, id, err)
								batchFailed++
								opts.DLQ.WriteFailed(sourceDB, sourceCollection, id, err, "initial", "insert", doc)
							}
						} else {
							batchFailed++
						}
					}
				}
			} else {
				// Handle non-bulk write errors (e.g. network partition or context timeout)
				bulkRetrySucceeded := false
				if retryManager != nil && err != context.Canceled {
					errType := retryManager.ClassifyError(err)
					if errType == ErrorTypeConnection || errType == ErrorTypeContention {
						m.log.Infof("Transient error detected for %s.%s. Retrying bulk insert with backoff...", sourceDB, sourceCollection)
						retryErr := retryManager.RetryWithBackoff(ctx, func() error {
							_, retryInsertErr := targetCol.InsertMany(ctx, batch, options.InsertMany().SetOrdered(false))
							return retryInsertErr
						})
						if retryErr == nil {
							m.log.Infof("Bulk insert for %s.%s succeeded after retry", sourceDB, sourceCollection)
							bulkRetrySucceeded = true
						} else {
							m.log.Warnf("Bulk insert for %s.%s still failed after retries: %v. Falling back to individual operations.", sourceDB, sourceCollection, retryErr)
						}
					}
				}

				if !bulkRetrySucceeded {
					// Fall back to individual operations with upsert for all documents
					for _, doc := range batch {
						if _, err := targetCol.InsertOne(ctx, doc); err != nil {
							docID := extractDocID(doc)
							if docID != nil {
								filter := bson.M{"_id": docID}
								if _, err := targetCol.ReplaceOne(ctx, filter, doc, options.Replace().SetUpsert(true)); err != nil {
									if err == context.Canceled {
										m.log.Debugf("Upserting document %v in %s.%s canceled due to context cancellation", docID, sourceDB, sourceCollection)
									} else {
										m.log.Errorf("Error upserting document %v in %s.%s: %v", docID, sourceDB, sourceCollection, err)
										batchFailed++
										if opts.DLQ != nil {
											opts.DLQ.WriteFailed(sourceDB, sourceCollection, docID, err, "initial", "insert", doc)
										}
									}
								}
							} else {
								batchFailed++
							}
						}
					}
				}
			}
		}

		successCount := int64(len(batch)) - batchFailed
		return successCount, batchFailed, nil
	} else {
		// Fail-Fast Mode (standard mode=migrate behavior)
		err := retryManager.RetryWithSplit(ctx, batch, sourceCollection, func(b []interface{}) error {
			return processBatch(ctx, targetCol, b, opts.UpsertMode, m.log, sourceDB, sourceCollection)
		})
		if err != nil {
			return 0, int64(len(batch)), err
		}
		return int64(len(batch)), 0, nil
	}
}
