package migration

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
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
	config        *config.Config
	log           *logger.Logger
	LiveStartTime *primitive.Timestamp
	DryRun        bool
	isLive        bool
	CheckpointDir string
}

// getCheckpointDir returns the configured checkpoint directory, defaulting to current directory if unset.
func (m *Migrator) getCheckpointDir() string {
	if m.CheckpointDir != "" {
		return m.CheckpointDir
	}
	return "."
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
	m.isLive = (mode == "live" || mode == "live-only")

	// Validate mode
	if mode != "migrate" && mode != "live" && mode != "live-only" && mode != "retry-dlq" {
		return fmt.Errorf("invalid mode: %s, must be 'migrate', 'live', 'live-only', or 'retry-dlq'", mode)
	}

	m.log.Infof("Starting MongoDB to MongoDB %s process", mode)

	if m.DryRun {
		m.log.Info("[Dry Run] Running target compatibility check reports before starting migration")
		if err := RunCompatibilityTest(ctx, m.config, m.DryRun, m.log); err != nil {
			m.log.Warnf("Target compatibility check connection check failed: %v", err)
		}
	}

	if mode == "migrate" || mode == "retry-dlq" {
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
		if mode == "retry-dlq" {
			m.log.Info("DLQ reprocessing completed successfully")
		} else {
			m.log.Info("Migration completed successfully")
		}
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
	if mode == "retry-dlq" {
		return m.reprocessDLQ(ctx, pair, pairIndex)
	}

	liveOnly := mode == "live-only"

	// Initialize shared stats tracking for this database pair
	statsInterval := time.Duration(m.config.StatsIntervalMinutes) * time.Minute
	incrementalStatsManager := NewIncrementalStatsManager(m.log, statsInterval, m.config.GroupOpsByDistinctId)
	incrementalStatsManager.DryRun = m.DryRun

	backfillStatsManager := NewBackfillStatsManager(m.log, statsInterval, incrementalStatsManager)
	backfillStatsManager.DryRun = m.DryRun

	// Start backfill stats manager
	backfillStatsCtx, cancelBackfillStats := context.WithCancel(ctx)
	defer cancelBackfillStats()
	backfillStatsManager.Start(backfillStatsCtx)

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
	sourceDB, err := db.NewMongoDB(pair.Source.ConnectionString, pair.Source.Database, 128, 256, 0, incrementalStatsManager.GetSourcePoolMonitor(), m.log) // Source uses static pool size (min 128, max 256)
	if err != nil {
		return fmt.Errorf("failed to connect to source MongoDB: %w", err)
	}

	// Get maximum connection idle timeout for target
	maxConnIdleTimeTarget := time.Duration(m.config.TargetMaxConnIdleSeconds) * time.Second

	// Connect to target MongoDB
	m.log.Infof("Connecting to target MongoDB at %s (MinPoolSize: %d, MaxPoolSize: %d, MaxIdleTime: %v)", pair.Target.ConnectionString, m.config.TargetMinPoolSize, m.config.TargetMaxPoolSize, maxConnIdleTimeTarget)
	targetDB, err := db.NewMongoDB(pair.Target.ConnectionString, pair.Target.Database, uint64(m.config.TargetMinPoolSize), uint64(m.config.TargetMaxPoolSize), maxConnIdleTimeTarget, incrementalStatsManager.GetTargetPoolMonitor(), m.log)
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
		// Configure index build concurrency before launching any async builds
		if m.config.IndexConcurrency > 0 {
			targetDB.SetIndexConcurrency(m.config.IndexConcurrency)
		}

		if err := m.syncIndexes(ctx, sourceDB, targetDB, pair, collections); err != nil {
			m.log.Warnf("Index sync encountered issues: %v (continuing with migration)", err)
			// Continue with migration even if index sync has issues
		}

		// Index-Only mode: wait for all async index builds then return without migrating data
		if pair.Target.IndexOnly {
			m.log.Info("IndexOnly mode enabled. Waiting for all async index creation to complete...")
			targetDB.WaitForIndexCreation()
			m.logFailedIndexes(targetDB)
			m.log.Info("IndexOnly mode: all indexes synced. Skipping data migration.")
			return nil
		}

		// Wait for all async index creation to complete before starting data migration
		// This prevents "schema change" errors from Firestore when indexes are being built
		// concurrently with data writes
		m.log.Info("Waiting for all async index creation to complete before starting data migration...")
		targetDB.WaitForIndexCreation()
		m.logFailedIndexes(targetDB)
		m.log.Info("All indexes created. Proceeding with data migration.")
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

		throttlerCtx, throttlerCancel := context.WithCancel(ctx)
		defer throttlerCancel()

		// Initialize the throttler for backfill traffic writes with burst size based on batch size
		burstSize := 2 * m.config.InitialWriteBatchSize
		throttler := NewWriteThrottler(m.config.BackfillRampUp, burstSize)
		if throttler != nil {
			throttler.StartRampUp(throttlerCtx)
			backfillStatsManager.SetThrottler(throttler)
		}

		for _, collConfig := range collections {
			wg.Add(1)
			// Acquire semaphore
			semaphore <- struct{}{}

			// Start migration in a goroutine
			go func(collConfig config.CollectionConfig) {
				defer wg.Done()
				defer func() { <-semaphore }() // Release semaphore when done

				opts := MigrateOptions{
					DLQ:                  nil, // fail-fast
					StatsManager:         nil,
					BackfillStatsManager: backfillStatsManager,
					UpsertMode:           collConfig.UpsertMode,
					Throttler:            throttler,
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
		backfillStatsManager.ReportStats(true)
		backfillStatsManager.Stop()
	} else if mode == "live" || mode == "live-only" {
		// Use client-level change stream for live replication
		if err := m.startClientLevelReplication(ctx, sourceDB, targetDB, pair.Source.Database, pair.Target.Database, collections, pair, pairIndex, liveOnly, incrementalStatsManager, backfillStatsManager); err != nil {
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
func (m *Migrator) startClientLevelReplication(ctx context.Context, sourceDB, targetDB *db.MongoDB, sourceDBName, targetDBName string, collections []config.CollectionConfig, pair config.DatabasePair, pairIndex int, liveOnly bool, incrementalStatsManager *IncrementalStatsManager, backfillStatsManager *BackfillStatsManager) error {
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
		return m.startChangeStreamReplication(ctx, sourceDB, targetDB, sourceDBName, targetDBName, collections, pair, pairIndex, liveOnly, incrementalStatsManager, backfillStatsManager)
	}
}

// startChangeStreamReplication starts replication using change streams
func (m *Migrator) startChangeStreamReplication(ctx context.Context, sourceDB, targetDB *db.MongoDB, sourceDBName, targetDBName string, collections []config.CollectionConfig, pair config.DatabasePair, pairIndex int, liveOnly bool, incrementalStatsManager *IncrementalStatsManager, backfillStatsManager *BackfillStatsManager) error {
	m.log.Info("Starting change stream-based replication for all collections")

	// Create client-level replicator
	replicator := NewClientLevelReplicator(sourceDB, targetDB, m.config, m.log)
	replicator.SetIncrementalStatsManager(incrementalStatsManager)
	replicator.SetBackfillStatsManager(backfillStatsManager)
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

	// Strict DLQ safety checks based on initial migration state
	dlqPath := m.getDLQPath(pairIndex)
	if initialMigrationState == nil || !initialMigrationState.IsCompleted() {
		if err := BackupAndClearDLQ(dlqPath, m.log); err != nil {
			return fmt.Errorf("failed to backup and clear DLQ: %w", err)
		}
	} else {
		if initialMigrationState.Status == StatusCompletedWithFailures {
			return fmt.Errorf("cannot start incremental replication: initial migration completed with failures in a previous run. Please reprocess the failures using DLQ retry first")
		}
	}

	// Create DLQ writer for this database pair
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
	if incrementalStatsManager != nil {
		incrementalStatsManager.SetDLQ(dlqInterface)
	}
	if backfillStatsManager != nil {
		backfillStatsManager.SetDLQ(dlqInterface)
	}

	// Start client-level replication (which will handle index sync during initial migration)
	return replicator.StartReplication(ctx, globalResumeToken, globalResumeTokenPath, initialMigrationState, initialMigrationStatePath, pair, liveOnly, m.LiveStartTime, m)
}

// startOplogReplication starts replication using oplog tailing
func (m *Migrator) startOplogReplication(ctx context.Context, sourceDB, targetDB *db.MongoDB, sourceDBName, targetDBName string, collections []config.CollectionConfig, pair config.DatabasePair, pairIndex int, liveOnly bool) error {
	m.log.Info("Starting oplog-based replication for all collections")

	// Use modern oplog replicator
	replicator := NewOplogReplicator(sourceDB, targetDB, m.config, m.log)
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

	// Strict DLQ safety checks based on initial migration state
	dlqPath := m.getDLQPath(pairIndex)
	if initialMigrationState == nil || !initialMigrationState.IsCompleted() {
		if err := BackupAndClearDLQ(dlqPath, m.log); err != nil {
			return fmt.Errorf("failed to backup and clear DLQ: %w", err)
		}
	} else {
		if initialMigrationState.Status == StatusCompletedWithFailures {
			return fmt.Errorf("cannot start incremental replication: initial migration completed with failures in a previous run. Please reprocess the failures using DLQ retry first")
		}
	}

	// Create DLQ writer for this database pair
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
	return replicator.StartReplication(ctx, globalTimestamp, oplogTimestampPath, initialMigrationState, initialMigrationStatePath, pair, liveOnly, m.LiveStartTime, m)
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

	// Strict DLQ safety checks based on initial migration state
	dlqPath := m.getDLQPath(pairIndex)
	if initialMigrationState == nil || !initialMigrationState.IsCompleted() {
		hasFailed, err := HasActiveFailedRecords(dlqPath)
		if err != nil {
			return fmt.Errorf("failed to check DLQ status: %w", err)
		}
		if hasFailed {
			return fmt.Errorf("DLQ safety violation: active failures found in dead letter queue %s. Please reprocess these failures (using DLQ retry) or clear the queue before running the initial migration.", dlqPath)
		}
	} else {
		if initialMigrationState.Status == StatusCompletedWithFailures {
			return fmt.Errorf("cannot start incremental replication: initial migration completed with failures in a previous run. Please reprocess the failures using DLQ retry first")
		}
	}

	// Create DLQ writer for this database pair
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
	return replicator.StartReplication(ctx, globalTimestamp, oplogTimestampPath, initialMigrationState, initialMigrationStatePath, pair, liveOnly, m.LiveStartTime, m)
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
	DLQ                  DLQ                      // If provided, failures are routed here and migration continues (resilient mode)
	StatsManager         *IncrementalStatsManager // If provided, statistics are updated thread-safely (live mode stats)
	BackfillStatsManager *BackfillStatsManager    // If provided, backfill statistics are recorded thread-safely
	UpsertMode           bool                     // Use upsert instead of insert (from CollectionConfig)
	Throttler            *WriteThrottler          // Write throttler for initial backfill QPS ramp-up
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

	if m.DryRun {
		m.log.Infof("[Dry Run] Skipping actual data migration for collection %s", collConfig.SourceCollection)
		// Perform sampling analysis to recommend ID partitioning strategy
		partitioner := NewCollectionPartitioner(
			sourceCollection,
			m.log,
			m.config.MaxReadPartitions,
			m.config.MinDocsPerPartition,
			m.config.SampleSize,
			m.config.IDTypeForPartition,
		)
		recType, err := partitioner.RecommendIDPartitioning(ctx)
		if err != nil {
			m.log.Warnf("[Dry Run] Failed to recommend ID partitioning for collection %s: %v", collConfig.SourceCollection, err)
		} else {
			m.log.Infof("[Dry Run] Recommended ID partitioning strategy for collection %s: %s", collConfig.SourceCollection, recType)
		}
		return 0, 0, nil
	}

	if opts.BackfillStatsManager != nil {
		opts.BackfillStatsManager.AddTargetCount(totalCount)
	}

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

	// In sequential backfill mode, the collection is treated as a single partition (0 of 1)
	const partitionIndex = 0
	const totalSplits = 1

	// Evaluate backfill resumption plan for sequential migration (partition count: 1)
	checkpointDir := m.getCheckpointDir()
	plan, err := DetermineBackfillResumptionPlan(checkpointDir, sourceDB.GetDatabaseName(), collConfig.SourceCollection, totalSplits)

	var resumeFilter bson.D
	var previouslyMigratedDocs int64

	// Initialize default fresh checkpoint state
	checkpoint := &PartitionCheckpoint{
		Database:                sourceDB.GetDatabaseName(),
		Collection:              collConfig.SourceCollection,
		PartitionIndex:          partitionIndex,
		TotalSplits:             totalSplits,
		ApproximateDocsMigrated: 0,
		TypeProgress:            make(map[BSONType]*TypeRangeBoundary),
		UpdatedAt:               time.Now().UTC(),
	}

	if err != nil {
		m.log.Warnf("[%s.%s] Error determining backfill resumption plan: %v. Starting fresh.", sourceDB.GetDatabaseName(), collConfig.SourceCollection, err)
		plan = &BackfillResumptionPlan{Mode: ResumptionModeFresh}
	}

	switch plan.Mode {
	case ResumptionModeDirect:
		checkpointPath := GetPartitionCheckpointPath(checkpointDir, sourceDB.GetDatabaseName(), collConfig.SourceCollection, partitionIndex, totalSplits)
		loadedCP, loadErr := LoadPartitionCheckpoint(checkpointPath)
		if loadErr != nil || loadedCP == nil {
			m.log.Warnf("[%s.%s] Failed to reload checkpoint from %s: %v. Falling back to fresh start.", sourceDB.GetDatabaseName(), collConfig.SourceCollection, checkpointPath, loadErr)
			plan.Mode = ResumptionModeFresh
		} else {
			checkpoint = loadedCP
			resumeFilter = plan.PartitionFilters[partitionIndex]
			previouslyMigratedDocs = plan.TotalDocsMigrated()
			m.log.Infof("[%s.%s] Resuming sequential initial backfill from checkpoint: ~%d docs previously migrated (filter: %+v)",
				sourceDB.GetDatabaseName(), collConfig.SourceCollection, previouslyMigratedDocs, resumeFilter)
		}

	case ResumptionModeResampleWithGlobalMin:
		previouslyMigratedDocs = plan.TotalDocsMigrated()
		checkpoint.ApproximateDocsMigrated = previouslyMigratedDocs
		for bType, minID := range plan.GlobalMinSafeIDs {
			checkpoint.TypeProgress[bType] = &TypeRangeBoundary{
				BSONType:    bType,
				SavedLastID: minID,
			}
		}
		filter, filterErr := BuildPartitionFilterFromCheckpoint(checkpoint)
		if filterErr != nil {
			m.log.Warnf("[%s.%s] Failed to build filter from global min checkpoint: %v. Falling back to fresh start.", sourceDB.GetDatabaseName(), collConfig.SourceCollection, filterErr)
			plan.Mode = ResumptionModeFresh
			checkpoint.ApproximateDocsMigrated = 0
			checkpoint.TypeProgress = make(map[BSONType]*TypeRangeBoundary)
			previouslyMigratedDocs = 0
		} else {
			resumeFilter = filter
			m.log.Infof("[%s.%s] Resuming sequential initial backfill with global min safe IDs across historical partitions: ~%d docs previously migrated (filter: %+v)",
				sourceDB.GetDatabaseName(), collConfig.SourceCollection, previouslyMigratedDocs, resumeFilter)
		}
	}

	if plan.Mode == ResumptionModeFresh {
		types, discErr := DiscoverPresentBSONTypes(ctx, sourceCollection)
		if discErr != nil {
			m.log.Warnf("[%s.%s] Failed to discover BSON types for initial backfill: %v", sourceDB.GetDatabaseName(), collConfig.SourceCollection, discErr)
		} else {
			for _, t := range types {
				checkpoint.TypeProgress[t] = &TypeRangeBoundary{BSONType: t}
			}
		}
		m.log.Infof("[%s.%s] Starting fresh sequential initial backfill (discovered types: %v)", sourceDB.GetDatabaseName(), collConfig.SourceCollection, types)
	}

	checkpointPath := GetPartitionCheckpointPath(checkpointDir, sourceDB.GetDatabaseName(), collConfig.SourceCollection, partitionIndex, totalSplits)
	checkpointInterval := time.Duration(m.config.CheckpointIntervalMinutes) * time.Minute
	saveThreshold := m.config.SaveThreshold

	tracker := NewBackfillPartitionTracker(m.log, checkpoint, checkpointPath, checkpointInterval, saveThreshold)
	tracker.Start(ctx)
	defer tracker.Close()

	// Create retry manager for batch processing.
	// [Safety Fix 8: Invalid ID Conversion] Firestore target APIs only support string, int64, or ObjectId document keys;
	// arrays or nested subdocuments trigger terminal errors. Enabling ConvertInvalidIds
	// transforms invalid types to strings to avoid write failures.
	retryManager := NewRetryManager(
		m.config.RetryConfig.MaxRetries,
		time.Duration(m.config.RetryConfig.BaseDelayMs)*time.Millisecond,
		time.Duration(m.config.RetryConfig.MaxDelayMs)*time.Millisecond,
		m.config.RetryConfig.EnableBatchSplitting,
		m.config.RetryConfig.MinBatchSize,
		m.config.RetryConfig.ConvertInvalidIds,
		m.log,
	)

	// [Safety Fix 1: MongoDB Cursor Timeout] SetNoCursorTimeout(true) is used to prevent the MongoDB read cursor from timing out (default 10 minutes)
	// when upstream readers are throttled or paused by write rate-limiting/backpressure downstream.
	findFilter := bson.D{}
	if resumeFilter != nil {
		findFilter = resumeFilter
	}
	// [Backfill Resumption Safety] Sort by _id ascending to guarantee monotonic traversal order.
	// The resumption filter uses $gte on the last checkpointed _id, so correct ordering is required
	// to ensure no documents are skipped or duplicated upon resume.
	cursor, err := sourceCollection.Find(ctx, findFilter, options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetBatchSize(int32(readBatchSize)).SetNoCursorTimeout(true))
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create cursor: %w", err)
	}
	// [Safety Fix 5: Memory Leak on Server] Ensure cursor is closed upon termination to prevent active server-side cursor leaks
	// and connection starvation on the source MongoDB replica set.
	defer cursor.Close(ctx)

	type backfillBatchItem struct {
		batch []interface{}
		seq   uint64
	}

	// Set up parallel batch processing
	var wg sync.WaitGroup
	channelBufferSize := m.config.InitialChannelBufferSize
	batchChan := make(chan backfillBatchItem, channelBufferSize) // Buffer for batches
	errorChan := make(chan error, 1)                             // Channel for errors
	doneChan := make(chan struct{})                              // Channel to signal completion

	// Track progress
	var successCount int64
	var failedCount int64
	var migratedCount int64
	var lastLoggedPercentage int = -1 // Start at -1 to ensure 0% is logged
	var mu sync.Mutex                 // Mutex for thread-safe updates to successCount, failedCount, migratedCount, and lastLoggedPercentage

	proactiveSkipEnabled := &atomic.Bool{}

	// Start worker pool for parallel batch processing
	workerCount := m.config.InitialMigrationWorkers
	m.log.Infof("Starting %d workers for parallel document batch processing", workerCount)

	for i := 0; i < workerCount; i++ {
		if i > 0 && m.config.BackfillRampUp.Enabled && m.config.BackfillRampUp.UseStaggeredWorkers && m.config.BackfillRampUp.WorkerDelayMs > 0 {
			m.log.Infof("Staggering worker startup: delaying worker %d startup by %dms...", i, m.config.BackfillRampUp.WorkerDelayMs)
			select {
			case <-ctx.Done():
				close(batchChan)
				wg.Wait()
				return 0, 0, ctx.Err()
			case <-time.After(time.Duration(m.config.BackfillRampUp.WorkerDelayMs) * time.Millisecond):
			}
		}
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for item := range batchChan {
				if opts.BackfillStatsManager != nil {
					opts.BackfillStatsManager.RecordWorkerReceived(int64(len(item.batch)))
				}
				succeeded, failed, err := m.writeBatch(ctx, targetCollection, item.batch, sourceDB.GetDatabaseName(), collConfig.SourceCollection, opts, retryManager, workerID, proactiveSkipEnabled)
				if err != nil {
					select {
					case errorChan <- fmt.Errorf("worker %d failed to process batch: %w", workerID, err):
					default:
					}
					return
				}

				if ctx.Err() == nil {
					tracker.AckBatch(item.seq, succeeded)
				}

				// Update progress
				mu.Lock()
				successCount += succeeded
				failedCount += failed
				migratedCount += int64(len(item.batch))
				currentSuccess := successCount
				currentFailed := failedCount

				cumulativeCount := previouslyMigratedDocs + successCount + failedCount
				if totalCount > 0 && cumulativeCount > totalCount {
					cumulativeCount = totalCount
				}
				currentPercentage := int(float64(cumulativeCount) / float64(totalCount) * 10)

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
						m.log.Infof("Collection %s progress: ~%d/%d documents (%.0f%%) [This run: %d successful, %d failed]",
							collConfig.SourceCollection, cumulativeCount, totalCount, float64(currentPercentage)*10, currentSuccess, currentFailed)
					} else {
						m.log.Infof("Collection %s progress: ~%d/%d documents (%.0f%%)",
							collConfig.SourceCollection, cumulativeCount, totalCount, float64(currentPercentage)*10)
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
		readStart := time.Now()
		hasNext := cursor.Next(ctx)
		if !hasNext {
			break
		}

		// Decode document
		var doc bson.D
		if err := cursor.Decode(&doc); err != nil {
			close(batchChan)
			return successCount, failedCount, fmt.Errorf("failed to decode document: %w", err)
		}
		readDuration := time.Since(readStart)

		if opts.BackfillStatsManager != nil {
			opts.BackfillStatsManager.RecordRead(readDuration, len(cursor.Current))
		}

		// Add to batch
		batch = append(batch, doc)
		batchCount++

		// Send batch if it reaches the write batch size
		if batchCount >= writeBatchSize {
			seq := tracker.RegisterBatch(batch)
			item := backfillBatchItem{batch: batch, seq: seq}
			sendStart := time.Now()
			select {
			case batchChan <- item:
				if opts.BackfillStatsManager != nil {
					opts.BackfillStatsManager.RecordIngestQueueStall(time.Since(sendStart))
				}
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
		seq := tracker.RegisterBatch(batch)
		item := backfillBatchItem{batch: batch, seq: seq}
		sendStart := time.Now()
		select {
		case batchChan <- item:
			if opts.BackfillStatsManager != nil {
				opts.BackfillStatsManager.RecordIngestQueueStall(time.Since(sendStart))
			}
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
		m.log.Warnf("Migration for %s completed with %d failures! Successful: %d, Failed: %d, Total: %d (check DLQ for failed documents)",
			collConfig.SourceCollection, failedCount, successCount, failedCount, migratedCount)
	} else {
		m.log.Infof("Migration for %s completed successfully! Total documents: %d",
			collConfig.SourceCollection, migratedCount)
	}

	// Always clean up backfill checkpoints when the full collection scan completes. If failedCount > 0, the failed documents will be found in the DLQ, and they should be handled explicitly and separately by users.
	tracker.MarkCompleted()
	if err := DeletePartitionCheckpoints(checkpointDir, sourceDB.GetDatabaseName(), collConfig.SourceCollection); err != nil {
		m.log.Warnf("[%s.%s] Failed to delete checkpoint files on completion: %v", sourceDB.GetDatabaseName(), collConfig.SourceCollection, err)
	}
	return successCount, failedCount, nil
}

// processBatch processes a batch of documents
func processBatch(ctx context.Context, collection *mongo.Collection, batch []interface{}, useUpsert bool, dbName, collName string, transformer *FieldTransformer) error {
	if len(batch) == 0 {
		return nil
	}

	transformedBatch, transErr := transformer.TransformBatch(batch, dbName, collName)
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
		m.config.IDTypeForPartition,
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

			// [Safety Fix 1: MongoDB Cursor Timeout] SetNoCursorTimeout(true) is used to prevent the partitioned MongoDB read cursor from timing out
			// when downstream write rate-limiting/backpressure pauses readers.
			cursor, err := sourceCollection.Find(ctx, filter, options.Find().SetBatchSize(int32(m.config.InitialReadBatchSize)).SetNoCursorTimeout(true))
			if err != nil {
				errorChan <- fmt.Errorf("failed to create cursor for partition %d: %w", partitionIndex, err)
				return
			}
			// [Safety Fix 5: Memory Leak on Server] Ensure partition cursor is closed on exit to prevent server-side resource leakage.
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

			proactiveSkipEnabled := &atomic.Bool{}

			m.log.Debugf("Starting %d workers for partition %d", workerCount, partitionIndex)

			for w := 0; w < workerCount; w++ {
				partitionWg.Add(1)
				go func(workerID int) {
					defer partitionWg.Done()

					for batch := range partitionBatchChan {
						if opts.BackfillStatsManager != nil {
							opts.BackfillStatsManager.RecordWorkerReceived(int64(len(batch)))
						}
						globalWorkerID := partitionIndex*100 + workerID
						succeeded, failed, err := m.writeBatch(ctx, targetCollection, batch, sourceDB.GetDatabaseName(), collConfig.SourceCollection, opts, retryManager, globalWorkerID, proactiveSkipEnabled)
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
				readStart := time.Now()
				hasNext := cursor.Next(ctx)
				if !hasNext {
					break
				}

				// Decode document
				var doc bson.D
				if err := cursor.Decode(&doc); err != nil {
					close(partitionBatchChan)
					errorChan <- fmt.Errorf("failed to decode document in partition %d: %w", partitionIndex, err)
					return
				}
				readDuration := time.Since(readStart)

				if opts.BackfillStatsManager != nil {
					opts.BackfillStatsManager.RecordRead(readDuration, len(cursor.Current))
				}

				// Add to batch
				batch = append(batch, doc)
				batchCount++

				if batchCount >= m.config.InitialWriteBatchSize {
					sendStart := time.Now()
					select {
					case partitionBatchChan <- batch:
						if opts.BackfillStatsManager != nil {
							opts.BackfillStatsManager.RecordIngestQueueStall(time.Since(sendStart))
						}
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
				sendStart := time.Now()
				select {
				case partitionBatchChan <- batch:
					if opts.BackfillStatsManager != nil {
						opts.BackfillStatsManager.RecordIngestQueueStall(time.Since(sendStart))
					}
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
	if m.DryRun {
		m.log.Info("[Dry Run] Skipping index synchronization")
		return nil
	}

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

// logFailedIndexes checks for and logs any indexes that failed to be created.
func (m *Migrator) logFailedIndexes(targetDB *db.MongoDB) {
	failed := targetDB.GetFailedIndexes()
	if len(failed) == 0 {
		return
	}
	m.log.Warnf("=== %d INDEX(ES) FAILED TO CREATE ===", len(failed))
	for i, f := range failed {
		m.log.Warnf("  [%d] Collection: %s, Index: %s, Error: %v", i+1, f.Collection, f.IndexName, f.Error)
	}
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
func (m *Migrator) writeBatch(ctx context.Context, targetCol *mongo.Collection, batch []interface{}, sourceDB, sourceCollection string, opts MigrateOptions, retryManager *RetryManager, workerID int, proactiveSkipEnabled *atomic.Bool) (int64, int64, error) {
	convertInvalidIds := m.config.RetryConfig.ConvertInvalidIds && m.isLive
	transformer := NewFieldTransformer(m.config.DropEmptyFieldNames, m.config.ConvertLongFieldNamesInNestedDocs, convertInvalidIds, m.log)

	writeStart := time.Now()

	if opts.DLQ != nil {
		originalBatch := make([]interface{}, len(batch))
		copy(originalBatch, batch)

		// Resilient Mode: Use DLQ Fallback (exactly identical to client_stream.go)
		transformedBatch, err := transformer.TransformBatch(batch, sourceDB, sourceCollection)
		if err != nil {
			m.log.Errorf("Field name transformation failed for batch in %s.%s: %v", sourceDB, sourceCollection, err)
			for _, doc := range batch {
				docID := extractDocID(doc)
				opts.DLQ.WriteFailed(sourceDB, sourceCollection, docID, err, "initial", "insert", doc, time.Time{})
			}
			writeDuration := time.Since(writeStart)
			if opts.BackfillStatsManager != nil {
				opts.BackfillStatsManager.RecordBulkWrite(writeDuration)
				opts.BackfillStatsManager.RecordWriteResult(0, int64(len(batch)), 0, int64(len(batch)), workerID)
			}
			return 0, int64(len(batch)), nil
		}
		batch = transformedBatch

		var batchFailed int64
		var duplicateKeys int64
		var dlqCount int64

		if proactiveSkipEnabled != nil && proactiveSkipEnabled.Load() {
			var ids []interface{}
			for _, doc := range batch {
				id := extractDocID(doc)
				if id != nil {
					ids = append(ids, id)
				}
			}

			if len(ids) > 0 {
				findCtx, cancelFind := context.WithTimeout(ctx, 30*time.Second)
				filter := bson.M{"_id": bson.M{"$in": ids}}
				projectionOpts := options.Find().SetProjection(bson.M{"_id": 1})
				cursor, findErr := targetCol.Find(findCtx, filter, projectionOpts)
				if findErr == nil {
					existingKeys := make(map[string]bool)
					for cursor.Next(findCtx) {
						var res struct {
							ID interface{} `bson:"_id"`
						}
						if err := cursor.Decode(&res); err == nil && res.ID != nil {
							existingKeys[toComparableIDKey(res.ID)] = true
						}
					}
					cursor.Close(findCtx)
					cancelFind()

					if len(existingKeys) == len(batch) {
						duplicateKeys = int64(len(batch))
						m.log.Debugf("[%s.%s] Proactively skipped entire batch of %d documents (already exist)", sourceDB, sourceCollection, len(batch))
						if opts.BackfillStatsManager != nil {
							opts.BackfillStatsManager.RecordWriteResult(0, 0, duplicateKeys, 0, workerID)
						}
						return 0, 0, nil
					} else if len(existingKeys) > 0 {
						var filteredBatch []interface{}
						var filteredOriginal []interface{}
						var skippedCount int64
						for idx, doc := range batch {
							id := extractDocID(doc)
							if existingKeys[toComparableIDKey(id)] {
								skippedCount++
							} else {
								filteredBatch = append(filteredBatch, doc)
								filteredOriginal = append(filteredOriginal, originalBatch[idx])
							}
						}
						m.log.Debugf("[%s.%s] Proactively skipped %d/%d duplicate documents", sourceDB, sourceCollection, skippedCount, len(batch))
						duplicateKeys += skippedCount
						batch = filteredBatch
						originalBatch = filteredOriginal
						proactiveSkipEnabled.Store(false)
					} else {
						proactiveSkipEnabled.Store(false)
					}
				} else {
					m.log.Warnf("[%s.%s] Proactive ID presence check failed: %v", sourceDB, sourceCollection, findErr)
					cancelFind()
				}
			}
		}

		// [Safety Fix 2: Connection Pool Starvation] Wait on Rate Limiting BEFORE checking out connections/sessions from target database pool.
		// This prevents worker threads from holding onto checked-out sessions in an idle state while blocked by rate limits,
		// which would starve other threads/replicators of available pool connections.
		if opts.Throttler != nil {
			if err := opts.Throttler.Wait(ctx, len(batch)); err != nil {
				return 0, 0, err
			}
		}

		// [Safety Fix 3: Context Timeout Shrinkage] Construct the query-level timeout context AFTER returning from the throttler wait block.
		// Constructing it beforehand would cause the rate-limit wait delay to count against the execution timeout
		// (timeout shrinkage), leading to premature write context cancellation failures.
		bulkCtx, cancelBulk := context.WithTimeout(ctx, 90*time.Second)
		bulkStart := time.Now()
		_, insertErr := targetCol.InsertMany(bulkCtx, batch, options.InsertMany().SetOrdered(false))
		bulkDuration := time.Since(bulkStart)
		cancelBulk()

		if opts.Throttler != nil {
			opts.Throttler.ReportResult(bulkDuration, isSystemError(insertErr))
		}

		if insertErr == nil {
			if opts.BackfillStatsManager != nil {
				opts.BackfillStatsManager.RecordBulkWrite(bulkDuration)
				opts.BackfillStatsManager.RecordWriteResult(int64(len(batch)), 0, 0, 0, workerID)
			}
			return int64(len(batch)), 0, nil
		}

		if opts.BackfillStatsManager != nil {
			opts.BackfillStatsManager.RecordBulkWrite(bulkDuration)
		}

		// Use errors.As instead of direct type assertion (insertErr.(mongo.BulkWriteException))
		// because the driver or retry wrapper may wrap the underlying BulkWriteException.
		var bulkWriteException mongo.BulkWriteException
		ok := errors.As(insertErr, &bulkWriteException)
		if ok {
			m.log.Debugf("Bulk insert partially failed for %s.%s: %d failed",
				sourceDB, sourceCollection, len(bulkWriteException.WriteErrors))

			if len(bulkWriteException.WriteErrors) == len(batch) {
				allDuplicates := true
				for _, writeErr := range bulkWriteException.WriteErrors {
					if writeErr.Code != 11000 {
						allDuplicates = false
						break
					}
				}
				if allDuplicates && proactiveSkipEnabled != nil {
					proactiveSkipEnabled.Store(true)
				}
			}

			for _, writeErr := range bulkWriteException.WriteErrors {
				var errDocID interface{}
				if writeErr.Index < len(batch) {
					errDocID = extractDocID(batch[writeErr.Index])
				}

				m.log.Debugf("[%s.%s] Insert error at index %d, _id=%v: %v", sourceDB, sourceCollection, writeErr.Index, errDocID, writeErr.Message)

				if writeErr.Code == 11000 && writeErr.Index < len(batch) {
					// Case 1: The server responded successfully but flagged a duplicate key error (code 11000).
					// Since we have 100% certainty that the document exists on the target and this is a backfill
					// of identical data, we can safely skip overwriting it to optimize performance and disk writes.
					m.log.Debugf("[%s.%s] Skipping duplicate document _id=%v at index %d", sourceDB, sourceCollection, errDocID, writeErr.Index)
					duplicateKeys++
				} else if writeErr.Index < len(batch) {
					// Note: We do not classify the error here to decide whether to skip the fallback.
					// Attempting fallback write first is safer: (1) if classification regexes are incomplete,
					// we avoid dropping valid writes; (2) some errors are batch-specific and succeed individually.
					doc := batch[writeErr.Index]
					id := extractDocID(doc)

					if id != nil {
						filter := bson.M{"_id": id}
						if opts.BackfillStatsManager != nil {
							opts.BackfillStatsManager.IncrementSequentialRetries("replace", 1)
						}
						err := m.executeWithRetry(ctx, retryManager, func() error {
							fallbackCtx, cancelFallback := context.WithTimeout(ctx, 10*time.Second)
							defer cancelFallback()
							_, replaceErr := targetCol.ReplaceOne(fallbackCtx, filter, doc, options.Replace().SetUpsert(true))
							return replaceErr
						})
						if err != nil {
							m.log.Errorf("[%s.%s] Retry upsert failed for document _id=%v: %v", sourceDB, sourceCollection, id, err)
							batchFailed++
							dlqCount++
							opts.DLQ.WriteFailed(sourceDB, sourceCollection, id, err, "initial", "insert", originalBatch[writeErr.Index], time.Time{})
						}
					} else {
						batchFailed++
						dlqCount++
						opts.DLQ.WriteFailed(sourceDB, sourceCollection, nil, fmt.Errorf("missing _id"), "initial", "insert", originalBatch[writeErr.Index], time.Time{})
					}
				}
			}
		} else {
			// Handle non-bulk write errors (e.g. network partition or context timeout)
			bulkRetrySucceeded := false
			if retryManager != nil && insertErr != context.Canceled {
				errType := retryManager.ClassifyError(insertErr)
				if errType == ErrorTypeConnection || errType == ErrorTypeContention {
					m.log.Infof("Transient error detected for %s.%s. Retrying bulk insert with backoff...", sourceDB, sourceCollection)
					retryErr := retryManager.RetryWithBackoff(ctx, func() error {
						retryCtx, cancelRetry := context.WithTimeout(ctx, 90*time.Second)
						defer cancelRetry()
						_, retryInsertErr := targetCol.InsertMany(retryCtx, batch, options.InsertMany().SetOrdered(false))
						return retryInsertErr
					})
					if retryErr == nil {
						m.log.Infof("Bulk insert for %s.%s succeeded after retry", sourceDB, sourceCollection)
						bulkRetrySucceeded = true
					} else {
						// Optimization: If the bulk retry failed solely due to duplicate key errors,
						// it means the documents were successfully written during the first (timed-out) attempt
						// and the remaining documents succeeded in the retry attempt. We can skip the slow fallback.
						var bulkWriteException mongo.BulkWriteException
						if errors.As(retryErr, &bulkWriteException) {
							allDuplicates := true
							for _, writeErr := range bulkWriteException.WriteErrors {
								if writeErr.Code != 11000 {
									allDuplicates = false
									break
								}
							}
							if allDuplicates {
								successCount := int64(len(batch) - len(bulkWriteException.WriteErrors))
								duplicateKeys = int64(len(bulkWriteException.WriteErrors))
								m.log.Infof("Bulk insert for %s.%s succeeded after retry with %d duplicate keys skipped (Optimization)",
									sourceDB, sourceCollection, duplicateKeys)
								bulkRetrySucceeded = true
								if proactiveSkipEnabled != nil {
									proactiveSkipEnabled.Store(true)
								}
								if opts.BackfillStatsManager != nil {
									opts.BackfillStatsManager.RecordWriteResult(successCount, 0, duplicateKeys, 0, workerID)
								}
								return successCount, 0, nil
							}
						}
						m.log.Warnf("Bulk insert for %s.%s still failed after retries: %v. Falling back to individual operations.", sourceDB, sourceCollection, retryErr)
					}
				}
			}

			if !bulkRetrySucceeded {
				// Fall back to individual operations with upsert for all documents
				for idx, doc := range batch {
					if opts.BackfillStatsManager != nil {
						opts.BackfillStatsManager.IncrementSequentialRetries("insert", 1)
					}
					err := m.executeWithRetry(ctx, retryManager, func() error {
						fallbackCtx, cancelFallback := context.WithTimeout(ctx, 10*time.Second)
						defer cancelFallback()
						_, insertErr := targetCol.InsertOne(fallbackCtx, doc)
						return insertErr
					})
					if err != nil {
						// [Safety Fix 4: Resilient Fallback Stale Overwrites]
						// If the individual InsertOne fails with a duplicate key error (code 11000) during resilient fallback,
						// we skip replacing it. Using ReplaceOne(upsert: true) here could revert newer updates or deletions
						// applied concurrently by the live stream replicator.
						isDup := false
						if writeErr, ok := err.(mongo.WriteException); ok {
							for _, we := range writeErr.WriteErrors {
								if we.Code == 11000 {
									isDup = true
									break
								}
							}
						}
						if !isDup && (strings.Contains(err.Error(), "duplicate key error") || strings.Contains(err.Error(), "E11000")) {
							isDup = true
						}

						if isDup {
							m.log.Debugf("[%s.%s] Fallback insert found existing document _id=%v, skipping overwrite to prevent stale reversion", sourceDB, sourceCollection, extractDocID(doc))
							duplicateKeys++
						} else {
							docID := extractDocID(doc)
							batchFailed++
							dlqCount++
							if opts.DLQ != nil {
								opts.DLQ.WriteFailed(sourceDB, sourceCollection, docID, err, "initial", "insert", originalBatch[idx], time.Time{})
							}
						}
					}
				}
			}
		}

		successCount := int64(len(batch)) - batchFailed
		if opts.BackfillStatsManager != nil {
			opts.BackfillStatsManager.RecordWriteResult(successCount, batchFailed, duplicateKeys, dlqCount, workerID)
		}
		return successCount, batchFailed, nil
	} else {
		// Fail-Fast Mode (standard mode=migrate behavior)
		if opts.Throttler != nil {
			if err := opts.Throttler.Wait(ctx, len(batch)); err != nil {
				return 0, 0, err
			}
		}
		err := retryManager.RetryWithSplit(ctx, batch, sourceCollection, func(b []interface{}) error {
			bulkCtx, cancelBulk := context.WithTimeout(ctx, 90*time.Second)
			defer cancelBulk()
			return processBatch(bulkCtx, targetCol, b, opts.UpsertMode, sourceDB, sourceCollection, transformer)
		})
		writeDuration := time.Since(writeStart)
		if opts.Throttler != nil {
			opts.Throttler.ReportResult(writeDuration, isSystemError(err))
		}
		if err != nil {
			if opts.BackfillStatsManager != nil {
				opts.BackfillStatsManager.RecordBulkWrite(writeDuration)
				opts.BackfillStatsManager.RecordWriteResult(0, int64(len(batch)), 0, 0, workerID)
			}
			return 0, int64(len(batch)), err
		}
		if opts.BackfillStatsManager != nil {
			opts.BackfillStatsManager.RecordBulkWrite(writeDuration)
			opts.BackfillStatsManager.RecordWriteResult(int64(len(batch)), 0, 0, 0, workerID)
		}
		return int64(len(batch)), 0, nil
	}
}

func (m *Migrator) executeWithRetry(ctx context.Context, retryManager *RetryManager, op func() error) error {
	err := op()
	if err != nil && retryManager != nil && err != context.Canceled && ctx.Err() != context.Canceled {
		errType := retryManager.ClassifyError(err)
		if errType == ErrorTypeConnection || errType == ErrorTypeContention {
			m.log.Infof("Transient error during fallback execution. Retrying with backoff...")
			err = retryManager.RetryWithBackoff(ctx, op)
		}
	}
	return err
}

// TargetCollection defines the minimal DB operations interface required for DLQ reprocessing,
// allowing it to be mocked in unit tests.
type TargetCollection interface {
	ReplaceOne(ctx context.Context, filter interface{}, replacement interface{}, opts ...*options.ReplaceOptions) (*mongo.UpdateResult, error)
	DeleteOne(ctx context.Context, filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error)
}

// reprocessDLQ reads the DLQ file for a database pair, re-applies failures, and writes new failures to a new DLQ.
func (m *Migrator) reprocessDLQ(ctx context.Context, pair config.DatabasePair, pairIndex int) (runErr error) {
	statePath := m.getInitialMigrationStatePath(pairIndex)
	initialMigrationState, err := LoadInitialMigrationState(statePath)
	if err != nil {
		return fmt.Errorf("failed to load initial migration state: %w", err)
	}

	dlqPath := m.getDLQPath(pairIndex)
	tempPath := dlqPath + ".retry-temp"

	// Recover leftover temporary file from a crashed previous run if it exists.
	// Discards any active partial file and restores the original temp file.
	if err := m.restoreLeftoverTempDLQ(dlqPath, tempPath); err != nil {
		return fmt.Errorf("failed to restore leftover temp DLQ file: %w", err)
	}

	m.log.Infof("Starting DLQ reprocessing for pair %d (%s -> %s) using file: %s", pairIndex, pair.Source.Database, pair.Target.Database, dlqPath)

	// Check if the DLQ file exists
	if _, err := os.Stat(dlqPath); os.IsNotExist(err) {
		m.log.Infof("No DLQ file found at %s. Skipping retry.", dlqPath)
		return nil
	}

	// Rename the DLQ file so we can read it while opening a new clean DLQ file for new failures
	if err := os.Rename(dlqPath, tempPath); err != nil {
		return fmt.Errorf("failed to rename DLQ file for reprocessing: %w", err)
	}
	defer func() {
		if runErr == nil {
			_ = os.Remove(tempPath)
		} else {
			m.log.Warnf("DLQ retry failed: %v. Restoring original DLQ file to preserve integrity.", runErr)
			_ = os.Remove(dlqPath) // Discard incomplete active retry file
			if restoreErr := os.Rename(tempPath, dlqPath); restoreErr != nil {
				m.log.Errorf("DLQ recovery: Failed to restore leftover temp DLQ file: %v", restoreErr)
			}
		}
	}()

	targetDB, err := db.NewMongoDB(pair.Target.ConnectionString, pair.Target.Database, 1, 10, 0, nil, m.log)
	if err != nil {
		return fmt.Errorf("failed to connect to target MongoDB: %w", err)
	}
	defer func() {
		if err := targetDB.Close(ctx); err != nil {
			m.log.Errorf("Error closing target DB: %v", err)
		}
	}()

	// Open new DLQ writer for any failures that occur during retry
	newDLQ, err := NewDLQWriter(dlqPath, m.log)
	if err != nil {
		return fmt.Errorf("failed to create new DLQ writer: %w", err)
	}
	defer newDLQ.Close()

	getCollection := func(collectionName string) TargetCollection {
		return targetDB.GetCollection(collectionName)
	}

	phase, failedCount, err := m.reprocessDLQLoop(ctx, tempPath, newDLQ, getCollection)
	if err != nil {
		runErr = err
		return runErr
	}

	// Sync initial migration state if we successfully completed reprocessing for the initial phase
	// and the original status was StatusCompletedWithFailures.
	// If the state was StatusInProgress, we do not mark it as completed because it was interrupted mid-run.
	if phase == "initial" && initialMigrationState != nil && initialMigrationState.Status == StatusCompletedWithFailures {
		statePath := m.getInitialMigrationStatePath(pairIndex)
		if failedCount == 0 {
			m.log.Infof("DLQ retry: Initial migration completed successfully. Updating state file to StatusCompleted.")
			if stateErr := SaveInitialMigrationState(statePath, StatusCompleted, 0); stateErr != nil {
				m.log.Errorf("Failed to update initial migration state: %v", stateErr)
			}
		} else {
			m.log.Warnf("DLQ retry: Initial migration completed with %d remaining failures. Updating state file.", failedCount)
			if stateErr := SaveInitialMigrationState(statePath, StatusCompletedWithFailures, failedCount); stateErr != nil {
				m.log.Errorf("Failed to update initial migration state: %v", stateErr)
			}
		}
	}

	return nil
}

// reprocessDLQLoop processes records inside the DLQ temp file, writing them back to target using the getCollection helper.
// It performs a chronological scan to de-duplicate updates and ignore resolved records, ensuring zero stale write overwrites.
func (m *Migrator) reprocessDLQLoop(ctx context.Context, tempPath string, newDLQ *DLQWriter, getCollection func(string) TargetCollection) (phase string, failedCount int64, runErr error) {
	file, err := os.Open(tempPath)
	if err != nil {
		return "", 0, fmt.Errorf("failed to open temp DLQ file: %w", err)
	}
	defer file.Close()

	// Read all lines into memory first to facilitate chronological scan and EOF analysis
	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) != "" {
			lines = append(lines, line)
		}
	}
	if err := scanner.Err(); err != nil {
		return "", 0, fmt.Errorf("error reading temp DLQ file: %w", err)
	}

	convertInvalidIds := true
	if m.config != nil {
		convertInvalidIds = m.config.RetryConfig.ConvertInvalidIds
	}
	transformer := NewFieldTransformer(
		m.config.DropEmptyFieldNames,
		m.config.ConvertLongFieldNamesInNestedDocs,
		convertInvalidIds,
		m.log,
	)

	type activeFailure struct {
		record  DLQRecord
		rawLine string
	}
	failuresMap := make(map[string]*activeFailure)
	var expectedPhase string

	// Pass 1: Chronological Scan & De-duplication
	for i, line := range lines {
		if i == 0 {
			var header struct {
				DLQVersion string `bson:"dlqVersion"`
			}
			if err := bson.UnmarshalExtJSON([]byte(line), false, &header); err == nil && header.DLQVersion != "" {
				if header.DLQVersion != DLQVersion {
					runErr = fmt.Errorf("DLQ retry: Safety violation: encountered unsupported DLQ version %q (expected %q). Please use the correct migration tool version to reprocess this file.", header.DLQVersion, DLQVersion)
					return "", 0, runErr
				}
				m.log.Infof("DLQ retry: DLQ file version %q matches tool DLQ version %q", header.DLQVersion, DLQVersion)
				continue // skip version header line
			}
		}

		var record DLQRecord
		if err := bson.UnmarshalExtJSON([]byte(line), false, &record); err != nil {
			// Gracefully skip corrupted lines at the very end of the file (survive partial write crashes)
			if i == len(lines)-1 {
				m.log.Warnf("DLQ retry: Skipping corrupted line at EOF (probable mid-write crash fragment): %v. Line: %s", err, line)
				continue
			}
			// Middle-file corruption represents a safety violation; abort replay immediately
			runErr = fmt.Errorf("DLQ retry: Failed to unmarshal record: %w. Line: %s", err, line)
			return "", 0, runErr
		}

		// Enforce single-phase validation
		if expectedPhase == "" {
			expectedPhase = record.Phase
			m.log.Infof("DLQ retry: Set expected phase to %q based on first record", expectedPhase)
		} else if record.Phase != expectedPhase {
			runErr = fmt.Errorf("DLQ retry: Safety violation: encountered mixed phases in DLQ file (expected %q, got %q). Reprocessing aborted.", expectedPhase, record.Phase)
			return "", 0, runErr
		}

		// Unique key per document identity
		if record.ResolvedID != nil {
			uniqueKey := MakeDLQKey(record.SourceDB, record.SourceCollection, record.ResolvedID)
			delete(failuresMap, uniqueKey)
		} else {
			uniqueKey := MakeDLQKey(record.SourceDB, record.SourceCollection, record.DocumentID)
			failuresMap[uniqueKey] = &activeFailure{
				record:  record,
				rawLine: line,
			}
		}
	}

	var processed, succeeded, failed int64

	// Pass 2: Execute Replays
	for _, f := range failuresMap {
		if ctx.Err() != nil {
			runErr = ctx.Err()
			return "", 0, runErr
		}

		processed++
		record := f.record

		m.log.Debugf("Retrying document [db=%s, collection=%s, id=%v, op=%s]",
			record.SourceDB, record.SourceCollection, record.DocumentID, record.OpType)

		targetColl := getCollection(record.SourceCollection)
		var writeErr error

		if record.OpType == "delete" {
			_, writeErr = targetColl.DeleteOne(ctx, bson.M{"_id": record.DocumentID})
		} else { // insert, update, replace, mixed
			var docToReplace interface{} = record.Document
			if record.Document != nil {
				transformed, err := transformer.Transform(record.Document, record.SourceDB, record.SourceCollection, record.DocumentID)
				if err != nil {
					m.log.Errorf("DLQ retry: Failed to transform document %v: %v", record.DocumentID, err)
					failed++
					newDLQ.WriteFailed(record.SourceDB, record.SourceCollection, record.DocumentID, err, record.Phase, record.OpType, record.Document, time.Time{})
					continue
				}
				docToReplace = transformed
			}
			opts := options.Replace().SetUpsert(true)
			_, writeErr = targetColl.ReplaceOne(ctx, bson.M{"_id": record.DocumentID}, docToReplace, opts)
		}

		if writeErr != nil {
			m.log.Errorf("DLQ retry: Failed to write document %v to target: %v", record.DocumentID, writeErr)
			failed++
			var eventTime time.Time
			if record.EventTime != "" {
				if t, err := time.Parse(time.RFC3339, record.EventTime); err == nil {
					eventTime = t
				}
			}
			newDLQ.WriteFailed(record.SourceDB, record.SourceCollection, record.DocumentID, writeErr, record.Phase, record.OpType, record.Document, eventTime)
		} else {
			m.log.Debugf("DLQ retry: Successfully recovered document %v", record.DocumentID)
			succeeded++
		}
	}

	m.log.Infof("DLQ reprocessing complete: processed %d, succeeded %d, failed %d", processed, succeeded, failed)
	return expectedPhase, failed, nil
}

// restoreLeftoverTempDLQ checks if a temporary DLQ retry file exists from a crashed run.
// If both active and temp files exist, it renames the temp file back to the active file.
// If only the temp file exists, it renames it to the active file.
func (m *Migrator) restoreLeftoverTempDLQ(dlqPath, tempPath string) error {
	// If temp file does not exist, nothing to restore
	if _, err := os.Stat(tempPath); os.IsNotExist(err) {
		return nil
	}

	// If active file does not exist, we can safely rename the temp file to active file
	if _, err := os.Stat(dlqPath); os.IsNotExist(err) {
		m.log.Warnf("DLQ recovery: Leftover retry-temp file found from previous crashed run. Restoring to %s.", dlqPath)
		return os.Rename(tempPath, dlqPath)
	}

	// Both files exist! Since we are doing "discard and restore" crash safety:
	// We discard the incomplete active file (from the crashed run) and restore the temp file as active.
	m.log.Warnf("DLQ recovery: Leftover retry-temp file and incomplete active file found. Discarding active file and restoring %s to %s.", tempPath, dlqPath)
	_ = os.Remove(dlqPath)
	return os.Rename(tempPath, dlqPath)
}

// isSystemError checks if the write error is a general database/network error rather than a benign duplicate key error.
func isSystemError(err error) bool {
	if err == nil || err == context.Canceled {
		return false
	}
	var bulkWriteException mongo.BulkWriteException
	if errors.As(err, &bulkWriteException) {
		for _, writeErr := range bulkWriteException.WriteErrors {
			if writeErr.Code != 11000 { // 11000 is duplicate key
				return true
			}
		}
		return false
	}
	return true
}
