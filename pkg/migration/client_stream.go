package migration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gsbingo17/mongodb-migration/pkg/config"
	"github.com/gsbingo17/mongodb-migration/pkg/db"
	"github.com/gsbingo17/mongodb-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// ClientLevelReplicator handles replication using a client-level change stream
type ClientLevelReplicator struct {
	sourceDB          *db.MongoDB
	targetDB          *db.MongoDB
	config            *config.Config
	log               *logger.Logger
	collectionMap     map[string]map[string]string // Map of database -> source collection -> target collection
	collectionConfigs map[string]map[string]config.CollectionConfig // Map of database -> source collection -> full config
	mu                sync.Mutex                   // Mutex for thread-safe operations
	dlq               DLQ                          // Dead Letter Queue for failed documents
	incrementalStatsManager      *IncrementalStatsManager                // Statistics manager
	backfillStatsManager         *BackfillStatsManager    // Backfill statistics manager
	DryRun            bool                         // Dry run flag
}

// NewClientLevelReplicator creates a new client-level replicator
func NewClientLevelReplicator(sourceDB, targetDB *db.MongoDB, cfg *config.Config, log *logger.Logger) *ClientLevelReplicator {
	return &ClientLevelReplicator{
		sourceDB:          sourceDB,
		targetDB:          targetDB,
		config:            cfg,
		log:               log,
		collectionMap:     make(map[string]map[string]string),
		collectionConfigs: make(map[string]map[string]config.CollectionConfig),
	}
}

// SetIncrementalStatsManager sets the stats manager for this replicator
func (r *ClientLevelReplicator) SetIncrementalStatsManager(sm *IncrementalStatsManager) {
	r.incrementalStatsManager = sm
}

// SetBackfillStatsManager sets the backfill stats manager for this replicator
func (r *ClientLevelReplicator) SetBackfillStatsManager(sm *BackfillStatsManager) {
	r.backfillStatsManager = sm
}

// SetDLQ sets the Dead Letter Queue writer for this replicator
func (r *ClientLevelReplicator) SetDLQ(dlq DLQ) {
	r.dlq = dlq
}

// AddCollection adds a collection to be watched
func (r *ClientLevelReplicator) AddCollection(sourceDB, targetDB string, collConfig config.CollectionConfig) {
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

// StartReplication starts the client-level replication
func (r *ClientLevelReplicator) StartReplication(ctx context.Context, globalResumeToken interface{}, globalResumeTokenPath string, initialMigrationState *InitialMigrationState, initialMigrationStatePath string, pair config.DatabasePair, liveOnly bool, liveStartTime *primitive.Timestamp, migrator *Migrator) error {
	if r.log == nil {
		r.log = logger.New()
	}
	partitions := 1
	if r.config != nil {
		partitions = r.config.IncrementalStreamPartitions
	}

	// Scan for any files matching: resumeToken-<db>-<coll>-partition-*-of-*.json
	dir := filepath.Dir(globalResumeTokenPath)
	files, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("failed to read checkpoint directory %s: %w", dir, err)
	}

	diskCheckpoints := ScanPartitionCheckpoints(files, globalResumeTokenPath)
	historicalTotal, existingPaths, usingCurrentPartitionFormat := ResolveActiveCheckpoints(diskCheckpoints)

	if historicalTotal > 0 {
		if historicalTotal != partitions || usingCurrentPartitionFormat {
			// --- PARTITION TRANSITION OR FORMAT UPGRADE PATH ---
			r.log.Infof("[Startup] Partition scaling/upgrade transition detected: historical partitions = %d, configured partitions = %d. Safe watermark resolution active.", historicalTotal, partitions)

			var minToken interface{}
			var minTime time.Time
			var minFile string

			// Assert that all historical checkpoints are present to prevent silent data loss
			for i := 0; i < historicalTotal; i++ {
				oldPath, exists := existingPaths[i]
				if !exists {
					// CRITICAL SAFETY EXCEPTION: Abort immediately if any historical checkpoint is missing
					return fmt.Errorf("safety violation: historical partition checkpoint file for partition %d of %d is missing on disk. Recovery aborted to prevent silent data loss. Please restore the file or start fresh", i+1, historicalTotal)
				}

				r.log.Infof("[Startup] [Watermark Assessment] Loading historical checkpoint %d/%d: %s", i+1, historicalTotal, filepath.Base(oldPath))
				token, err := LoadResumeToken(oldPath)
				if err != nil || token == nil {
					return fmt.Errorf("failed to load historical partition checkpoint %s: %w", oldPath, err)
				}

				// Load JSON timestamp metadata
				data, err := os.ReadFile(oldPath)
				if err == nil {
					var rt ResumeToken
					if err := json.Unmarshal(data, &rt); err == nil {
						eventTime := rt.Timestamp
						if eventTime.IsZero() {
							eventTime = time.Unix(0, 0) // Fallback for untimestamped tokens
						}
						if minTime.IsZero() || eventTime.Before(minTime) {
							minTime = eventTime
							minToken = token
							minFile = oldPath
						}
					}
				}
			}

			if minToken == nil {
				return fmt.Errorf("safety violation: cannot transition partition count because no valid event timestamps were found in checkpoint files")
			}

			r.log.Infof("[Startup] [Watermark Resolution] Safe unified minimum watermark resolved from %s (timestamp: %s).", filepath.Base(minFile), minTime.UTC().Format(time.RFC3339))
			r.log.Infof("[Startup] [Watermark Resolution] Initializing all %d new partition checkpoints with resolved watermark.", partitions)

			// 1. Initialize all new partition checkpoints
			for i := 0; i < partitions; i++ {
				newPath := GetPartitionResumeTokenPath(globalResumeTokenPath, i, partitions)
				if err := SaveResumeToken(newPath, minToken, minTime); err != nil {
					return fmt.Errorf("[Partition %d] failed to save converted partition checkpoint: %w", i+1, err)
				}
				r.log.Infof("[Startup] [Watermark Resolution] Saved new partition checkpoint: %s", filepath.Base(newPath))
			}

			// 2. Clean up the historical partition checkpoints
			r.log.Info("[Startup] [Watermark Resolution] Cleaning up stale historical partition checkpoints from disk.")
			for _, oldPath := range existingPaths {
				if err := DeleteResumeToken(oldPath); err != nil {
					r.log.Warnf("Failed to clean up stale partition checkpoint %s: %v", oldPath, err)
				} else {
					r.log.Infof("[Startup] [Watermark Resolution] Deleted stale checkpoint: %s", filepath.Base(oldPath))
				}
			}

			globalResumeToken = minToken
		} else {
			// --- NORMAL STARTUP / RESUME PATH ---
			r.log.Infof("[Startup] Normal resume path active. Verifying all %d partition checkpoints are healthy on disk.", partitions)
			
			// Ensure that all expected partition files exist and are valid
			for i := 0; i < partitions; i++ {
				oldPath, exists := existingPaths[i]
				if !exists {
					expectedPath := GetPartitionResumeTokenPath(globalResumeTokenPath, i, partitions)
					// CRITICAL SAFETY EXCEPTION: Missing checkpoint detected on normal resume
					return fmt.Errorf("safety violation: expected partition checkpoint file %s (partition %d of %d) is missing on disk. Fallback aborted to prevent data loss", filepath.Base(expectedPath), i+1, partitions)
				}

				token, err := LoadResumeToken(oldPath)
				if err != nil || token == nil {
					return fmt.Errorf("fatal: partition checkpoint file %s exists but is empty or unreadable: %v", oldPath, err)
				}
				r.log.Infof("[Startup] Verified healthy partition checkpoint %d/%d: %s", i+1, partitions, filepath.Base(oldPath))
			}

			// Populate globalResumeToken from the oldest partition file to satisfy safety invariants
			var minToken interface{}
			var minTime time.Time
			for i := 0; i < partitions; i++ {
				path := existingPaths[i]
				token, _ := LoadResumeToken(path)
				if token != nil {
					data, err := os.ReadFile(path)
					if err == nil {
						var rt ResumeToken
						if err := json.Unmarshal(data, &rt); err == nil {
							eventTime := rt.Timestamp
							if eventTime.IsZero() {
								eventTime = time.Unix(0, 0)
							}
							if minTime.IsZero() || eventTime.Before(minTime) {
								minTime = eventTime
								minToken = token
							}
						}
					}
				}
			}
			if minToken != nil {
				globalResumeToken = minToken
			}
		}
	} else {
		// --- CASE C: LEGACY NAMING (NON-PARTITIONED) UPGRADE ---
		if globalResumeToken != nil {
			r.log.Infof("[Startup] Legacy single change stream checkpoint detected: %s. Upgrading to partitioned change stream (%d partitions configured).", filepath.Base(globalResumeTokenPath), partitions)
			for i := 0; i < partitions; i++ {
				partitionPath := GetPartitionResumeTokenPath(globalResumeTokenPath, i, partitions)
				if err := SaveResumeToken(partitionPath, globalResumeToken); err != nil {
					return fmt.Errorf("[Partition %d] failed to initialize partition checkpoint: %w", i+1, err)
				}
				r.log.Infof("[Startup] Initialized partition checkpoint %d/%d: %s", i+1, partitions, filepath.Base(partitionPath))
			}
		}
	}

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

	// Enforce safety invariants between Initial Migration State and Resume Token Checkpoint
	if liveStartTime != nil && globalResumeToken != nil {
		return fmt.Errorf("safety violation: a custom live-start-timestamp is specified, but a global resume token checkpoint already exists. Clean up checkpoint file or omit live-start-timestamp to resume from the last checkpoint")
	}

	if liveStartTime == nil {
		if initialMigrationState == nil {
			if globalResumeToken != nil {
				return fmt.Errorf("safety violation: initial migration state file does not exist, but a global resume token checkpoint exists. Clean up checkpoint file or ensure state is in sync before proceeding")
			}
		} else if initialMigrationState.Status == StatusCompleted || initialMigrationState.Status == StatusSkipped {
			if globalResumeToken == nil {
				return fmt.Errorf("safety violation: initial migration state is marked as %s, but no global resume token checkpoint was found. Clean up state file or restore checkpoint before proceeding", initialMigrationState.Status)
			}
		}
	}

	var needsInitialMigration bool

	// We need to run initial migration if no state file exists OR if it is not marked completed
	if initialMigrationState == nil || !initialMigrationState.IsCompleted() {
		if liveOnly {
			r.log.Info("Live-only mode enabled. Skipping initial migration phase.")
			// Critical File-System State Checkpoint:
			// If we cannot persist the StatusSkipped state to disk, we exit with a terminal error.
			// Continuing silently would cause subsequent startup runs to attempt the backfill again,
			// leading to massive duplicate processing or index-recreation errors.
			if err := SaveInitialMigrationState(initialMigrationStatePath, StatusSkipped, 0); err != nil {
				return fmt.Errorf("failed to save initial migration state as skipped: %w", err)
			}
			needsInitialMigration = false
			initialMigrationState = &InitialMigrationState{
				Status: StatusSkipped,
			}
		} else {
			needsInitialMigration = true
		}
	}

	// If no resume token is available, and no custom liveStartTime is specified, we need to capture the
	// current cursor state of the database so that we have a valid checkpoint to resume replication from.
	// Note: If liveStartTime is provided, we don't capture a startup resume token, as the client-level
	// change stream will be configured to start replication directly from the specified liveStartTime.
	if globalResumeToken == nil && liveStartTime == nil {
		if liveOnly {
			r.log.Info("No global resume token found in live-only mode. Obtaining current resume token to start incremental replication.")
		} else {
			r.log.Info("No global resume token found. Creating a new one and will perform initial migration.")
		}

		// Create a change stream to get an initial resume token
		initialChangeStream, err := r.sourceDB.CreateClientLevelChangeStream(ctx, nil, nil, 0, nil)
		if err != nil {
			return fmt.Errorf("failed to create initial client-level change stream: %w", err)
		}

		// Get the initial resume token
		initialResumeToken := initialChangeStream.ResumeToken()
		r.log.Infof("Obtained initial resume token: %v", initialResumeToken)

		// Convert the BSON resume token to a map with _data field
		var initialResumeTokenDoc bson.M
		if err := bson.Unmarshal(initialResumeToken, &initialResumeTokenDoc); err != nil {
			return fmt.Errorf("failed to unmarshal initial resume token BSON: %w", err)
		}
		r.log.Infof("Converted initial resume token: %v", initialResumeTokenDoc)

		// Save this initial resume token to all configured partition checkpoint files.
		// Critical Safety Checkpoint: If any file write fails (e.g. permissions, disk full), we abort immediately.
		// Continuing silently would leave incremental replication without starting checkpoints, risking
		// replication gaps or severe operational overhead on subsequent process restarts.
		for i := 0; i < r.config.IncrementalStreamPartitions; i++ {
			partitionPath := GetPartitionResumeTokenPath(globalResumeTokenPath, i, r.config.IncrementalStreamPartitions)
			if err := SaveResumeToken(partitionPath, initialResumeTokenDoc); err != nil {
				return fmt.Errorf("[Partition %d] failed to save initial partition resume token to %s: %w", i, partitionPath, err)
			}
			r.log.Infof("[Partition %d] Saved initial partition resume token successfully to %s", i, partitionPath)
		}

		// Close the initial change stream
		initialChangeStream.Close(ctx)

		// Use the converted resume token
		globalResumeToken = initialResumeTokenDoc
	} else if liveStartTime != nil {
		r.log.Infof("No resume token available. Starting replication from liveStartTime: %s", time.Unix(int64(liveStartTime.T), 0).UTC().Format(time.RFC3339))
	} else {
		r.log.Info("Global resume token available. Starting incremental replication.")
	}

	// Perform initial migration if needed
	if needsInitialMigration {
		// Critical File-System State Checkpoint:
		// We flag the initial migration status as StatusInProgress on disk before running the hot migration loop.
		// If this file write fails, we halt immediately to keep progress safe from untracked restart states.
		if err := SaveInitialMigrationState(initialMigrationStatePath, StatusInProgress, 0); err != nil {
			return fmt.Errorf("failed to save initial migration state as incomplete: %w", err)
		}
		initialMigrationStart := time.Now()
		r.log.Info("Performing initial migration for all collections")

		// Sync indexes before migrating data if configured
		if pair.Target.SyncAllIndexes || len(pair.Target.Indexes) > 0 {
			r.log.Info("Syncing indexes before initial migration")
			var collections []config.CollectionConfig
			for _, colls := range r.collectionConfigs {
				for _, collConfig := range colls {
					collections = append(collections, collConfig)
				}
			}
			if err := migrator.syncIndexes(ctx, r.sourceDB, r.targetDB, pair, collections); err != nil {
				r.log.Warnf("Index sync encountered issues: %v (continuing with migration)", err)
			}

			// Index-Only mode: wait for all async index builds then return without migrating data
			if pair.Target.IndexOnly {
				r.log.Info("IndexOnly mode enabled. Waiting for all async index creation to complete...")
				r.targetDB.WaitForIndexCreation()
				r.log.Info("IndexOnly mode: all indexes synced successfully. Skipping data migration.")

				// Determine if initial migration completed with failures (none in IndexOnly since no data migrated)
				if err := SaveInitialMigrationState(initialMigrationStatePath, StatusCompleted, 0); err != nil {
					r.log.Errorf("Error saving initial migration state as complete: %v", err)
				}
				return nil
			}
		}

		// Use a semaphore to limit the number of concurrent collection migrations
		concurrentCollections := r.config.ConcurrentCollections
		if concurrentCollections <= 0 {
			concurrentCollections = 4
		}
		r.log.Infof("Processing up to %d collections concurrently", concurrentCollections)
		semaphore := make(chan struct{}, concurrentCollections)
		var wg sync.WaitGroup

		// Track overall statistics
		var totalMigratedCount int64
		var totalFailedCount int64
		var completedCollections int64
		var mu sync.Mutex // Mutex for thread-safe updates to statistics

		// Track critical errors
		var criticalErr error
		var errOnce sync.Once

		totalCollections := 0
		for _, colls := range r.collectionConfigs {
			totalCollections += len(colls)
		}

		// Iterate through all collections in the map
		for sourceDB, collections := range r.collectionConfigs {
			for sourceCollection, collConfig := range collections {
				wg.Add(1)
				semaphore <- struct{}{}

				go func(sourceDB, sourceCollection string, collConfig config.CollectionConfig) {
					defer wg.Done()
					defer func() { <-semaphore }()

					r.log.Infof("Starting initial migration for %s.%s to %s", sourceDB, sourceCollection, collConfig.TargetCollection)

					opts := MigrateOptions{
						DLQ:                  r.dlq,
						StatsManager:         r.incrementalStatsManager,
						BackfillStatsManager: r.backfillStatsManager,
						UpsertMode:           true, // resilient mode always performs upserts on duplicates
					}

					succeeded, failed, err := migrator.migrateCollection(ctx, r.sourceDB, r.targetDB, collConfig, opts)
					if err != nil {
						errOnce.Do(func() {
							criticalErr = fmt.Errorf("critical error during migration of collection %s.%s: %w", sourceDB, sourceCollection, err)
						})
					}

					// Update overall statistics and log overall progress
					mu.Lock()
					totalMigratedCount += succeeded + failed
					totalFailedCount += failed
					completedCollections++
					r.log.Infof("Overall progress: %d/%d collections completed", completedCollections, totalCollections)
					mu.Unlock()
				}(sourceDB, sourceCollection, collConfig)
			}
		}

		// Wait for all collection migrations to complete
		wg.Wait()
		if r.backfillStatsManager != nil {
			r.backfillStatsManager.ReportStats(true)
		}

		if criticalErr != nil {
			return criticalErr
		}

		initialMigrationDuration := time.Since(initialMigrationStart)
		var failurePercentage float64
		if totalMigratedCount > 0 {
			failurePercentage = (float64(totalFailedCount) * 100.0) / float64(totalMigratedCount)
		}
		r.log.Infof("Initial migration completed in %.2f seconds. Total collections: %d, Total documents: %d (Success: %d, Failed: %d, Failure Rate: %.2f%%)",
			initialMigrationDuration.Seconds(), totalCollections, totalMigratedCount, totalMigratedCount-totalFailedCount, totalFailedCount, failurePercentage)

		// Issue warning if the sum of all failed collections does not match actual DLQ write count
		if r.dlq != nil {
			if _, isNop := r.dlq.(*NopDLQWriter); !isNop {
				dlqCount := r.dlq.Count()
				if totalFailedCount != dlqCount {
					r.log.Warnf("DLQ Metric mismatch: sum of failed counts across collections (%d) does not match DLQ written count (%d)",
						totalFailedCount, dlqCount)
				}
			}
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

		// Critical File-System State Checkpoint:
		// We persist the completed backfill status (StatusCompleted or StatusCompletedWithFailures) to disk.
		// If this file write fails, we abort replication immediately to avoid starting incremental operations
		// without solid baseline checkpoints on disk.
		if err := SaveInitialMigrationState(initialMigrationStatePath, status, totalFailedCount); err != nil {
			return fmt.Errorf("failed to save initial migration state as complete: %w", err)
		}

		if status == StatusCompletedWithFailures {
			return fmt.Errorf("initial migration completed with %d failures and/or DLQ entries. Aborting replication", totalFailedCount)
		}
		r.log.Info("Starting incremental replication.")
	} else {
		r.log.Info("Initial migration already marked as completed. Skipping.")
	}

	// Index-Only mode: sync indexes (if not already done during initial migration) and exit
	if pair.Target.IndexOnly {
		if !needsInitialMigration && (pair.Target.SyncAllIndexes || len(pair.Target.Indexes) > 0) {
			// Resume token exists, so initial migration was skipped — sync indexes now
			r.log.Info("IndexOnly mode: resume token exists, performing index sync directly")
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
		r.log.Info("IndexOnly mode: skipping change stream. Index replication complete.")
		return nil
	}


	// Load partition-level resume tokens from their independent files.
	// Suffix path names are generated using partition indices (e.g. resumeToken-pair0-0.json, resumeToken-pair0-1.json).
	var partitionTokens []interface{}
	for i := 0; i < r.config.IncrementalStreamPartitions; i++ {
		partitionPath := GetPartitionResumeTokenPath(globalResumeTokenPath, i, r.config.IncrementalStreamPartitions)
		token, err := LoadResumeToken(partitionPath)
		if err != nil || token == nil {
			// If a token is missing/nil here (despite our check), it's a fatal failure in partitioned mode.
			if r.config.IncrementalStreamPartitions > 1 {
				return fmt.Errorf("fatal: partition checkpoint file %s exists but is empty or unreadable: %v", partitionPath, err)
			}
			// Fallback is only allowed in legacy single-stream mode
			r.log.Warnf("[Partition %d] No valid partition checkpoint found at %s (falling back to global checkpoint: %v, token: %v)", i, partitionPath, err, token)
			token = globalResumeToken
		}
		partitionTokens = append(partitionTokens, token)
	}

	var changeStreams []*mongo.ChangeStream
	var openError error

	// Open the partitioned change streams in parallel. Each change stream aggregates with a
	// server-side BSON aggregation pipeline stage filtering for its corresponding deterministic partition index.
	r.log.Infof("Starting %d client-level change streams for all databases and collections", r.config.IncrementalStreamPartitions)
	for i := 0; i < r.config.IncrementalStreamPartitions; i++ {
		var token interface{}
		if len(partitionTokens) > i {
			token = partitionTokens[i]
		}

		// Build a zero-JS, loopless FNV-inspired BSON aggregation pipeline stage for this partition index
		pipeline := BuildPartitionPipeline(i, r.config.IncrementalStreamPartitions)
		r.log.Infof("[Partition %d/%d] Creating client-level change stream (ResumeToken: %v)", i+1, r.config.IncrementalStreamPartitions, token != nil)

		stream, err := r.sourceDB.CreateClientLevelChangeStream(
			ctx,
			token,
			liveStartTime,
			r.config.IncrementalReadBatchSize,
			pipeline,
		)
		if err != nil {
			openError = err
			break
		}
		changeStreams = append(changeStreams, stream)
	}

	if openError != nil {
		// Close any successfully opened change streams first before fallback
		for _, stream := range changeStreams {
			if stream != nil {
				stream.Close(ctx)
			}
		}

		// Check if the error is due to the resume token being too old
		if strings.Contains(openError.Error(), "ChangeStreamHistoryLost") ||
			strings.Contains(openError.Error(), "Resume of change stream was not possible") {
			r.log.Warn("Resume token is too old and no longer in the oplog. Deleting resume token files and starting fresh.")

			// Delete all partition and global resume token files
			for p := 0; p < r.config.IncrementalStreamPartitions; p++ {
				path := GetPartitionResumeTokenPath(globalResumeTokenPath, p, r.config.IncrementalStreamPartitions)
				if err := DeleteResumeToken(path); err != nil {
					r.log.Errorf("Error deleting partition resume token file %s: %v", path, err)
				}
			}
			if err := DeleteResumeToken(globalResumeTokenPath); err != nil {
				r.log.Errorf("Error deleting global resume token file: %v", err)
			}

			// Delete the initial migration state file
			if err := DeleteInitialMigrationState(initialMigrationStatePath); err != nil {
				r.log.Errorf("Error deleting initial migration state file: %v", err)
			}

			// Perform initial migration again
			r.log.Info("Starting fresh with initial migration...")
			return r.StartReplication(ctx, nil, globalResumeTokenPath, nil, initialMigrationStatePath, pair, liveOnly, nil, migrator)
		}

		return fmt.Errorf("failed to create client-level change stream partition: %w", openError)
	}

	defer func() {
		for _, stream := range changeStreams {
			if stream != nil {
				stream.Close(ctx)
			}
		}
	}()

	// Create event distributor for parallel processing
	r.log.Infof("Starting parallel change stream processing with %d workers", r.config.IncrementalWorkerCount)
	distributor := NewEventDistributor(
		ctx,
		r.sourceDB,
		r.targetDB,
		r.collectionConfigs,
		changeStreams,
		r.log,
		globalResumeTokenPath,
		time.Duration(r.config.CheckpointIntervalMinutes)*time.Minute,
		r.config.SaveThreshold,
		r.config.IncrementalWorkerCount,
		r.config.IncrementalWriteBatchSize,
		r.config.ForceOrderedOperations,
		time.Duration(r.config.FlushIntervalMs)*time.Millisecond,
		r.config,
		r.dlq,
		r.incrementalStatsManager,
	)
	distributor.DryRun = r.DryRun

	// Start event distribution
	err = distributor.Start()
	// Don't propagate context.Canceled as an error
	if err == context.Canceled {
		r.log.Info("Replication stopped due to context cancellation")
		return nil
	}
	return err
}


