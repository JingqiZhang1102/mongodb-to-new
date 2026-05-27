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
	statsManager      *StatsManager                // Statistics manager
	DontApply         bool                         // Don't apply flag
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

// SetStatsManager sets the stats manager for this replicator
func (r *ClientLevelReplicator) SetStatsManager(sm *StatsManager) {
	r.statsManager = sm
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
func (r *ClientLevelReplicator) StartReplication(ctx context.Context, globalResumeToken interface{}, globalResumeTokenPath string, initialMigrationState *InitialMigrationState, initialMigrationStatePath string, pair config.DatabasePair, liveOnly bool, cdcStartTime *primitive.Timestamp, migrator *Migrator) error {
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
	if cdcStartTime != nil && globalResumeToken != nil {
		return fmt.Errorf("safety violation: a custom cdc-start-timestamp is specified, but a global resume token checkpoint already exists. Clean up checkpoint file or omit cdc-start-timestamp to resume from the last checkpoint")
	}

	if cdcStartTime == nil {
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

	var changeStream *mongo.ChangeStream
	var err error
	var needsInitialMigration bool

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

	// If no resume token is available, and no custom cdcStartTime is specified, we need to capture the
	// current cursor state of the database so that we have a valid checkpoint to resume replication from.
	// Note: If cdcStartTime is provided, we don't capture a startup resume token, as the client-level
	// change stream will be configured to start replication directly from the specified cdcStartTime.
	if globalResumeToken == nil && cdcStartTime == nil {
		if liveOnly {
			r.log.Info("No global resume token found in live-only mode. Obtaining current resume token to start incremental replication.")
		} else {
			r.log.Info("No global resume token found. Creating a new one and will perform initial migration.")
		}

		// Create a change stream to get an initial resume token
		initialChangeStream, err := r.sourceDB.CreateClientLevelChangeStream(ctx, nil, nil, 0)
		if err != nil {
			return fmt.Errorf("failed to create initial client-level change stream: %w", err)
		}

		// Get the initial resume token
		initialResumeToken := initialChangeStream.ResumeToken()
		r.log.Infof("Obtained initial resume token: %v", initialResumeToken)

		// Convert the BSON resume token to a map with _data field
		var initialResumeTokenDoc bson.M
		if err := bson.Unmarshal(initialResumeToken, &initialResumeTokenDoc); err != nil {
			r.log.Errorf("Error unmarshaling initial resume token: %v", err)
		}
		r.log.Infof("Converted initial resume token: %v", initialResumeTokenDoc)

		// Save this initial resume token
		if err := SaveResumeToken(globalResumeTokenPath, initialResumeTokenDoc); err != nil {
			r.log.Errorf("Error saving initial global resume token: %v", err)
		} else {
			r.log.Info("Saved initial global resume token")
		}

		// Close the initial change stream
		initialChangeStream.Close(ctx)

		// Use the converted resume token
		globalResumeToken = initialResumeTokenDoc
	} else if cdcStartTime != nil {
		r.log.Infof("No resume token available. Starting replication from cdcStartTime: %s", time.Unix(int64(cdcStartTime.T), 0).UTC().Format(time.RFC3339))
	} else {
		r.log.Info("Global resume token available. Starting incremental replication.")
	}

	// Perform initial migration if needed
	if needsInitialMigration {
		// Mark initial migration state as incomplete before starting
		if err := SaveInitialMigrationState(initialMigrationStatePath, StatusInProgress, 0); err != nil {
			r.log.Errorf("Error saving initial migration state as incomplete: %v", err)
		}

		retryManager := NewRetryManagerFromConfig(r.config, r.log)
		initialMigrator := NewInitialMigrator(r.sourceDB, r.targetDB, r.config, r.log, r.collectionConfigs, r.dlq, retryManager)
		initialMigrator.DontApply = r.DontApply
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

	// Create client-level change stream with the resume token and batch size
	r.log.Info("Starting client-level change stream for all databases and collections")
	changeStream, err = r.sourceDB.CreateClientLevelChangeStream(
		ctx,
		globalResumeToken,
		cdcStartTime,
		r.config.IncrementalReadBatchSize,
	)
	if err != nil {
		// Check if the error is due to the resume token being too old
		if strings.Contains(err.Error(), "ChangeStreamHistoryLost") ||
			strings.Contains(err.Error(), "Resume of change stream was not possible") {
			r.log.Warn("Resume token is too old and no longer in the oplog. Deleting resume token and starting fresh.")

			// Delete the resume token file
			if err := DeleteResumeToken(globalResumeTokenPath); err != nil {
				r.log.Errorf("Error deleting resume token file: %v", err)
			}

			// Delete the initial migration state file
			if err := DeleteInitialMigrationState(initialMigrationStatePath); err != nil {
				r.log.Errorf("Error deleting initial migration state file: %v", err)
			}

			// Perform initial migration again
			r.log.Info("Starting fresh with initial migration...")
			return r.StartReplication(ctx, nil, globalResumeTokenPath, nil, initialMigrationStatePath, pair, liveOnly, nil, migrator)
		}

		return fmt.Errorf("failed to create client-level change stream: %w", err)
	}
	defer changeStream.Close(ctx)

	// Create event distributor for parallel processing
	r.log.Infof("Starting parallel change stream processing with %d workers", r.config.IncrementalWorkerCount)
	distributor := NewEventDistributor(
		ctx,
		r.sourceDB,
		r.targetDB,
		r.collectionConfigs,
		changeStream,
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
		r.statsManager,
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
