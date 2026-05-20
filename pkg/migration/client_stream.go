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
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ClientLevelReplicator handles replication using a client-level change stream
type ClientLevelReplicator struct {
	sourceDB      *db.MongoDB
	targetDB      *db.MongoDB
	config        *config.Config
	log           *logger.Logger
	collectionMap map[string]map[string]string // Map of database -> source collection -> target collection
	mu            sync.Mutex                   // Mutex for thread-safe operations
	dlq           DLQ                          // Dead Letter Queue for failed documents
}

// NewClientLevelReplicator creates a new client-level replicator
func NewClientLevelReplicator(sourceDB, targetDB *db.MongoDB, cfg *config.Config, log *logger.Logger) *ClientLevelReplicator {
	return &ClientLevelReplicator{
		sourceDB:      sourceDB,
		targetDB:      targetDB,
		config:        cfg,
		log:           log,
		collectionMap: make(map[string]map[string]string),
	}
}

// SetDLQ sets the Dead Letter Queue writer for this replicator
func (r *ClientLevelReplicator) SetDLQ(dlq DLQ) {
	r.dlq = dlq
}

// AddCollection adds a collection to be watched
func (r *ClientLevelReplicator) AddCollection(sourceDB, targetDB, sourceCollection, targetCollection string) {
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

// StartReplication starts the client-level replication
func (r *ClientLevelReplicator) StartReplication(ctx context.Context, globalResumeToken interface{}, globalResumeTokenPath string, initialMigrationState *InitialMigrationState, initialMigrationStatePath string, pair config.DatabasePair, liveOnly bool, migrator *Migrator) error {
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
	if initialMigrationState == nil {
		if globalResumeToken != nil {
			return fmt.Errorf("safety violation: initial migration state file does not exist, but a global resume token checkpoint exists. Clean up checkpoint file or ensure state is in sync before proceeding")
		}
	} else if initialMigrationState.Status == StatusCompleted || initialMigrationState.Status == StatusSkipped {
		if globalResumeToken == nil {
			return fmt.Errorf("safety violation: initial migration state is marked as %s, but no global resume token checkpoint was found. Clean up state file or restore checkpoint before proceeding", initialMigrationState.Status)
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

	// If no resume token is available, we need to create one
	if globalResumeToken == nil {
		if liveOnly {
			r.log.Info("No global resume token found in live-only mode. Obtaining current resume token to start incremental replication.")
		} else {
			r.log.Info("No global resume token found. Creating a new one and will perform initial migration.")
		}

		// Create a change stream to get an initial resume token
		initialChangeStream, err := r.sourceDB.CreateClientLevelChangeStream(ctx, nil, 0)
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
	} else {
		r.log.Info("Global resume token available. Starting incremental replication.")
	}

	// Perform initial migration if needed
	if needsInitialMigration {
		// Mark initial migration state as incomplete before starting
		if err := SaveInitialMigrationState(initialMigrationStatePath, StatusInProgress, 0); err != nil {
			r.log.Errorf("Error saving initial migration state as incomplete: %v", err)
		}

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

			// Index-Only mode: wait for all async index builds then return without migrating data
			if pair.Target.IndexOnly {
				r.log.Info("IndexOnly mode enabled. Waiting for all async index creation to complete...")
				r.targetDB.WaitForIndexCreation()
				r.log.Info("IndexOnly mode: all indexes synced successfully. Skipping data migration and change stream.")
				return nil
			}
		}

		// Use a semaphore to limit the number of concurrent collection migrations
		// Use ConcurrentCollections for collection-level concurrency (separate from per-collection worker count)
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

		// Pre-compute total collection count before launching goroutines
		// so the progress log always shows the correct total
		totalCollections := 0
		for _, colls := range r.collectionMap {
			totalCollections += len(colls)
		}

		// Iterate through all collections in the map
		for sourceDB, collections := range r.collectionMap {
			for sourceCollection, targetCollection := range collections {
				wg.Add(1)

				// Acquire semaphore
				semaphore <- struct{}{}

				// Start migration in a goroutine
				go func(sourceDB, sourceCollection, targetCollection string) {
					defer wg.Done()
					defer func() { <-semaphore }() // Release semaphore when done

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

					// Set up batch processing using configuration parameters
					readBatchSize := r.config.InitialReadBatchSize
					writeBatchSize := r.config.InitialWriteBatchSize

					r.log.Infof("Using read batch size: %d, write batch size: %d for %s.%s",
						readBatchSize, writeBatchSize, sourceDB, sourceCollection)

					cursor, err := sourceDBCollection.Find(ctx, bson.D{}, options.Find().SetBatchSize(int32(readBatchSize)))
					if err != nil {
						r.log.Errorf("Error creating cursor for %s.%s: %v", sourceDB, sourceCollection, err)
						return
					}
					defer cursor.Close(ctx)

					// Set up parallel batch processing
					var workerWg sync.WaitGroup
					channelBufferSize := r.config.InitialChannelBufferSize
					batchChan := make(chan []interface{}, channelBufferSize) // Buffer for batches
					errorChan := make(chan error, 1)                         // Channel for errors
					doneChan := make(chan struct{})                          // Channel to signal completion

					// Track progress metrics:
					// - successCount: documents successfully written to target database
					// - failedCount: documents that failed target writes and were routed to the DLQ
					// - migratedCount: total documents processed from source cursor (successCount + failedCount)
					var successCount int64
					var failedCount int64
					var migratedCount int64
					var lastLoggedPercentage int = -1 // Start at -1 to ensure 0% is logged
					var workerMu sync.Mutex           // Mutex for thread-safe updates to successCount, failedCount, migratedCount, and lastLoggedPercentage

					// Start worker pool for parallel batch processing
					workerCount := r.config.InitialMigrationWorkers
					r.log.Infof("Starting %d workers for parallel document batch processing for %s.%s",
						workerCount, sourceDB, sourceCollection)

					for i := 0; i < workerCount; i++ {
						workerWg.Add(1)
						go func(workerID int) {
							defer workerWg.Done()

							for batch := range batchChan {
								// Transform __*__ field names to _*_ for Firestore compatibility
								transformedBatch, err := TransformBatch(batch, r.log, sourceDB, sourceCollection)
								if err != nil {
									r.log.Errorf("Field name transformation failed for batch in %s.%s: %v", sourceDB, sourceCollection, err)
									for _, doc := range batch {
										docID := extractDocID(doc)
										if r.dlq != nil {
											r.dlq.WriteFailed(sourceDB, sourceCollection, docID, err, "initial", "insert", doc)
										}
									}
									// Phase 1 (Pre-Database failure): Update metrics early since the entire batch 
									// is aborted here and will skip the post-database metrics block below.
									workerMu.Lock()
									failedCount += int64(len(batch))
									migratedCount += int64(len(batch))
									workerMu.Unlock()
									continue
								}
								batch = transformedBatch

								var batchFailed int64

								// Process batch
								if _, err := targetDBCollection.InsertMany(ctx, batch, options.InsertMany().SetOrdered(false)); err != nil {
									// Handle bulk write errors
									bulkWriteException, ok := err.(mongo.BulkWriteException)
									if ok {
										r.log.Debugf("Bulk insert partially failed for %s.%s: %d failed",
											sourceDB, sourceCollection, len(bulkWriteException.WriteErrors))

										// Process individual errors
										for _, writeErr := range bulkWriteException.WriteErrors {
											// Extract document ID for logging
											var errDocID interface{}
											if writeErr.Index < len(batch) {
												switch d := batch[writeErr.Index].(type) {
												case bson.D:
													for _, elem := range d {
														if elem.Key == "_id" {
															errDocID = elem.Value
															break
														}
													}
												case bson.M:
													errDocID = d["_id"]
												}
											}

											r.log.Debugf("[%s.%s] Insert error at index %d, _id=%v: %v", sourceDB, sourceCollection, writeErr.Index, errDocID, writeErr.Message)

											// Check if it's a duplicate key error (code 11000)
											if writeErr.Code == 11000 && writeErr.Index < len(batch) {
												// Skip duplicate key errors as they likely mean the document already exists
												r.log.Debugf("[%s.%s] Skipping duplicate document _id=%v at index %d", sourceDB, sourceCollection, errDocID, writeErr.Index)
											} else if writeErr.Index < len(batch) {
												// For non-duplicate key errors, retry with upsert
												doc := batch[writeErr.Index]
												var id interface{}

												// Extract ID from document
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
													filter := bson.M{"_id": id}
													if _, err := targetDBCollection.ReplaceOne(ctx, filter, doc, options.Replace().SetUpsert(true)); err != nil {
														r.log.Errorf("[%s.%s] Retry upsert failed for document _id=%v: %v", sourceDB, sourceCollection, id, err)
														// Increment batch failures because even individual fallback retry upsert failed
														batchFailed++
														if r.dlq != nil {
															r.dlq.WriteFailed(sourceDB, sourceCollection, id, err, "initial", "insert", doc)
														}
													}
												} else {
													// Increment batch failures because we cannot retry upsert without a valid _id
													batchFailed++
												}
											}
										}
									} else {
										// Handle non-bulk write errors (e.g. network partition or context timeout)
										// Mark the entire batch as failed since the call was aborted globally without itemized status
										batchFailed = int64(len(batch))
										select {
										case errorChan <- fmt.Errorf("worker %d failed to process batch: %w", workerID, err):
										default:
											// Error channel already has an error
											r.log.Errorf("Worker %d: Error processing batch: %v", workerID, err)
										}
									}
								}

								// Phase 2 (Post-Database write execution): Distribute metrics dynamically 
								// based on successful writes and database retry failures within this batch.
								workerMu.Lock()
								successCount += int64(len(batch)) - batchFailed
								failedCount += batchFailed
								migratedCount += int64(len(batch))
								currentCount := migratedCount // Copy for logging outside the lock
								currentSuccess := successCount
								currentFailed := failedCount

								// Calculate current percentage (0-10 for 0%-100%)
								currentPercentage := int(float64(currentCount) / float64(count) * 10)

								// Only log when crossing a 10% threshold at the collection level
								if currentPercentage > lastLoggedPercentage {
									r.log.Infof("Collection %s.%s progress: %d/%d documents (%.0f%%) - Successful: %d, Failed: %d",
										sourceDB, sourceCollection, currentCount, count, float64(currentPercentage)*10, currentSuccess, currentFailed)
									lastLoggedPercentage = currentPercentage
								}
								workerMu.Unlock()
							}
						}(i)
					}

					// Start a goroutine to close channels when all batches are processed
					go func() {
						workerWg.Wait()
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
							r.log.Errorf("Error during migration of %s.%s: %v", sourceDB, sourceCollection, err)
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
							r.log.Errorf("Error decoding document from %s.%s: %v", sourceDB, sourceCollection, err)
							continue
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
								r.log.Errorf("Error during migration of %s.%s: %v", sourceDB, sourceCollection, err)
								return
							case <-ctx.Done():
								// Context cancelled
								cursor.Close(ctx)
								close(batchChan)
								r.log.Info("Migration interrupted due to context cancellation")
								return
							}

							// Reset batch
							batch = nil
							batchCount = 0
						}
					}

					// Check for cursor errors
					if err := cursor.Err(); err != nil {
						close(batchChan)
						r.log.Errorf("Cursor error for %s.%s: %v", sourceDB, sourceCollection, err)
						return
					}

					// Process any remaining documents
					if len(batch) > 0 {
						select {
						case batchChan <- batch:
							// Final batch sent to worker
						case err := <-errorChan:
							// Error from a worker
							close(batchChan)
							r.log.Errorf("Error during migration of %s.%s: %v", sourceDB, sourceCollection, err)
							return
						case <-ctx.Done():
							// Context cancelled
							close(batchChan)
							r.log.Info("Migration interrupted due to context cancellation")
							return
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
						r.log.Errorf("Error during migration of %s.%s: %v", sourceDB, sourceCollection, err)
						return
					case <-ctx.Done():
						// Context cancelled
						r.log.Info("Migration interrupted due to context cancellation")
						return
					}

					if failedCount > 0 {
						r.log.Warnf("Migration for %s.%s completed with %d failures! Successful: %d, Failed: %d, Total: %d",
							sourceDB, sourceCollection, failedCount, successCount, failedCount, migratedCount)
					} else {
						r.log.Infof("Migration for %s.%s completed successfully! Total documents: %d",
							sourceDB, sourceCollection, migratedCount)
					}

					// Update overall statistics and log overall progress
					mu.Lock()
					totalMigratedCount += migratedCount
					totalFailedCount += failedCount
					completedCollections++
					r.log.Infof("Overall progress: %d/%d collections completed", completedCollections, totalCollections)
					mu.Unlock()
				}(sourceDB, sourceCollection, targetCollection)
			}
		}

		// Wait for all collection migrations to complete
		wg.Wait()

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
			return r.StartReplication(ctx, nil, globalResumeTokenPath, nil, initialMigrationStatePath, pair, liveOnly, migrator)
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
		r.collectionMap,
		changeStream,
		r.log,
		globalResumeTokenPath,
		time.Duration(r.config.CheckpointInterval)*time.Minute,
		r.config.SaveThreshold,
		r.config.IncrementalWorkerCount,
		r.config.IncrementalWriteBatchSize,
		r.config.ForceOrderedOperations,
		time.Duration(r.config.FlushIntervalMs)*time.Millisecond,
		r.config,
		r.dlq,
	)

	// Start event distribution
	err = distributor.Start()
	// Don't propagate context.Canceled as an error
	if err == context.Canceled {
		r.log.Info("Replication stopped due to context cancellation")
		return nil
	}
	return err
}
