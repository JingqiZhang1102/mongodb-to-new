package db

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gsbingo17/mongodb-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoDB represents a MongoDB connection
type MongoDB struct {
	client         *mongo.Client
	database       *mongo.Database
	log            *logger.Logger
	indexSemaphore chan struct{}  // limits concurrent async index builds to prevent Firestore cross-transaction contention
	indexWg        sync.WaitGroup // tracks in-flight async index creation goroutines
}

// NewMongoDB creates a new MongoDB connection with pool size and idle timeouts configured dynamically
func NewMongoDB(connectionString, databaseName string, minPoolSize, maxPoolSize uint64, maxConnIdleTime time.Duration, poolMonitor *event.PoolMonitor, log *logger.Logger) (*MongoDB, error) {
	// Set client options
	clientOptions := options.Client().
		ApplyURI(connectionString).
		SetMaxPoolSize(maxPoolSize).
		SetMinPoolSize(minPoolSize).
		SetConnectTimeout(30 * time.Second).
		SetSocketTimeout(120 * time.Second)

	if maxConnIdleTime > 0 {
		clientOptions.SetMaxConnIdleTime(maxConnIdleTime)
	}

	if poolMonitor != nil {
		clientOptions.SetPoolMonitor(poolMonitor)
	}

	// Connect to MongoDB
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Ping the database to verify connection
	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	// Get database
	database := client.Database(databaseName)

	return &MongoDB{
		client:         client,
		database:       database,
		log:            log,
		indexSemaphore: make(chan struct{}, 1), // serialize async index builds to prevent Firestore cross-transaction contention
	}, nil
}

// WaitForIndexCreation blocks until all async index creation goroutines have finished.
// Used by index-only mode to ensure the process doesn't exit before indexes are created.
func (m *MongoDB) WaitForIndexCreation() {
	m.indexWg.Wait()
}

// Close closes the MongoDB connection
func (m *MongoDB) Close(ctx context.Context) error {
	return m.client.Disconnect(ctx)
}

// GetCollection returns a MongoDB collection
func (m *MongoDB) GetCollection(collectionName string) *mongo.Collection {
	return m.database.Collection(collectionName)
}

// ListCollections returns a list of all collection names in the database
func (m *MongoDB) ListCollections(ctx context.Context) ([]string, error) {
	collections, err := m.database.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("failed to list collections: %w", err)
	}
	return collections, nil
}

// GetDatabaseName returns the database name
func (m *MongoDB) GetDatabaseName() string {
	return m.database.Name()
}

// GetClient returns the MongoDB client
func (m *MongoDB) GetClient() *mongo.Client {
	return m.client
}

// CreateChangeStream creates a change stream for a collection
func (m *MongoDB) CreateChangeStream(ctx context.Context, collectionName string, resumeToken interface{}) (*mongo.ChangeStream, error) {
	collection := m.GetCollection(collectionName)

	// Set pipeline for full document lookup on updates
	pipeline := mongo.Pipeline{}

	// Set options
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	if resumeToken != nil {
		opts.SetResumeAfter(resumeToken)
	}

	// Create change stream
	changeStream, err := collection.Watch(ctx, pipeline, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create change stream for collection %s: %w", collectionName, err)
	}

	return changeStream, nil
}

// CreateClientLevelChangeStream creates a change stream at the client level
// This watches for changes across all collections in all databases.
// It accepts both a resumeToken and a cdcStartTime:
// - If resumeToken is provided, it takes precedence and instructs the driver to resume from a specific checkpoint.
// - If resumeToken is nil but cdcStartTime is specified, it configures SetStartAtOperationTime to begin reading changes from that exact historical moment.
func (m *MongoDB) CreateClientLevelChangeStream(ctx context.Context, resumeToken interface{}, cdcStartTime *primitive.Timestamp, batchSize int, pipeline mongo.Pipeline) (*mongo.ChangeStream, error) {
	if pipeline == nil {
		pipeline = mongo.Pipeline{}
	}

	// Set options
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	if resumeToken != nil {
		// Resume replication from a previously saved checkpoint token
		opts.SetResumeAfter(resumeToken)
	} else if cdcStartTime != nil {
		// If no resume token exists but the user provided a historical starting point,
		// set the stream's start point to that specific cluster operation time.
		opts.SetStartAtOperationTime(cdcStartTime)
		m.log.Infof("Starting change stream at operation time: %s", time.Unix(int64(cdcStartTime.T), 0).UTC().Format(time.RFC3339))
	}

	// Set batch size if provided
	if batchSize > 0 {
		opts.SetBatchSize(int32(batchSize))
		m.log.Infof("Setting change stream batch size to %d", batchSize)
	}

	// Create client-level change stream
	changeStream, err := m.client.Watch(ctx, pipeline, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create client-level change stream: %w", err)
	}

	m.log.Info("Created client-level change stream watching all databases and collections")
	return changeStream, nil
}

// ListIndexes returns all indexes for a collection
func (m *MongoDB) ListIndexes(ctx context.Context, collectionName string) ([]bson.M, error) {
	collection := m.GetCollection(collectionName)
	cursor, err := collection.Indexes().List(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list indexes for collection %s: %w", collectionName, err)
	}
	defer cursor.Close(ctx)

	var indexes []bson.M
	if err = cursor.All(ctx, &indexes); err != nil {
		return nil, fmt.Errorf("failed to decode indexes: %w", err)
	}

	return indexes, nil
}

// CreateIndexFromDefinition creates an index on a collection using an index definition
func (m *MongoDB) CreateIndexFromDefinition(ctx context.Context, collectionName string, indexDef bson.M) error {
	collection := m.GetCollection(collectionName)

	// Extract index name
	indexName, ok := indexDef["name"].(string)
	if !ok {
		return fmt.Errorf("index definition missing 'name' field")
	}

	// Extract index keys and ensure they're in ordered format
	keysRaw, ok := indexDef["key"]
	if !ok {
		return fmt.Errorf("index definition missing 'key' field")
	}

	// Convert keys to bson.D (ordered) format to preserve field order
	// Note: bson.D is an alias for primitive.D, and bson.M is an alias for primitive.M
	var keys bson.D
	switch k := keysRaw.(type) {
	case bson.D:
		// bson.D and primitive.D are the same type
		keys = k
	case bson.M:
		// bson.M and primitive.M are the same type
		// Convert bson.M to bson.D
		// Note: Map iteration order is preserved in Go 1.12+ for unmodified maps
		for key, value := range k {
			keys = append(keys, bson.E{Key: key, Value: value})
		}
	default:
		return fmt.Errorf("unexpected type for index keys: %T", keysRaw)
	}

	// Build index model
	indexModel := mongo.IndexModel{
		Keys: keys,
	}

	// Build index options
	opts := options.Index().SetName(indexName)

	// Add unique constraint if present
	if unique, ok := indexDef["unique"].(bool); ok && unique {
		opts.SetUnique(true)
	}

	// Add sparse option if present
	if sparse, ok := indexDef["sparse"].(bool); ok && sparse {
		opts.SetSparse(true)
	}

	// Add TTL (expireAfterSeconds) if present
	if expireAfter, ok := indexDef["expireAfterSeconds"].(int32); ok {
		opts.SetExpireAfterSeconds(expireAfter)
	}

	// Add partial filter expression if present
	if partialFilter, ok := indexDef["partialFilterExpression"]; ok {
		opts.SetPartialFilterExpression(partialFilter)
	}

	// Add text index options if present
	if defaultLanguage, ok := indexDef["default_language"].(string); ok {
		opts.SetDefaultLanguage(defaultLanguage)
	}
	if languageOverride, ok := indexDef["language_override"].(string); ok {
		opts.SetLanguageOverride(languageOverride)
	}

	// Add text index weights if present
	if weights, ok := indexDef["weights"]; ok {
		opts.SetWeights(weights)
	}

	// Always build indexes in the background to avoid blocking and timeouts.
	// On MongoDB < 4.2: the server returns immediately, index builds asynchronously.
	// On MongoDB 4.2+: this option is deprecated and ignored (all builds are hybrid/non-blocking).
	opts.SetBackground(true)

	indexModel.Options = opts

	// Create the index
	_, err := collection.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return fmt.Errorf("failed to create index '%s' on collection %s: %w", indexName, collectionName, err)
	}

	return nil
}

// isContentionError checks if an error is a Firestore cross-transaction contention error.
func isContentionError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "cross-transaction contention") ||
		strings.Contains(errStr, "Aborted") && strings.Contains(errStr, "contention") ||
		strings.Contains(errStr, "too much contention")
}

// CreateIndexFromDefinitionAsync creates an index asynchronously in a goroutine.
// It uses a dedicated MongoDB client with no socket timeout so index builds can run
// as long as needed without being killed by the main client's 120-second socket timeout.
//
// Throttling: A semaphore (capacity=1) serializes index builds to prevent Firestore
// cross-transaction contention. Goroutines queue up and execute one at a time.
//
// Retry: If a contention error still occurs (e.g., from an external process), the
// operation retries up to 3 times with 30s/60s/120s delays.
//
// The goroutine logs success or failure — the caller does not wait.
func (m *MongoDB) CreateIndexFromDefinitionAsync(connectionString, collectionName string, indexDef bson.M) {
	// Extract index name for logging
	indexName, _ := indexDef["name"].(string)

	// Track this goroutine so callers can wait for all index builds to finish
	m.indexWg.Add(1)

	go func() {
		defer m.indexWg.Done()
		// Acquire semaphore — blocks until a slot is available.
		// This serializes index creation to avoid Firestore cross-transaction contention.
		if m.indexSemaphore != nil {
			m.log.Debugf("[async] Index '%s' on '%s': waiting for semaphore...", indexName, collectionName)
			m.indexSemaphore <- struct{}{}
			defer func() { <-m.indexSemaphore }()
		}

		startTime := time.Now()

		// Create a dedicated client with no socket timeout for long-running index builds
		clientOptions := options.Client().
			ApplyURI(connectionString).
			SetMaxPoolSize(4).
			SetMinPoolSize(1).
			SetConnectTimeout(30 * time.Second)
		// Intentionally NO SetSocketTimeout — index builds can take hours

		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Hour)
		defer cancel()

		client, err := mongo.Connect(ctx, clientOptions)
		if err != nil {
			m.log.Errorf("[async] Failed to connect for index '%s' on '%s': %v", indexName, collectionName, err)
			return
		}
		defer client.Disconnect(ctx)

		// Create the index using the dedicated client, with retry for contention errors
		collection := client.Database(m.database.Name()).Collection(collectionName)

		const maxRetries = 3
		retryDelays := []time.Duration{30 * time.Second, 60 * time.Second, 120 * time.Second}

		var lastErr error
		for attempt := 0; attempt <= maxRetries; attempt++ {
			if attempt > 0 {
				delay := retryDelays[attempt-1]
				m.log.Infof("[async] Retrying index '%s' on '%s' (attempt %d/%d) after %v...",
					indexName, collectionName, attempt, maxRetries, delay)
				select {
				case <-time.After(delay):
				case <-ctx.Done():
					m.log.Warnf("[async] Context canceled while waiting to retry index '%s' on '%s'", indexName, collectionName)
					return
				}
			}

			lastErr = m.createIndexOnCollection(ctx, collection, collectionName, indexDef)
			if lastErr == nil {
				m.log.Infof("[async] Successfully created index '%s' on '%s' (took %v)",
					indexName, collectionName, time.Since(startTime).Round(time.Second))

				// Small cooldown after success to reduce pressure on Firestore metadata
				time.Sleep(2 * time.Second)
				return
			}

			// Only retry on contention errors
			if !isContentionError(lastErr) {
				break
			}

			m.log.Warnf("[async] Contention error creating index '%s' on '%s': %v (attempt %d/%d, took %v so far)",
				indexName, collectionName, lastErr, attempt+1, maxRetries+1, time.Since(startTime).Round(time.Second))
		}

		// All attempts exhausted or non-contention error
		m.log.Warnf("[async] Failed to create index '%s' on '%s': %v (took %v)",
			indexName, collectionName, lastErr, time.Since(startTime).Round(time.Second))
	}()
}

// createIndexOnCollection is a helper that creates an index on a given collection.
// It contains the shared index model building logic used by both sync and async paths.
func (m *MongoDB) createIndexOnCollection(ctx context.Context, collection *mongo.Collection, collectionName string, indexDef bson.M) error {
	indexName, ok := indexDef["name"].(string)
	if !ok {
		return fmt.Errorf("index definition missing 'name' field")
	}

	keysRaw, ok := indexDef["key"]
	if !ok {
		return fmt.Errorf("index definition missing 'key' field")
	}

	var keys bson.D
	switch k := keysRaw.(type) {
	case bson.D:
		keys = k
	case bson.M:
		for key, value := range k {
			keys = append(keys, bson.E{Key: key, Value: value})
		}
	default:
		return fmt.Errorf("unexpected type for index keys: %T", keysRaw)
	}

	indexModel := mongo.IndexModel{Keys: keys}
	opts := options.Index().SetName(indexName)

	if unique, ok := indexDef["unique"].(bool); ok && unique {
		opts.SetUnique(true)
	}
	if sparse, ok := indexDef["sparse"].(bool); ok && sparse {
		opts.SetSparse(true)
	}
	if expireAfter, ok := indexDef["expireAfterSeconds"].(int32); ok {
		opts.SetExpireAfterSeconds(expireAfter)
	}
	if partialFilter, ok := indexDef["partialFilterExpression"]; ok {
		opts.SetPartialFilterExpression(partialFilter)
	}
	if defaultLanguage, ok := indexDef["default_language"].(string); ok {
		opts.SetDefaultLanguage(defaultLanguage)
	}
	if languageOverride, ok := indexDef["language_override"].(string); ok {
		opts.SetLanguageOverride(languageOverride)
	}
	if weights, ok := indexDef["weights"]; ok {
		opts.SetWeights(weights)
	}
	opts.SetBackground(true)

	indexModel.Options = opts

	m.log.Infof("[async] Sending index creation request for '%s' on collection '%s' to Firestore...", indexName, collectionName)
	startTime := time.Now()
	_, err := collection.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		m.log.Infof("[async] Received ERROR response for index '%s' on collection '%s' after %v: %v", 
			indexName, collectionName, time.Since(startTime).Round(time.Second), err)
		return fmt.Errorf("failed to create index '%s' on collection %s: %w", indexName, collectionName, err)
	}
	m.log.Infof("[async] Received SUCCESS response for index '%s' on collection '%s' after %v", 
		indexName, collectionName, time.Since(startTime).Round(time.Second))
	return nil
}
