package db

import (
	"context"
	"fmt"
	"time"

	"github.com/gsbingo17/mongodb-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoDB represents a MongoDB connection
type MongoDB struct {
	client   *mongo.Client
	database *mongo.Database
	log      *logger.Logger
}

// NewMongoDB creates a new MongoDB connection
func NewMongoDB(connectionString, databaseName string, log *logger.Logger) (*MongoDB, error) {
	// Set client options
	clientOptions := options.Client().
		ApplyURI(connectionString).
		SetMaxPoolSize(256).
		SetMinPoolSize(128).
		SetConnectTimeout(30 * time.Second).
		SetSocketTimeout(120 * time.Second)

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
		client:   client,
		database: database,
		log:      log,
	}, nil
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
// This watches for changes across all collections in all databases
func (m *MongoDB) CreateClientLevelChangeStream(ctx context.Context, resumeToken interface{}, batchSize int) (*mongo.ChangeStream, error) {
	// Set pipeline for full document lookup on updates
	pipeline := mongo.Pipeline{}

	// Set options
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	if resumeToken != nil {
		opts.SetResumeAfter(resumeToken)
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

// CreateIndexFromDefinitionAsync creates an index asynchronously in a goroutine.
// It uses a dedicated MongoDB client with no socket timeout so index builds can run
// as long as needed without being killed by the main client's 120-second socket timeout.
// The goroutine logs success or failure — the caller does not wait.
func (m *MongoDB) CreateIndexFromDefinitionAsync(connectionString, collectionName string, indexDef bson.M) {
	// Extract index name for logging
	indexName, _ := indexDef["name"].(string)

	go func() {
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

		// Create the index using the dedicated client
		collection := client.Database(m.database.Name()).Collection(collectionName)

		if err := m.createIndexOnCollection(ctx, collection, collectionName, indexDef); err != nil {
			m.log.Warnf("[async] Failed to create index '%s' on '%s': %v (took %v)", indexName, collectionName, err, time.Since(startTime).Round(time.Second))
		} else {
			m.log.Infof("[async] Successfully created index '%s' on '%s' (took %v)", indexName, collectionName, time.Since(startTime).Round(time.Second))
		}
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

	_, err := collection.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return fmt.Errorf("failed to create index '%s' on collection %s: %w", indexName, collectionName, err)
	}
	return nil
}
