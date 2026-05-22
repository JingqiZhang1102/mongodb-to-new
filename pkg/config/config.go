package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
)

// Config represents the main configuration structure
type Config struct {
	DatabasePairs          []DatabasePair `json:"databasePairs"`
	SaveThreshold          int            `json:"saveThreshold"`          // Number of processed events before saving a resume token checkpoint
	CheckpointIntervalMinutes int            `json:"checkpointIntervalMinutes"` // Checkpoint interval in minutes
	ForceOrderedOperations bool           `json:"forceOrderedOperations"` // Force ordered operations for all types
	FlushIntervalMs        int            `json:"flushIntervalMs"`        // Flush interval in milliseconds
	TargetMaxConnIdleSeconds int            `json:"targetMaxConnIdleSeconds"` // Maximum connection idle time for target in seconds

	// Parameters for initial migration
	InitialReadBatchSize     int `json:"initialReadBatchSize"`     // Number of documents to read in a batch during initial migration
	InitialWriteBatchSize    int `json:"initialWriteBatchSize"`    // Number of documents to write in a batch during initial migration
	InitialChannelBufferSize int `json:"initialChannelBufferSize"` // Size of channel buffer for batches during initial migration
	InitialMigrationWorkers  int `json:"initialMigrationWorkers"`  // Number of worker goroutines for batch processing
	ConcurrentCollections    int `json:"concurrentCollections"`    // Number of collections to process concurrently

	// Parameters for incremental replication
	IncrementalReadBatchSize  int `json:"incrementalReadBatchSize"`  // Number of change events to read at once
	IncrementalWriteBatchSize int `json:"incrementalWriteBatchSize"` // Maximum size of operation groups
	IncrementalWorkerCount    int `json:"incrementalWorkerCount"`    // Number of worker goroutines
	StatsIntervalMinutes      int `json:"statsIntervalMinutes"`      // Interval for reporting change stream statistics in minutes

	// Parallel read configuration for large collections
	ParallelReadsEnabled    bool `json:"parallelReadsEnabled"`    // Enable parallel reads for large collections
	MaxReadPartitions       int  `json:"maxReadPartitions"`       // Maximum number of partitions for parallel reads
	MinDocsPerPartition     int  `json:"minDocsPerPartition"`     // Minimum number of documents per partition
	MinDocsForParallelReads int  `json:"minDocsForParallelReads"` // Minimum collection size for parallel reads
	SampleSize              int  `json:"sampleSize"`              // Number of documents to sample for partitioning
	WorkersPerPartition     int  `json:"workersPerPartition"`     // Number of worker goroutines per partition

	// Retry configuration
	RetryConfig RetryConfig `json:"retryConfig"` // Configuration for retry mechanisms
}

// RetryConfig represents retry configuration
type RetryConfig struct {
	MaxRetries           int  `json:"maxRetries"`           // Maximum number of retries
	BaseDelayMs          int  `json:"baseDelayMs"`          // Base delay in milliseconds
	MaxDelayMs           int  `json:"maxDelayMs"`           // Maximum delay in milliseconds
	EnableBatchSplitting bool `json:"enableBatchSplitting"` // Enable batch splitting for contention errors
	MinBatchSize         int  `json:"minBatchSize"`         // Minimum batch size for splitting
	ConvertInvalidIds    bool `json:"convertInvalidIds"`    // Convert invalid _id types to string
}

// DatabasePair represents a source and target database pair
type DatabasePair struct {
	Source SourceConfig `json:"source"`
	Target TargetConfig `json:"target"`
}

// SourceConfig represents the source MongoDB configuration
type SourceConfig struct {
	ConnectionString  string `json:"connectionString"`
	Database          string `json:"database"`
	ReplicationMethod string `json:"replicationMethod,omitempty"` // "changestream" (default) or "oplog"
}

// TargetConfig represents the target MongoDB configuration
type TargetConfig struct {
	ConnectionString string             `json:"connectionString"`
	Database         string             `json:"database"`
	Collections      []CollectionConfig `json:"collections,omitempty"`
	SyncAllIndexes   bool               `json:"syncAllIndexes,omitempty"` // Sync all indexes (excluding _id_) from source
	IndexOnly        bool               `json:"indexOnly,omitempty"`      // Only sync indexes, skip data migration
	Indexes          []IndexSyncConfig  `json:"indexes,omitempty"`
}

// CollectionConfig represents a collection mapping
type CollectionConfig struct {
	SourceCollection string `json:"sourceCollection"`
	TargetCollection string `json:"targetCollection"`
	UpsertMode       bool   `json:"upsertMode,omitempty"` // Use upsert by default instead of insert
}

// IndexSyncConfig represents index sync configuration for a collection
type IndexSyncConfig struct {
	SourceCollection string   `json:"sourceCollection"`
	IndexNames       []string `json:"indexNames"`
}

// LoadConfig loads the configuration from a file
func LoadConfig(configPath string) (*Config, error) {
	// Set default config path if not provided
	if configPath == "" {
		configPath = "mongodb_replication_config.json"
	}

	// Read the config file
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("error reading config file: %w", err)
	}

	// Parse the config strictly (fail on unrecognized fields)
	var config Config
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("error parsing config file (unrecognized configuration fields present): %w", err)
	}

	// Validate the config
	if err := validateConfig(&config); err != nil {
		return nil, err
	}

	// Set default save threshold if not provided
	if config.SaveThreshold <= 0 {
		config.SaveThreshold = 100
	}

	// Set default checkpoint interval in minutes if not provided
	if config.CheckpointIntervalMinutes <= 0 {
		config.CheckpointIntervalMinutes = 5 // Default to 5 minutes
	}

	// Set default values for incremental replication parameters
	if config.IncrementalReadBatchSize <= 0 {
		config.IncrementalReadBatchSize = 8192 // Default to 8192 change events
	}

	if config.IncrementalWriteBatchSize <= 0 {
		config.IncrementalWriteBatchSize = 128 // Default to 128 operations per group
	}

	if config.IncrementalWorkerCount <= 0 {
		config.IncrementalWorkerCount = runtime.NumCPU() // Default to number of CPU cores
	}

	if config.StatsIntervalMinutes <= 0 {
		config.StatsIntervalMinutes = 5 // Default to 5 minutes
	}

	// Set default flush interval if not provided
	if config.FlushIntervalMs <= 0 {
		config.FlushIntervalMs = 500 // Default to 500 milliseconds
	}

	// Set default target connection idle time in seconds if not provided
	if config.TargetMaxConnIdleSeconds <= 0 {
		config.TargetMaxConnIdleSeconds = 30 // Default to 30 seconds
	}

	// Set default values for initial migration parameters
	if config.InitialReadBatchSize <= 0 {
		config.InitialReadBatchSize = 8192 // Default to 8192 documents
	}

	if config.InitialWriteBatchSize <= 0 {
		config.InitialWriteBatchSize = 128 // Default to 128 documents
	}

	if config.InitialChannelBufferSize <= 0 {
		config.InitialChannelBufferSize = 10 // Default to buffer for 10 batches
	}

	if config.InitialMigrationWorkers <= 0 {
		config.InitialMigrationWorkers = 5 // Default to 5 worker goroutines
	}

	if config.ConcurrentCollections <= 0 {
		config.ConcurrentCollections = 4 // Default to 4 concurrent collections
	}

	// Already set default values for incremental replication parameters above

	// Set default values for parallel reads
	if config.MaxReadPartitions <= 0 {
		config.MaxReadPartitions = 8 // Default to 8 partitions
	}

	if config.MinDocsPerPartition <= 0 {
		config.MinDocsPerPartition = 10000 // Default to 10,000 docs per partition
	}

	if config.MinDocsForParallelReads <= 0 {
		config.MinDocsForParallelReads = 50000 // Default to 50,000 docs for parallel reads
	}

	if config.SampleSize <= 0 {
		config.SampleSize = 1000 // Default to 1,000 samples
	}

	if config.WorkersPerPartition <= 0 {
		config.WorkersPerPartition = 3 // Default to 3 workers per partition
	}

	// Set default values for retry configuration
	if config.RetryConfig.MaxRetries <= 0 {
		config.RetryConfig.MaxRetries = 5 // Default to 5 retries
	}

	if config.RetryConfig.BaseDelayMs <= 0 {
		config.RetryConfig.BaseDelayMs = 100 // Default to 100ms base delay
	}

	if config.RetryConfig.MaxDelayMs <= 0 {
		config.RetryConfig.MaxDelayMs = 5000 // Default to 5s max delay
	}

	if config.RetryConfig.MinBatchSize <= 0 {
		config.RetryConfig.MinBatchSize = 10 // Default to 10 docs per batch
	}

	// Set default value for ConvertInvalidIds
	// Default to true to automatically convert invalid _id types
	if !config.RetryConfig.ConvertInvalidIds {
		config.RetryConfig.ConvertInvalidIds = true
	}

	// No backward compatibility needed anymore

	return &config, nil
}

// validateConfig validates the configuration
func validateConfig(config *Config) error {
	if len(config.DatabasePairs) == 0 {
		return fmt.Errorf("no database pairs specified in config")
	}

	for i, pair := range config.DatabasePairs {
		// Validate source config
		if pair.Source.ConnectionString == "" {
			return fmt.Errorf("source connection string is required for database pair %d", i)
		}
		if pair.Source.Database == "" {
			return fmt.Errorf("source database name is required for database pair %d", i)
		}

		// Validate target config
		if pair.Target.ConnectionString == "" {
			return fmt.Errorf("target connection string is required for database pair %d", i)
		}
		if pair.Target.Database == "" {
			return fmt.Errorf("target database name is required for database pair %d", i)
		}

		// Validate collection configs if provided
		for j, coll := range pair.Target.Collections {
			if coll.SourceCollection == "" {
				return fmt.Errorf("source collection name is required for collection mapping at index %d in database pair %d", j, i)
			}
			if coll.TargetCollection == "" {
				return fmt.Errorf("target collection name is required for collection mapping at index %d in database pair %d", j, i)
			}
		}
	}

	return nil
}

// GetMaxWorkersForLive returns the maximum concurrent workers based on the operation mode.
// It only considers initial migration workers for 'migrate' and 'live' modes,
// and uses strictly the incremental worker count for 'live-only' mode.
func (c *Config) GetMaxWorkersForLive(mode string) int {
	maxWorkers := c.IncrementalWorkerCount
	if mode != "live-only" {
		maxWorkers = max(maxWorkers, c.ConcurrentCollections*c.InitialMigrationWorkers)
	}
	return maxWorkers
}
