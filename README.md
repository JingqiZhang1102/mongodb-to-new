# MongoDB to MongoDB Migration and Replication Tool

This Go application replicates data from one MongoDB database to another MongoDB database, and supports migration from MongoDB to *Firestore with MongoDB compatibility*. It provides two modes of operation:

- **Migrate:** Performs a one-time migration of data from a source MongoDB database to a target MongoDB database.
- **Live:** Sets up a live replication using MongoDB change streams to continuously synchronize data between the two databases. This includes:
    - **Initial Migration:** A one-time migration of existing data from the source MongoDB to the target MongoDB.
    - **Incremental Replication:** Uses MongoDB change streams to continuously synchronize data between the two databases, replicating any new changes made after the initial migration.

## Prerequisites

### General Requirements
- Go 1.21 or later
- MongoDB servers running and accessible (both source and target)

### Replication Method Requirements

The tool supports three replication methods for live mode, each with different prerequisites:

#### 1. Change Stream Replication (Default - `replicationMethod: "changestream"`)
- **Source MongoDB**: Version 3.6 or later
- **Replica Set**: Source MongoDB **must** be running as a replica set
- **Recommended for**: Modern MongoDB deployments (3.6+)
- **Advantages**: 
  - High-level API with server-side filtering
  - Structured change events
  - Official MongoDB feature with long-term support

#### 2. Oplog Replication (`replicationMethod: "oplog"`)
- **Source MongoDB**: Any version with replica set support (2.0+)
- **Replica Set**: Source MongoDB **must** be running as a replica set (oplog only exists on replica sets)
- **Wire Protocol**: Modern wire protocol (version 6+)
- **Recommended for**:
  - MongoDB 3.0, 3.2, 3.4 (wire protocol version 6)
  - Scenarios requiring low-level oplog access
- **Advantages**:
  - Works with older MongoDB versions that don't support change streams
  - Direct access to operation log

#### 3. Legacy Oplog Replication (`replicationMethod: "oplog-legacy"`)
- **Source MongoDB**: MongoDB 3.0, 3.2, 3.4 (wire protocol version 3)
- **Replica Set**: Source MongoDB **must** be running as a replica set
- **Target MongoDB**: Modern MongoDB (3.6+) or MongoDB-compatible databases (e.g., Firestore)
- **Recommended for**: 
  - Migrating from very old MongoDB versions (3.0/3.2) to modern MongoDB
  - Bridging the gap between legacy and modern MongoDB versions
- **Implementation**:
  - Uses dual-driver architecture (mgo for source, mongo-driver for target)
  - Leverages GTM legacy library for oplog tailing
  - Full initial migration + incremental replication support

### Target MongoDB Requirements
- **Migrate mode**: Any MongoDB version
- **Live mode**: No specific version requirements (receives standard insert/update/delete operations)

### Quick Decision Guide

| Your Source MongoDB | Recommended Method | Configuration |
|-------------------|-------------------|---------------|
| MongoDB 3.6 or later | Change Streams | `"replicationMethod": "changestream"` (default) |
| MongoDB 3.0, 3.2, 3.4 (wire v6) | Oplog | `"replicationMethod": "oplog"` |
| MongoDB 3.0, 3.2, 3.4 (wire v3) | Legacy Oplog | `"replicationMethod": "oplog-legacy"` |
| Single-node deployment | Migrate mode only | Not applicable (live mode requires replica set) |

**Note**: All live replication methods require the source MongoDB to be running as a replica set. See "Setting Up a Single-Node Replica Set for Development" section below for local development setup.

## Installation

1. Clone this repository:

   ```bash
   git clone https://github.com/gsbingo17/mongodb-to-new.git
   cd mongodb-to-new
   ```

2. Build the application:

   ```bash
   go mod tidy
   go build -o migrate ./cmd/migrate
   ```

## Configuration

Create a `mongodb_replication_config.json` file: This file defines the replication settings, including the source and target MongoDB connection details. A complete sample configuration file with all available options is provided in `sample_config.json`.

Here's a basic example:

### Full Database Migration (Automatic Collection Detection)

```json
{
  "databasePairs": [
    {
      "source": {
        "connectionString": "mongodb://localhost:27017?replicaSet=rs0",
        "database": "source_db"
      },
      "target": {
        "connectionString": "mongodb://localhost:27017",
        "database": "target_db"
      }
    }
  ],
  "saveThreshold": 1000,
  "checkpointInterval": 5,
  "forceOrderedOperations": false,
  "flushIntervalMs": 500
}
```

When no collections are specified, the tool will automatically detect all collections in the source database and migrate them to the target database with the same collection names.

### Specific Collections Migration

If you want to migrate only specific collections or rename collections during migration, you can specify them explicitly:

```json
{
  "databasePairs": [
    {
      "source": {
        "connectionString": "mongodb://localhost:27017/?replicaSet=rs0",
        "database": "source_db"
      },
      "target": {
        "connectionString": "mongodb://localhost:27017",
        "database": "target_db",
        "collections": [
          {
            "sourceCollection": "source_collection",
            "targetCollection": "target_collection",
            "upsertMode": true
          }
        ]
      }
    }
  ],
  "saveThreshold": 1000
}
```

### Configuration Options

#### Database Configuration
- **databasePairs**: An array of objects, each defining a source MongoDB database and a target MongoDB database to replicate.
- **connectionString**: The MongoDB connection string for source and target databases.
- **database**: The name of the MongoDB database for source and target.
- **collections**: (Optional) An array of objects, each defining a source MongoDB collection and a target MongoDB collection to replicate. If omitted, all collections will be migrated with the same names.
  - **sourceCollection**: The name of the collection in the source database.
  - **targetCollection**: The name of the collection in the target database.
  - **upsertMode**: (Optional) Whether to use upsert operations instead of inserts. Default is false.

#### Checkpoint Configuration
- **saveThreshold**: The number of changes to process before saving the resume token (for live replication).
- **checkpointInterval**: The time interval in minutes to save the resume token regardless of the number of changes (default: 5).

#### Performance Configuration
- **initialReadBatchSize**: Number of documents to read in a batch during initial migration (default: 8192).
- **initialWriteBatchSize**: Number of documents to write in a batch during initial migration (default: 128).
- **initialChannelBufferSize**: Size of channel buffer for batches during initial migration (default: 10).
- **initialMigrationWorkers**: Number of worker goroutines for batch processing during standard migration (default: 5).
- **concurrentCollections**: Number of collections to process concurrently (default: 4).
- **incrementalReadBatchSize**: Number of change events to read at once (default: 8192).
- **incrementalWriteBatchSize**: Maximum size of operation groups (default: 128).
- **incrementalWorkerCount**: Number of worker goroutines for incremental replication (default: number of CPU cores).
- **statsIntervalMinutes**: Interval for reporting change stream statistics in minutes (default: 5).
- **groupOpsByDistinctId**: Enable key-collision grouping in live replication instead of optype-based grouping (default: false).
- **flushIntervalMs**: Flush interval in milliseconds for operation groups (default: 500).
- **targetMinPoolSize**: Minimum MongoDB connection pool size for target database (default: 128).
- **targetMaxPoolSize**: Maximum MongoDB connection pool size for target database (default: 256).
- **incrementalIncomingQueueSize**: Buffer size of the concurrent workers' raw events queue channel (default: 8192).
- **incrementalProcessingQueueSize**: Buffer size of the concurrent workers' writing batches queue channel (default: 4096). Bounding this to a small number (e.g. 2 or 4) applies strict in-memory backpressure, preventing memory backups and capping Queue Latency under slow writes.
- **forceOrderedOperations**: Whether to force ordered operations for all operation types (default: false). When false, insert and delete operations use unordered bulk writes for better performance, while update and replace operations always use ordered bulk writes to ensure consistency.

#### Parallel Reads Configuration
- **parallelReadsEnabled**: Enable parallel reads for large collections (default: true).
- **maxReadPartitions**: Maximum number of partitions for parallel reads (default: 8).
- **minDocsPerPartition**: Minimum number of documents per partition (default: 10000).
- **minDocsForParallelReads**: Minimum collection size for parallel reads (default: 50000).
- **sampleSize**: Number of documents to sample for partitioning (default: 1000).
- **workersPerPartition**: Number of worker goroutines per partition for parallel batch processing (default: 3).

#### Retry Configuration
- **retryConfig**: Configuration for retry mechanisms.
  - **maxRetries**: Maximum number of retries (default: 5).
  - **baseDelayMs**: Base delay in milliseconds (default: 100).
  - **maxDelayMs**: Maximum delay in milliseconds (default: 5000).
  - **enableBatchSplitting**: Enable batch splitting for contention errors (default: true).
  - **minBatchSize**: Minimum batch size for splitting (default: 10).
  - **convertInvalidIds**: Automatically convert invalid _id types to string (default: true). When enabled, the system will detect errors like "_id must be an objectId, string, long; found int" and automatically convert the problematic _id fields to strings.

#### Index Synchronization Configuration
- **syncAllIndexes**: (Optional) When set to `true`, automatically syncs all indexes (excluding `_id_`) from every source collection to the corresponding target collection. Default is `false`.
- **indexOnly**: (Optional) When set to `true`, the tool **only syncs indexes** and skips all data migration and incremental replication. The process exits after all indexes are created. Must be used with `syncAllIndexes: true` or explicit `indexes` configuration. Default is `false`.
- **indexes**: (Optional) An array of index configurations for synchronizing specific indexes from source to target collections.
  - **sourceCollection**: The name of the source collection containing the indexes to sync.
  - **indexNames**: An array of index names to synchronize (the tool will retrieve the full index definitions from the source).

**Index Sync Behavior:**
- Index synchronization occurs **only during initial migration** (not during incremental replication)
- Indexes are created on the target collection **before data migration** begins
- **Skip existing indexes**: If an index already exists on the target collection, it is skipped (no duplicate creation attempts)
- The tool automatically resolves the target collection name:
  - If a mapping is defined in the `collections` configuration, it uses the mapped target collection name
  - If no mapping is found, it assumes the target collection has the same name as the source collection
- **Non-blocking errors**: If index creation fails, the tool logs a warning and continues with data migration
- **Preserves existing indexes**: Indexes already present on the target collection that don't exist in the source are kept unchanged
- The `_id_` index is automatically skipped as it's created by MongoDB
- **Async with throttling**: Index builds are launched asynchronously with a concurrency limit of 1 to prevent Firestore cross-transaction contention. Each build uses a dedicated client with no socket timeout so long-running index builds are not killed.

**Example Configuration:**
```json
{
  "databasePairs": [
    {
      "source": {
        "connectionString": "mongodb://localhost:27017/?replicaSet=rs0",
        "database": "source_db"
      },
      "target": {
        "connectionString": "mongodb://localhost:27017",
        "database": "target_db",
        "collections": [
          {
            "sourceCollection": "users",
            "targetCollection": "app_users"
          }
        ],
        "indexes": [
          {
            "sourceCollection": "users",
            "indexNames": ["email_1", "created_at_-1"]
          },
          {
            "sourceCollection": "orders",
            "indexNames": ["user_id_1", "status_1_created_at_-1"]
          }
        ]
      }
    }
  ]
}
```

In this example:
- The `email_1` and `created_at_-1` indexes from the `users` collection will be created on the `app_users` collection (following the collection mapping)
- The `user_id_1` and `status_1_created_at_-1` indexes from the `orders` collection will be created on the `orders` collection (same name, no mapping)

#### Index-Only Replication

If you want to **only sync indexes** without migrating any data, set `indexOnly` to `true`. This is useful when:
- You want to pre-create indexes on the target before running a full migration
- You need to sync indexes independently of data migration
- You want to verify index compatibility with the target (e.g., Firestore)

```json
{
  "databasePairs": [
    {
      "source": {
        "connectionString": "mongodb://localhost:27017/?replicaSet=rs0",
        "database": "source_db",
        "replicationMethod": "oplog-legacy"
      },
      "target": {
        "connectionString": "mongodb://target:27017",
        "database": "target_db",
        "syncAllIndexes": true,
        "indexOnly": true
      }
    }
  ]
}
```

**Index-Only Replication Behavior:**
- Works with all modes: `migrate`, `changestream`, `oplog`, and `oplog-legacy`
- Reads all index definitions from the source database
- Skips indexes that already exist on the target (no duplicate creation)
- Creates indexes asynchronously with throttling (one at a time) to prevent Firestore cross-transaction contention
- Waits for all index builds to complete before exiting
- **No data is migrated** — only index definitions are synced
- **No incremental replication** — the process exits after indexes are created (no oplog tailing or change stream)

You can run it with either mode:
```bash
# Using migrate mode (simplest — no replica set needed for target)
./migrate -mode=migrate

# Using live mode (will sync indexes and exit without starting replication)
./migrate -mode=live
```

#### Replication Method Configuration
- **replicationMethod**: (Optional) Specifies the replication method for live mode. Possible values:
  - `"changestream"` (default): Uses MongoDB change streams for incremental replication (requires MongoDB 3.6+ with replica set)
  - `"oplog"`: Uses MongoDB oplog tailing for incremental replication (works with older MongoDB versions)

**Oplog-Based Replication:**

For databases that don't support change streams or for legacy MongoDB versions, you can use oplog-based replication:

```json
{
  "databasePairs": [
    {
      "source": {
        "connectionString": "mongodb://legacy:27017/?replicaSet=rs0",
        "database": "legacy_db",
        "replicationMethod": "oplog"
      },
      "target": {
        "connectionString": "mongodb://localhost:27017",
        "database": "new_db",
        "collections": [
          {
            "sourceCollection": "orders",
            "targetCollection": "orders"
          }
        ]
      }
    }
  ]
}
```

**Legacy MongoDB Support (MongoDB 3.0/3.2):**

For very old MongoDB versions (3.0, 3.2) that use wire protocol version 3, use the `oplog-legacy` replication method:

```json
{
  "databasePairs": [
    {
      "source": {
        "connectionString": "mongodb://oldserver:27017/?replicaSet=rs0",
        "database": "legacy_db",
        "replicationMethod": "oplog-legacy"
      },
      "target": {
        "connectionString": "mongodb://newserver:27018",
        "database": "modern_db"
      }
    }
  ]
}
```

When no `collections` are specified (as above), the tool will automatically detect all collections in the source database using the legacy mgo driver and migrate them to the target database with the same collection names. You can also specify explicit collection mappings if you want to rename collections during migration:

```json
{
  "databasePairs": [
    {
      "source": {
        "connectionString": "mongodb://oldserver:27017/?replicaSet=rs0",
        "database": "legacy_db",
        "replicationMethod": "oplog-legacy"
      },
      "target": {
        "connectionString": "mongodb://newserver:27018",
        "database": "modern_db",
        "collections": [
          { "sourceCollection": "users", "targetCollection": "app_users" },
          { "sourceCollection": "orders", "targetCollection": "orders" }
        ]
      }
    }
  ]
}
```

**Legacy Mode Implementation:**
- Uses **mgo driver** for source MongoDB (supports wire version 3)
- Uses **modern mongo-driver** for target MongoDB (supports wire version 12+)
- Both drivers coexist in the same binary without conflicts
- Leverages GTM legacy library with mgo for oplog tailing
- Supports full initial migration + incremental replication
- Perfect for migrating from MongoDB 3.0/3.2 to modern MongoDB/Firestore

**When to Use oplog-legacy:**
- Source MongoDB version 3.0, 3.2, or 3.4 (wire version 3)
- Target MongoDB is modern version (3.6+ or wire version 6+)
- You need to bridge the gap between very old and very new MongoDB versions

**When to Use Oplog Replication (Standard):**
- Source database doesn't support change streams
- Migrating from MongoDB versions earlier than 3.6
- Source MongoDB doesn't have change streams enabled
- You need lower-level access to the operation log

**Oplog Replication Behavior:**
- Requires source MongoDB to be running as a replica set (oplog only exists on replica sets)
- Uses the GTM (Go Tail Mongo) library for robust oplog tailing
- Automatically handles reconnection and resume from last processed timestamp
- Stores resume position in `oplogTimestamp-global.json` file
- Same seamless initial + incremental migration flow as change streams:
  1. Captures current oplog timestamp before initial migration
  2. Performs full initial migration (including index sync if configured)
  3. Starts tailing oplog from captured timestamp to catch all changes during migration
- Filters operations to only process configured collections
- Supports insert, update, and delete operations
- Automatically converts oplog operations to unified event format

**Oplog vs Change Streams:**

| Feature | Change Streams | Oplog |
|---------|---------------|-------|
| MongoDB Version | 3.6+ | All versions with replica set |
| API Level | High-level, structured events | Low-level, raw oplog entries |
| Server-side Filtering | Yes | No (filtered client-side) |
| Resume Token | Opaque binary token | Timestamp-based |
| Recommended For | Modern MongoDB (3.6+) | Legacy MongoDB or special cases |

## Usage

1. Migrate Mode:

   To perform a one-time migration of data from source MongoDB to target MongoDB:

   ```bash
   ./migrate -mode=migrate
   ```

2. Live Mode:

   To set up live replication using MongoDB change streams:

   ```bash
   ./migrate -mode=live
   ```

   The application will continuously listen for changes in the specified MongoDB collections and replicate them to the target MongoDB.

3. Additional Options:

   ```bash
   ./migrate -help
   ```

   This will display all available command-line options:

   ```
   Options:
     -config string
           Path to configuration file (default "mongodb_replication_config.json")
     -mode string
           Operation mode: 'migrate' or 'live' (default "migrate")
     -log-level string
           Log level: debug, info, warn, error (default "info")
     -help
           Display this help information
   ```

## Key Features

### Multi-level Parallelism

The application implements parallelism at multiple levels to maximize performance:

1. **Collection-Level Parallelism**:
   - Multiple collections are processed concurrently
   - Controlled by the `concurrentCollections` parameter (default: 4)
   - Each collection is processed in its own goroutine
   - A semaphore limits the maximum number of concurrent collections
   - Higher values allow more collections to be migrated simultaneously

2. **Batch-Level Parallelism in Standard Migration**:
   - For collections that don't use partitioning (smaller collections)
   - Controlled by the `initialMigrationWorkers` parameter (default: 5)
   - Documents are read sequentially but processed in batches by multiple workers
   - Each worker processes batches in parallel

3. **Partition-Level Parallelism**:
   - For large collections (size >= `minDocsForParallelReads`)
   - The collection is divided into partitions based on document ID ranges
   - Controlled by the `maxReadPartitions` parameter (default: 8)
   - Each partition is processed in its own goroutine with its own cursor
   - Partitions are created using sampling to ensure even distribution

4. **Batch-Level Parallelism within Partitions**:
   - Within each partition, batches are processed by multiple workers
   - Controlled by the `workersPerPartition` parameter (default: 3)
   - Documents are read sequentially within each partition but processed in parallel
   - Provides an additional level of parallelism for large collections

5. **Change Stream Parallelism** (Live Mode):
   - In live mode, a client-level change stream watches all collections
   - Change events are distributed to workers based on document ID hash
   - Controlled by the `incrementalWorkerCount` parameter (default: CPU cores)
   - Ensures operations for the same document are always processed by the same worker

### Tuning Parallelism Parameters

For optimal performance, consider these guidelines:

1. **concurrentCollections**:
   - Set based on the number and size of collections
   - Higher values process more collections simultaneously
   - Consider memory constraints when setting this value
   - For systems with many small collections, higher values (8-16) may improve throughput
   - For systems with few large collections, lower values (2-4) may be more efficient

2. **initialMigrationWorkers**:
   - Set based on available CPU cores and I/O capacity
   - Controls batch processing parallelism for standard migration
   - For CPU-bound workloads: set to number of available cores
   - For I/O-bound workloads: can be set higher than available cores

3. **maxReadPartitions**:
   - Controls how many partitions large collections are divided into
   - Higher values create more partitions but with smaller document ranges
   - Optimal values typically range from 4-16 depending on collection size

4. **workersPerPartition**:
   - Controls batch processing parallelism within each partition
   - For balanced resource allocation: total_cores ÷ maxReadPartitions
   - Avoid setting too high to prevent contention within partitions

### Enhanced Checkpoint Mechanism

The application implements a robust checkpoint mechanism using a single client-level resume token:

1. **Initial Replication Process**:
   - When starting in live mode, the tool first checks for an existing global resume token
   - If a resume token exists, it begins incremental replication immediately from that point
   - If no resume token exists (new replication):
     1. The tool creates a change stream and obtains an initial resume token
     2. It performs a full migration of all collections
     3. After full migration completes, incremental replication starts using the initial resume token, capturing all changes that occurred after the initial migration

2. **Client-Level Resume Token**:
   - A single global resume token is used for the client-level change stream
   - This token acts as a checkpoint that covers all databases and collections
   - Stored in a file named `resumeToken-global.json`
   - Automatically backed up before being updated to prevent corruption
   - Dual checkpoint timing mechanism:
     - **Count-based checkpoints**: Save after processing the number of changes specified by `saveThreshold`
     - **Time-based checkpoints**: Save at the interval specified by `checkpointInterval` (in minutes) regardless of the number of changes

3. **Failure Recovery Process**:
   - If replication fails or the process is interrupted:
     1. On restart, the tool loads the last saved global resume token
     2. Replication resumes precisely from the last checkpoint
     3. No data is lost or duplicated during the recovery

### Parallel Processing in Live Mode

The application implements a sophisticated parallel processing system for change stream events in live mode:

1. **Hash-Based Distribution**: Operations are distributed to workers based on document ID hash, ensuring that operations for the same document always go to the same worker.

2. **Data-Driven Processing**: Within each worker, operations are grouped by namespace and operation type. A new group is created whenever:
   - The operation type changes
   - The namespace changes
   - The current group reaches the maximum size

3. **Sequential Group Processing**: Groups are processed in strict sequential order within each worker, ensuring data consistency.

4. **Optimized Bulk Writes**: Operations within a group are executed as bulk writes:
   - Insert and delete operations use unordered bulk writes for better performance
   - Update and replace operations use ordered bulk writes to ensure consistency
   - The `forceOrderedOperations` configuration option can force ordered operations for all types

5. **Efficient Error Handling**: If a bulk operation fails, the system falls back to individual operations for the failed items, ensuring robustness.

### Parallel Reads for Large Collections

For large collections, the application uses parallel reads to speed up the initial migration:

1. **Intelligent Partitioning**: The collection is partitioned based on the _id field type:
   - For ObjectIDs: Uses timestamp-based or sampling-based partitioning
   - For numeric IDs: Uses range-based or sampling-based partitioning
   - For other types: Uses hash-based partitioning with the $mod operator

2. **Adaptive Partition Count**: The number of partitions is calculated based on collection size and configuration parameters.

3. **Two-Level Parallelism**:
   - **Partition-Level Parallelism**: Each partition is processed in parallel, with its own cursor
   - **Batch-Level Parallelism**: Within each partition, multiple worker goroutines process batches in parallel
   - **Configurable Worker Count**: The number of workers per partition can be configured using the `workersPerPartition` parameter

4. **Efficient Batch Distribution**: Within each partition, batches are distributed to workers through channels, allowing for optimal resource utilization.

### Robust Retry Mechanism

The application includes a sophisticated retry mechanism for handling errors:

1. **Error Classification**: Errors are classified into different types:
   - Connection errors: Network-related issues
   - Contention errors: Lock timeouts, write conflicts, etc.
   - Other errors: Any other type of error

2. **Exponential Backoff**: Retries use exponential backoff with jitter to avoid thundering herd problems.

3. **Batch Splitting**: For contention errors, batches are progressively split to reduce contention.

4. **Special Handling**: Different error types receive specialized handling:
   - Contention errors: Fixed delay before retry
   - Duplicate key errors: Automatic fallback to upsert operations
   - Connection errors: Exponential backoff with the full batch
   - Invalid _id type errors: Automatic conversion of _id fields to strings when enabled

5. **_id Type Conversion**: When `convertInvalidIds` is enabled:
   - Detects errors like "_id must be an objectId, string, long; found int"
   - Automatically converts problematic _id fields to strings
   - Logs the conversion details for troubleshooting
   - Retries the operation with the converted _id fields
   - Only converts _id fields that cause errors, preserving the original types when possible

## Setting Up a Single-Node Replica Set for Development

If you're developing locally and want to test the live replication feature, you can set up a single-node replica set:

1. Start MongoDB with the replica set option:

   ```bash
   mongod --replSet rs0 --dbpath /path/to/data/directory
   ```

2. Initialize the replica set:

   ```bash
   mongosh
   > rs.initiate()
   ```

3. Verify the replica set status:

   ```bash
   > rs.status()
   ```

This will allow you to use change streams, which are required for the live replication feature.

## Project Structure

- `cmd/migrate/`: Contains the main application entry point.
- `pkg/config/`: Configuration handling.
- `pkg/db/`: MongoDB connection and operations.
- `pkg/logger/`: Logging utilities.
- `pkg/migration/`: Migration and replication logic.
  - `client_stream.go`: Client-level change stream implementation
  - `oplog_replicator.go`: Oplog-based replication implementation using GTM
  - `oplog_timestamp.go`: Oplog timestamp tracking and persistence
  - `oplog_converter.go`: GTM operation to event conversion
  - `migrator.go`: Core migration and replication logic
  - `resumetoken.go`: Resume token management
  - `parallel.go`: Parallel processing implementation for live mode
  - `parallel_read.go`: Parallel read implementation for large collections
  - `retry.go`: Retry mechanisms with exponential backoff and batch splitting
