package migration

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/gsbingo17/mongodb-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// CollectionPartitioner handles partitioning a collection for parallel reads
type CollectionPartitioner struct {
	sourceCollection    *mongo.Collection
	log                 *logger.Logger
	maxPartitions       int
	minDocsPerPartition int
	sampleSize          int
}

// NewCollectionPartitioner creates a new collection partitioner
func NewCollectionPartitioner(sourceCollection *mongo.Collection,
	log *logger.Logger, maxPartitions, minDocsPerPartition, sampleSize int) *CollectionPartitioner {
	return &CollectionPartitioner{
		sourceCollection:    sourceCollection,
		log:                 log,
		maxPartitions:       maxPartitions,
		minDocsPerPartition: minDocsPerPartition,
		sampleSize:          sampleSize,
	}
}

// Partition creates partitions for a collection
func (p *CollectionPartitioner) Partition(ctx context.Context) ([]bson.D, error) {
	// Count documents to determine if partitioning is needed
	// Use a longer timeout for the count operation
	countCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	count, err := p.sourceCollection.EstimatedDocumentCount(countCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to count documents: %w", err)
	}

	// If collection is small, return a single partition
	if count < int64(p.minDocsPerPartition) {
		p.log.Infof("Collection has only %d documents, using a single partition", count)
		return []bson.D{{}}, nil
	}

	// Calculate optimal partition count
	partitionCount := int(count) / p.minDocsPerPartition
	if partitionCount > p.maxPartitions {
		partitionCount = p.maxPartitions
	}
	if partitionCount < 1 {
		partitionCount = 1
	}

	p.log.Infof("Partitioning collection with %d documents into %d partitions", count, partitionCount)

	// If only one partition, return a single empty filter
	if partitionCount == 1 {
		return []bson.D{{}}, nil
	}

	// Determine partition strategy based on _id type
	// First, get a sample document to check _id type
	var sampleDoc bson.M
	err = p.sourceCollection.FindOne(ctx, bson.D{}).Decode(&sampleDoc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			// Empty collection, return single empty filter
			return []bson.D{{}}, nil
		}
		return nil, fmt.Errorf("failed to get sample document: %w", err)
	}

	// Check _id type
	idValue, ok := sampleDoc["_id"]
	if !ok {
		return nil, fmt.Errorf("sample document has no _id field")
	}

	switch idValue.(type) {
	case primitive.ObjectID:
		// Use sampling-based partitioning for ObjectIDs
		return p.createObjectIDPartitionsWithSampling(ctx, partitionCount)
	case int, int32, int64, float64:
		// Use sampling-based partitioning for numeric IDs
		return p.createNumericPartitionsWithSampling(ctx, partitionCount)
	default:
		// For other types, use the $mod operator (hash-based partitioning)
		return p.createModPartitions(partitionCount)
	}
}

// createObjectIDPartitionsWithSampling creates partitions based on ObjectID sampling
func (p *CollectionPartitioner) createObjectIDPartitionsWithSampling(ctx context.Context, partitionCount int) ([]bson.D, error) {
	// Adjust sample size based on collection size, but ensure it's large enough
	sampleSize := p.sampleSize
	if sampleSize < partitionCount*10 {
		sampleSize = partitionCount * 10 // Ensure at least 10 samples per partition
	}

	p.log.Infof("Sampling %d documents to create %d partitions", sampleSize, partitionCount)

	// Sample documents to understand the _id distribution
	pipeline := mongo.Pipeline{
		{{Key: "$sample", Value: bson.D{{Key: "size", Value: sampleSize}}}},
		{{Key: "$project", Value: bson.D{{Key: "_id", Value: 1}}}},
		{{Key: "$sort", Value: bson.D{{Key: "_id", Value: 1}}}},
	}

	cursor, err := p.sourceCollection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("failed to sample documents: %w", err)
	}
	defer cursor.Close(ctx)

	// Collect all sampled _ids
	var sampledIDs []primitive.ObjectID
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("failed to decode sampled document: %w", err)
		}

		if id, ok := doc["_id"].(primitive.ObjectID); ok {
			sampledIDs = append(sampledIDs, id)
		}
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error during sampling: %w", err)
	}

	// If not enough samples, fall back to min/max approach
	if len(sampledIDs) < partitionCount+1 {
		p.log.Warnf("Not enough samples (%d) for %d partitions, falling back to min/max approach",
			len(sampledIDs), partitionCount)
		return p.createObjectIDPartitionsWithMinMax(ctx)
	}

	// Use sampled IDs to create partitions
	partitions := make([]bson.D, 0, partitionCount)

	// Calculate step size to evenly distribute partitions
	step := len(sampledIDs) / (partitionCount + 1)

	// Create partition filters
	for i := 0; i < partitionCount; i++ {
		startIdx := (i + 1) * step // Skip the first step to avoid edge cases
		endIdx := (i + 2) * step

		if endIdx >= len(sampledIDs) {
			endIdx = len(sampledIDs) - 1
		}

		startID := sampledIDs[startIdx]
		endID := sampledIDs[endIdx]

		if i == 0 {
			// First partition includes everything up to the first boundary
			partitions = append(partitions, bson.D{{Key: "_id", Value: bson.D{{Key: "$lt", Value: endID}}}})
		} else if i == partitionCount-1 {
			// Last partition includes everything from the last boundary
			partitions = append(partitions, bson.D{{Key: "_id", Value: bson.D{{Key: "$gte", Value: startID}}}})
		} else {
			// Middle partitions
			partitions = append(partitions, bson.D{
				{Key: "_id", Value: bson.D{
					{Key: "$gte", Value: startID},
					{Key: "$lt", Value: endID},
				}},
			})
		}
	}

	return partitions, nil
}

// createObjectIDPartitionsWithMinMax creates partitions based on min/max ObjectIDs
func (p *CollectionPartitioner) createObjectIDPartitionsWithMinMax(ctx context.Context) ([]bson.D, error) {
	// Find min and max ObjectIDs
	var minDoc, maxDoc bson.M

	err := p.sourceCollection.FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{Key: "_id", Value: 1}})).Decode(&minDoc)
	if err != nil {
		return nil, fmt.Errorf("failed to find min _id: %w", err)
	}

	err = p.sourceCollection.FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}})).Decode(&maxDoc)
	if err != nil {
		return nil, fmt.Errorf("failed to find max _id: %w", err)
	}

	minID, _ := minDoc["_id"].(primitive.ObjectID)
	maxID, _ := maxDoc["_id"].(primitive.ObjectID)

	// For ObjectIDs, we can use timestamp-based partitioning
	minTime := minID.Timestamp()
	maxTime := maxID.Timestamp()
	timeRange := maxTime.Sub(minTime)

	// Calculate optimal partition count based on time range
	partitionCount := p.maxPartitions
	partitionDuration := timeRange / time.Duration(partitionCount)

	// Create partitions
	partitions := make([]bson.D, 0, partitionCount)

	for i := 0; i < partitionCount; i++ {
		startTime := minTime.Add(partitionDuration * time.Duration(i))
		startID := primitive.NewObjectIDFromTimestamp(startTime)

		var endTime time.Time
		if i == partitionCount-1 {
			// Last partition includes the max ID
			endTime = maxTime.Add(time.Second) // Add a second to ensure inclusion
		} else {
			endTime = minTime.Add(partitionDuration * time.Duration(i+1))
		}
		endID := primitive.NewObjectIDFromTimestamp(endTime)

		if i == 0 {
			// First partition
			partitions = append(partitions, bson.D{{Key: "_id", Value: bson.D{{Key: "$lt", Value: endID}}}})
		} else if i == partitionCount-1 {
			// Last partition
			partitions = append(partitions, bson.D{{Key: "_id", Value: bson.D{{Key: "$gte", Value: startID}}}})
		} else {
			// Middle partitions
			partitions = append(partitions, bson.D{
				{Key: "_id", Value: bson.D{
					{Key: "$gte", Value: startID},
					{Key: "$lt", Value: endID},
				}},
			})
		}
	}

	return partitions, nil
}

// createNumericPartitionsWithSampling creates partitions based on numeric _id sampling
func (p *CollectionPartitioner) createNumericPartitionsWithSampling(ctx context.Context, partitionCount int) ([]bson.D, error) {
	// Adjust sample size based on collection size, but ensure it's large enough
	sampleSize := p.sampleSize
	if sampleSize < partitionCount*10 {
		sampleSize = partitionCount * 10 // Ensure at least 10 samples per partition
	}

	p.log.Infof("Sampling %d documents to create %d partitions", sampleSize, partitionCount)

	// Sample documents to understand the _id distribution
	pipeline := mongo.Pipeline{
		{{Key: "$sample", Value: bson.D{{Key: "size", Value: sampleSize}}}},
		{{Key: "$project", Value: bson.D{{Key: "_id", Value: 1}}}},
		{{Key: "$sort", Value: bson.D{{Key: "_id", Value: 1}}}},
	}

	cursor, err := p.sourceCollection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("failed to sample documents: %w", err)
	}
	defer cursor.Close(ctx)

	// Collect all sampled _ids as float64 for consistent handling
	var sampledIDs []float64
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("failed to decode sampled document: %w", err)
		}

		// Convert various numeric types to float64
		var numericID float64
		switch id := doc["_id"].(type) {
		case int:
			numericID = float64(id)
		case int32:
			numericID = float64(id)
		case int64:
			numericID = float64(id)
		case float64:
			numericID = id
		default:
			continue // Skip non-numeric IDs
		}

		sampledIDs = append(sampledIDs, numericID)
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error during sampling: %w", err)
	}

	// If not enough samples, fall back to min/max approach
	if len(sampledIDs) < partitionCount+1 {
		p.log.Warnf("Not enough samples (%d) for %d partitions, falling back to min/max approach",
			len(sampledIDs), partitionCount)
		return p.createNumericPartitionsWithMinMax(ctx)
	}

	// Use sampled IDs to create partitions
	partitions := make([]bson.D, 0, partitionCount)

	// Calculate step size to evenly distribute partitions
	step := len(sampledIDs) / (partitionCount + 1)

	// Create partition filters
	for i := 0; i < partitionCount; i++ {
		startIdx := (i + 1) * step // Skip the first step to avoid edge cases
		endIdx := (i + 2) * step

		if endIdx >= len(sampledIDs) {
			endIdx = len(sampledIDs) - 1
		}

		startID := sampledIDs[startIdx]
		endID := sampledIDs[endIdx]

		if i == 0 {
			// First partition includes everything up to the first boundary
			partitions = append(partitions, bson.D{{Key: "_id", Value: bson.D{{Key: "$lt", Value: endID}}}})
		} else if i == partitionCount-1 {
			// Last partition includes everything from the last boundary
			partitions = append(partitions, bson.D{{Key: "_id", Value: bson.D{{Key: "$gte", Value: startID}}}})
		} else {
			// Middle partitions
			partitions = append(partitions, bson.D{
				{Key: "_id", Value: bson.D{
					{Key: "$gte", Value: startID},
					{Key: "$lt", Value: endID},
				}},
			})
		}
	}

	return partitions, nil
}

// createNumericPartitionsWithMinMax creates partitions based on min/max numeric IDs
func (p *CollectionPartitioner) createNumericPartitionsWithMinMax(ctx context.Context) ([]bson.D, error) {
	// Find min and max numeric IDs
	var minDoc, maxDoc bson.M

	err := p.sourceCollection.FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{Key: "_id", Value: 1}})).Decode(&minDoc)
	if err != nil {
		return nil, fmt.Errorf("failed to find min _id: %w", err)
	}

	err = p.sourceCollection.FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}})).Decode(&maxDoc)
	if err != nil {
		return nil, fmt.Errorf("failed to find max _id: %w", err)
	}

	// Convert to float64 for consistent handling
	var minID, maxID float64

	switch id := minDoc["_id"].(type) {
	case int:
		minID = float64(id)
	case int32:
		minID = float64(id)
	case int64:
		minID = float64(id)
	case float64:
		minID = id
	}

	switch id := maxDoc["_id"].(type) {
	case int:
		maxID = float64(id)
	case int32:
		maxID = float64(id)
	case int64:
		maxID = float64(id)
	case float64:
		maxID = id
	}

	// Calculate range for each partition
	partitionCount := p.maxPartitions
	idRange := maxID - minID
	partitionSize := idRange / float64(partitionCount)

	// Create partitions
	partitions := make([]bson.D, 0, partitionCount)

	for i := 0; i < partitionCount; i++ {
		startID := minID + (partitionSize * float64(i))

		var endID float64
		if i == partitionCount-1 {
			// Last partition includes the max ID
			endID = maxID + 1 // Add 1 to ensure inclusion
		} else {
			endID = minID + (partitionSize * float64(i+1))
		}

		if i == 0 {
			// First partition
			partitions = append(partitions, bson.D{{Key: "_id", Value: bson.D{{Key: "$lt", Value: endID}}}})
		} else if i == partitionCount-1 {
			// Last partition
			partitions = append(partitions, bson.D{{Key: "_id", Value: bson.D{{Key: "$gte", Value: startID}}}})
		} else {
			// Middle partitions
			partitions = append(partitions, bson.D{
				{Key: "_id", Value: bson.D{
					{Key: "$gte", Value: startID},
					{Key: "$lt", Value: endID},
				}},
			})
		}
	}

	return partitions, nil
}

// createModPartitions creates partitions using the $mod operator (hash-based partitioning)
func (p *CollectionPartitioner) createModPartitions(partitionCount int) ([]bson.D, error) {
	p.log.Infof("Using hash-based partitioning with %d partitions", partitionCount)

	partitions := make([]bson.D, 0, partitionCount)

	for i := 0; i < partitionCount; i++ {
		partitions = append(partitions, bson.D{
			{Key: "_id", Value: bson.D{
				{Key: "$mod", Value: bson.A{partitionCount, i}},
			}},
		})
	}

	return partitions, nil
}

// Helper function to interpolate between two hex strings
func interpolateHex(minHex, maxHex string, ratio float64) string {
	if len(minHex) != len(maxHex) {
		return minHex // Fallback
	}

	result := make([]byte, len(minHex))

	for i := 0; i < len(minHex); i++ {
		minVal, _ := strconv.ParseInt(string(minHex[i]), 16, 8)
		maxVal, _ := strconv.ParseInt(string(maxHex[i]), 16, 8)

		interpolated := minVal + int64(ratio*float64(maxVal-minVal))
		if interpolated > 15 {
			interpolated = 15
		}

		result[i] = "0123456789abcdef"[interpolated]
	}

	return string(result)
}
