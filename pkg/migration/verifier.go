package migration

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"reflect"
	"time"

	"github.com/gsbingo17/mongodb-migration/pkg/config"
	"github.com/gsbingo17/mongodb-migration/pkg/db"
	"github.com/gsbingo17/mongodb-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// Verifier handles post-migration data integrity verification
type Verifier struct {
	config     *config.Config
	log        *logger.Logger
	SampleSize int
	ReportPath string
}

// VerificationResult holds the output of the verification run
type VerificationResult struct {
	StartTime          time.Time                   `json:"startTime"`
	EndTime            time.Time                   `json:"endTime"`
	TotalCollections   int                         `json:"totalCollections"`
	MatchedCollections int                         `json:"matchedCollections"`
	FailedCollections  int                         `json:"failedCollections"`
	CollectionReports  map[string]CollectionReport `json:"collectionReports"`
}

// CollectionReport holds the verification statistics for a single collection mapping
type CollectionReport struct {
	SourceCollection string            `json:"sourceCollection"`
	TargetCollection string            `json:"targetCollection"`
	SourceCount      int64             `json:"sourceCount"`
	TargetCount      int64             `json:"targetCount"`
	CountMatches     bool              `json:"countMatches"`
	SampledDocs      int               `json:"sampledDocs"`
	MatchedDocs      int               `json:"matchedDocs"`
	MissingDocs      int               `json:"missingDocs"`
	MismatchedDocs   int               `json:"mismatchedDocs"`
	Details          []MismatchDetails `json:"details,omitempty"`
}

// MismatchDetails describes a single document content discrepancy
type MismatchDetails struct {
	DocID        interface{} `json:"docId"`
	Type         string      `json:"type"` // "missing" or "mismatch"
	Discrepancy  string      `json:"discrepancy,omitempty"`
}

// NewVerifier creates a new Verifier instance
func NewVerifier(config *config.Config, log *logger.Logger, sampleSize int, reportPath string) *Verifier {
	if sampleSize <= 0 {
		sampleSize = 1000 // Default sample size
	}
	if reportPath == "" {
		reportPath = "verification_report.json"
	}
	return &Verifier{
		config:     config,
		log:        log,
		SampleSize: sampleSize,
		ReportPath: reportPath,
	}
}

// Verify runs the verification process across all database pairs
func (v *Verifier) Verify(ctx context.Context) (*VerificationResult, error) {
	v.log.Info("Starting MongoDB migration verification process")
	result := &VerificationResult{
		StartTime:         time.Now(),
		CollectionReports: make(map[string]CollectionReport),
	}

	for i, pair := range v.config.DatabasePairs {
		v.log.Infof("Verifying database pair %d/%d (Source: %s, Target: %s)", i+1, len(v.config.DatabasePairs), pair.Source.Database, pair.Target.Database)

		// Connect to source MongoDB
		v.log.Infof("Connecting to source database %s...", pair.Source.Database)
		sourceDB, err := db.NewMongoDB(pair.Source.ConnectionString, pair.Source.Database, 10, 50, 0, nil, v.log)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to source database: %w", err)
		}
		defer sourceDB.Close(ctx)

		// Connect to target MongoDB
		v.log.Infof("Connecting to target database %s...", pair.Target.Database)
		targetDB, err := db.NewMongoDB(pair.Target.ConnectionString, pair.Target.Database, 10, 50, 0, nil, v.log)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to target database: %w", err)
		}
		defer targetDB.Close(ctx)

		// Get collections list
		collections := pair.Target.Collections
		if len(collections) == 0 {
			v.log.Info("No collections specified in config. Auto-detecting source collections...")
			sourceCollections, err := sourceDB.ListCollections(ctx)
			if err != nil {
				return nil, fmt.Errorf("failed to list collections: %w", err)
			}
			for _, name := range sourceCollections {
				collections = append(collections, config.CollectionConfig{
					SourceCollection: name,
					TargetCollection: name,
				})
			}
		}

		for _, collConfig := range collections {
			report, err := v.verifyCollection(ctx, sourceDB, targetDB, collConfig)
			if err != nil {
				v.log.Errorf("Error verifying collection %s: %v", collConfig.SourceCollection, err)
				continue
			}

			key := fmt.Sprintf("%s.%s -> %s.%s", sourceDB.GetDatabaseName(), collConfig.SourceCollection, targetDB.GetDatabaseName(), collConfig.TargetCollection)
			result.CollectionReports[key] = report
			result.TotalCollections++

			if report.CountMatches && report.MismatchedDocs == 0 && report.MissingDocs == 0 {
				result.MatchedCollections++
			} else {
				result.FailedCollections++
			}
		}
	}

	result.EndTime = time.Now()
	v.printSummary(result)
	
	if err := v.writeReport(result); err != nil {
		v.log.Errorf("Failed to save verification report: %v", err)
	} else {
		v.log.Infof("Detailed verification report written to: %s", v.ReportPath)
	}

	return result, nil
}

// verifyCollection verifies a single collection mapping
func (v *Verifier) verifyCollection(ctx context.Context, sourceDB, targetDB *db.MongoDB, collConfig config.CollectionConfig) (CollectionReport, error) {
	v.log.Infof("Verifying collection %s -> %s", collConfig.SourceCollection, collConfig.TargetCollection)

	sourceColl := sourceDB.GetCollection(collConfig.SourceCollection)
	targetColl := targetDB.GetCollection(collConfig.TargetCollection)

	report := CollectionReport{
		SourceCollection: collConfig.SourceCollection,
		TargetCollection: collConfig.TargetCollection,
	}

	// 1. Check counts (non-blocking)
	srcCount, err := sourceColl.EstimatedDocumentCount(ctx)
	if err != nil {
		v.log.Warnf("Collection %s: failed to get estimated document count from source: %v", collConfig.SourceCollection, err)
		srcCount = -1
	}
	report.SourceCount = srcCount

	tgtCount, err := targetColl.CountDocuments(ctx, bson.M{})
	if err != nil {
		v.log.Warnf("Collection %s: failed to get document count from target: %v", collConfig.TargetCollection, err)
		tgtCount = -1
	}
	report.TargetCount = tgtCount

	if srcCount != -1 && tgtCount != -1 {
		report.CountMatches = srcCount == tgtCount
		if !report.CountMatches {
			v.log.Warnf("Collection %s: Count mismatch! Source: %d, Target: %d (diff: %d)",
				collConfig.SourceCollection, srcCount, tgtCount, srcCount-tgtCount)
		}
	} else {
		report.CountMatches = false
		v.log.Warnf("Collection %s: Could not verify count matches because count query failed (Source: %d, Target: %d)",
			collConfig.SourceCollection, srcCount, tgtCount)
	}

	if srcCount == 0 {
		return report, nil
	}

	// 2. Random Sampling & content verification
	sampleSize := v.SampleSize
	if srcCount > 0 && int64(sampleSize) > srcCount {
		sampleSize = int(srcCount)
	}
	report.SampledDocs = sampleSize

	v.log.Infof("Fetching a native random sample of %d documents from %s...", sampleSize, collConfig.SourceCollection)
	pipeline := mongo.Pipeline{
		{{Key: "$sample", Value: bson.D{{Key: "size", Value: sampleSize}}}},
	}
	cursor, err := sourceColl.Aggregate(ctx, pipeline)
	if err != nil {
		return report, fmt.Errorf("failed to aggregate native sample: %w", err)
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var srcDoc bson.D
		if err := cursor.Decode(&srcDoc); err != nil {
			return report, fmt.Errorf("failed to decode source sample document: %w", err)
		}

		// Extract ID
		id := extractDocID(srcDoc)
		if id == nil {
			v.log.Warn("Sampled document from source has no _id, skipping field content comparison")
			continue
		}

		// Transform the source document to target schema format using same rules as Migrator
		transformedSrc, err := TransformFieldNames(srcDoc, nil, sourceDB.GetDatabaseName(), collConfig.SourceCollection, id)
		if err != nil {
			report.MismatchedDocs++
			report.Details = append(report.Details, MismatchDetails{
				DocID:       id,
				Type:        "mismatch",
				Discrepancy: fmt.Sprintf("Source document transformation error: %v", err),
			})
			continue
		}

		// Find in target DB
		var tgtDoc bson.D
		err = targetColl.FindOne(ctx, bson.M{"_id": id}).Decode(&tgtDoc)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				report.MissingDocs++
				report.Details = append(report.Details, MismatchDetails{
					DocID: id,
					Type:  "missing",
				})
			} else {
				return report, fmt.Errorf("failed to query target document by _id %v: %w", id, err)
			}
			continue
		}

		// Deep compare the documents
		matches, diff := CompareDocuments(transformedSrc, tgtDoc)
		if !matches {
			report.MismatchedDocs++
			report.Details = append(report.Details, MismatchDetails{
				DocID:       id,
				Type:        "mismatch",
				Discrepancy: diff,
			})
		} else {
			report.MatchedDocs++
		}
	}

	if err := cursor.Err(); err != nil {
		return report, fmt.Errorf("cursor error during aggregation: %w", err)
	}

	return report, nil
}

// CompareDocuments deep-compares two transformed BSON documents.
func CompareDocuments(sourceDoc, targetDoc interface{}) (bool, string) {
	srcMap, okS := toStandardMap(sourceDoc).(map[string]interface{})
	tgtMap, okT := toStandardMap(targetDoc).(map[string]interface{})

	if !okS || !okT {
		return false, fmt.Sprintf("BSON documents could not be converted to maps: srcIsMap=%t, tgtIsMap=%t", okS, okT)
	}

	return compareMaps(srcMap, tgtMap, "")
}

// toStandardMap normalizes BSON documents into standard Go maps recursively.
func toStandardMap(val interface{}) interface{} {
	switch v := val.(type) {
	case bson.D:
		m := make(map[string]interface{})
		for _, elem := range v {
			m[elem.Key] = toStandardMap(elem.Value)
		}
		return m
	case bson.M:
		m := make(map[string]interface{})
		for k, val := range v {
			m[k] = toStandardMap(val)
		}
		return m
	case map[string]interface{}:
		m := make(map[string]interface{})
		for k, val := range v {
			m[k] = toStandardMap(val)
		}
		return m
	case []interface{}:
		arr := make([]interface{}, len(v))
		for i, item := range v {
			arr[i] = toStandardMap(item)
		}
		return arr
	case bson.A:
		arr := make([]interface{}, len(v))
		for i, item := range v {
			arr[i] = toStandardMap(item)
		}
		return arr
	case primitive.Binary:
		return v.Data
	case primitive.DateTime:
		return v.Time()
	case time.Time:
		return v
	default:
		return v
	}
}

// compareMaps recursively compares two normalized standard maps.
func compareMaps(src, tgt map[string]interface{}, path string) (bool, string) {
	for k, srcVal := range src {
		currentPath := k
		if path != "" {
			currentPath = path + "." + k
		}

		tgtVal, ok := tgt[k]
		if !ok {
			return false, fmt.Sprintf("Field '%s' missing in target document", currentPath)
		}

		if ok, diff := compareValues(srcVal, tgtVal, currentPath); !ok {
			return false, diff
		}
	}

	// Check if target has unexpected extra fields
	for k := range tgt {
		currentPath := k
		if path != "" {
			currentPath = path + "." + k
		}
		if _, ok := src[k]; !ok {
			return false, fmt.Sprintf("Extra field '%s' in target document", currentPath)
		}
	}

	return true, ""
}

// compareValues checks BSON-compatible values for logical equivalence.
func compareValues(src, tgt interface{}, path string) (bool, string) {
	if src == nil && tgt == nil {
		return true, ""
	}
	if (src == nil && tgt != nil) || (src != nil && tgt == nil) {
		return false, fmt.Sprintf("Field '%s' mismatch: expected nil, got %v", path, tgt)
	}

	// Recursively handle maps
	srcMap, srcIsMap := src.(map[string]interface{})
	tgtMap, tgtIsMap := tgt.(map[string]interface{})
	if srcIsMap && tgtIsMap {
		return compareMaps(srcMap, tgtMap, path)
	}
	if srcIsMap || tgtIsMap {
		return false, fmt.Sprintf("Field '%s' type mismatch: expected map, got %T", path, tgt)
	}

	// Recursively handle slices
	srcSlice, srcIsSlice := src.([]interface{})
	tgtSlice, tgtIsSlice := tgt.([]interface{})
	if srcIsSlice && tgtIsSlice {
		if len(srcSlice) != len(tgtSlice) {
			return false, fmt.Sprintf("Field '%s' array length mismatch: expected %d elements, got %d", path, len(srcSlice), len(tgtSlice))
		}
		for i := range srcSlice {
			if ok, diff := compareValues(srcSlice[i], tgtSlice[i], fmt.Sprintf("%s[%d]", path, i)); !ok {
				return false, diff
			}
		}
		return true, ""
	}
	if srcIsSlice || tgtIsSlice {
		return false, fmt.Sprintf("Field '%s' type mismatch: expected array, got %T", path, tgt)
	}

	// Standardize numerical comparisons (e.g., compare float64, int32, int64 as floats)
	if isNumeric(src) && isNumeric(tgt) {
		srcNum := toFloat(src)
		tgtNum := toFloat(tgt)
		if math.Abs(srcNum-tgtNum) > 1e-9 {
			return false, fmt.Sprintf("Field '%s' numeric mismatch: expected %v, got %v", path, src, tgt)
		}
		return true, ""
	}

	// Standardize date/time comparison
	srcTime, srcIsTime := src.(time.Time)
	tgtTime, tgtIsTime := tgt.(time.Time)
	if srcIsTime && tgtIsTime {
		// Compare millisecond precision (BSON Date has millisecond resolution)
		if srcTime.UnixNano()/1e6 != tgtTime.UnixNano()/1e6 {
			return false, fmt.Sprintf("Field '%s' timestamp mismatch: expected %v, got %v", path, srcTime.Format(time.RFC3339Nano), tgtTime.Format(time.RFC3339Nano))
		}
		return true, ""
	}

	// Handle raw bytes
	srcBytes, srcIsBytes := src.([]byte)
	tgtBytes, tgtIsBytes := tgt.([]byte)
	if srcIsBytes && tgtIsBytes {
		if !reflect.DeepEqual(srcBytes, tgtBytes) {
			return false, fmt.Sprintf("Field '%s' binary content mismatch", path)
		}
		return true, ""
	}

	// Generic deep equal fallback for standard types (strings, booleans, ObjectIDs, etc.)
	if !reflect.DeepEqual(src, tgt) {
		return false, fmt.Sprintf("Field '%s' mismatch: expected %v (%T), got %v (%T)", path, src, src, tgt, tgt)
	}

	return true, ""
}

func isNumeric(val interface{}) bool {
	switch val.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return true
	}
	return false
}

func toFloat(val interface{}) float64 {
	switch v := val.(type) {
	case int:
		return float64(v)
	case int8:
		return float64(v)
	case int16:
		return float64(v)
	case int32:
		return float64(v)
	case int64:
		return float64(v)
	case uint:
		return float64(v)
	case uint8:
		return float64(v)
	case uint16:
		return float64(v)
	case uint32:
		return float64(v)
	case uint64:
		return float64(v)
	case float32:
		return float64(v)
	case float64:
		return v
	}
	return 0.0
}

// printSummary logs a human-readable summary of the verification results to terminal
func (v *Verifier) printSummary(res *VerificationResult) {
	fmt.Println("\n=============================================")
	fmt.Println("         MIGRATION VERIFICATION REPORT        ")
	fmt.Println("=============================================")
	fmt.Printf("Start Time:  %s\n", res.StartTime.Format(time.RFC3339))
	fmt.Printf("End Time:    %s\n", res.EndTime.Format(time.RFC3339))
	fmt.Printf("Duration:    %s\n", res.EndTime.Sub(res.StartTime).Round(time.Second))
	fmt.Println("---------------------------------------------")
	fmt.Printf("Collections Checked: %d\n", res.TotalCollections)
	fmt.Printf("Matches:             %d\n", res.MatchedCollections)
	fmt.Printf("Mismatches/Failures: %d\n", res.FailedCollections)
	fmt.Println("---------------------------------------------")
	fmt.Println("Detailed Collection Status:")

	for collName, report := range res.CollectionReports {
		fmt.Printf("\n* Collection: %s\n", collName)
		fmt.Printf("  - Count Status:  ")
		if report.CountMatches {
			fmt.Printf("PASS (Source: %d, Target: %d)\n", report.SourceCount, report.TargetCount)
		} else {
			fmt.Printf("FAIL (Source: %d, Target: %d, Diff: %d)\n", report.SourceCount, report.TargetCount, report.SourceCount-report.TargetCount)
		}

		fmt.Printf("  - Content Check: ")
		if report.MismatchedDocs == 0 && report.MissingDocs == 0 {
			fmt.Printf("PASS (Sampled: %d, Matched: %d)\n", report.SampledDocs, report.MatchedDocs)
		} else {
			fmt.Printf("FAIL (Sampled: %d, Matched: %d, Missing in Target: %d, Mismatched fields: %d)\n",
				report.SampledDocs, report.MatchedDocs, report.MissingDocs, report.MismatchedDocs)
			if len(report.Details) > 0 {
				fmt.Println("    Discrepancy Samples (First 5):")
				limit := len(report.Details)
				if limit > 5 {
					limit = 5
				}
				for i := 0; i < limit; i++ {
					det := report.Details[i]
					if det.Type == "missing" {
						fmt.Printf("      * Doc ID %v: Missing in target\n", det.DocID)
					} else {
						fmt.Printf("      * Doc ID %v: %s\n", det.DocID, det.Discrepancy)
					}
				}
			}
		}
	}
	fmt.Println("=============================================")
}

// writeReport saves the verification results as a JSON file
func (v *Verifier) writeReport(res *VerificationResult) error {
	data, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(v.ReportPath, data, 0644)
}
