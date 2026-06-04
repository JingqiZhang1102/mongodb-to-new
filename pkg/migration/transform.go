package migration

import (
	"encoding/json"
	"fmt"

	"github.com/gsbingo17/mongodb-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
)

// maxFieldNameLength is the maximum allowed field name length.
// Firestore has a 1,500-byte limit on field names. We use 1,000 as a safety threshold.
// Objects containing field names exceeding this limit are stringified to JSON.
const maxFieldNameLength = 1000

// FieldTransformer handles Firestore compatibility transformations
type FieldTransformer struct {
	enableFieldTransformations bool
	log                        *logger.Logger
}

// NewFieldTransformer creates a new FieldTransformer
func NewFieldTransformer(enable bool, log *logger.Logger) *FieldTransformer {
	return &FieldTransformer{
		enableFieldTransformations: enable,
		log:                        log,
	}
}

// extractDocID extracts the _id field from a document for logging purposes.
func extractDocID(doc interface{}) interface{} {
	switch d := doc.(type) {
	case bson.D:
		for _, elem := range d {
			if elem.Key == "_id" {
				return elem.Value
			}
		}
	case bson.M:
		return d["_id"]
	case map[string]interface{}:
		return d["_id"]
	}
	return nil
}

// Transform recursively walks a document and applies Firestore-compatible transformations:
//   - Removes empty field names
//   - Stringifies nested objects that contain field names exceeding maxFieldNameLength
//
// Returns the transformed document and an error if a field name collision is detected.
// Supports bson.D, bson.M, map[string]interface{}, and arrays.
// Logs transformations at Info/Warn level with db, collection, and document ID context.
func (t *FieldTransformer) Transform(doc interface{}, dbName, collName string, docID interface{}) (interface{}, error) {
	if !t.enableFieldTransformations {
		return doc, nil
	}
	// Root-level documents cannot be stringified (must remain documents for MongoDB insert).
	// Warn about long keys at root level but don't stringify.
	if doc != nil {
		t.warnRootLongKeys(doc, dbName, collName, docID)
	}
	return t.transformFieldNamesRecursive(doc, dbName, collName, docID, true)
}

// warnRootLongKeys logs warnings for any root-level field names exceeding maxFieldNameLength.
// Root documents cannot be stringified, so we can only warn about them.
func (t *FieldTransformer) warnRootLongKeys(doc interface{}, dbName, collName string, docID interface{}) {
	if t.log == nil {
		return
	}
	switch d := doc.(type) {
	case bson.D:
		for _, elem := range d {
			if len(elem.Key) > maxFieldNameLength {
				t.log.Warnf("Root-level field name exceeds %d chars (len=%d): \"%s...\" [db=%s, collection=%s, _id=%v]. Cannot stringify root document.",
					maxFieldNameLength, len(elem.Key), elem.Key[:80], dbName, collName, docID)
			}
		}
	case bson.M:
		for k := range d {
			if len(k) > maxFieldNameLength {
				t.log.Warnf("Root-level field name exceeds %d chars (len=%d): \"%s...\" [db=%s, collection=%s, _id=%v]. Cannot stringify root document.",
					maxFieldNameLength, len(k), k[:80], dbName, collName, docID)
			}
		}
	case map[string]interface{}:
		for k := range d {
			if len(k) > maxFieldNameLength {
				t.log.Warnf("Root-level field name exceeds %d chars (len=%d): \"%s...\" [db=%s, collection=%s, _id=%v]. Cannot stringify root document.",
					maxFieldNameLength, len(k), k[:80], dbName, collName, docID)
			}
		}
	}
}

// transformFieldNamesRecursive is the internal recursive implementation.
// isRoot=true for the top-level document (skip long key stringification),
// isRoot=false for nested objects (enable long key stringification).
func (t *FieldTransformer) transformFieldNamesRecursive(doc interface{}, dbName, collName string, docID interface{}, isRoot bool) (interface{}, error) {
	if doc == nil {
		return nil, nil
	}

	switch d := doc.(type) {
	case bson.D:
		// For nested objects: if any immediate key exceeds maxFieldNameLength,
		// stringify the entire object to avoid Firestore field name errors.
		if !isRoot {
			for _, elem := range d {
				if len(elem.Key) > maxFieldNameLength {
					if t.log != nil {
						t.log.Warnf("Field name exceeds %d chars (len=%d) in nested object. Stringifying parent object [db=%s, collection=%s, _id=%v]",
							maxFieldNameLength, len(elem.Key), dbName, collName, docID)
					}
					jsonBytes, err := json.Marshal(bsonDToMap(d))
					if err != nil {
						if t.log != nil {
							t.log.Errorf("Failed to stringify object with long field name [db=%s, collection=%s, _id=%v]: %v",
								dbName, collName, docID, err)
						}
						return nil, fmt.Errorf("failed to stringify object with long field name: %w", err)
					}
					return string(jsonBytes), nil
				}
			}
		}

		result := make(bson.D, 0, len(d))

		for _, elem := range d {
			// Remove empty field names (Firestore does not support them)
			if elem.Key == "" {
				if t.log != nil {
					t.log.Warnf("Removed empty field name from document [db=%s, collection=%s, _id=%v]",
						dbName, collName, docID)
				}
				continue
			}

			transformedValue, err := t.transformFieldNamesRecursive(elem.Value, dbName, collName, docID, false)
			if err != nil {
				return nil, err
			}

			result = append(result, bson.E{
				Key:   elem.Key,
				Value: transformedValue,
			})
		}
		return result, nil

	case bson.M:
		// For nested objects: if any immediate key exceeds maxFieldNameLength,
		// stringify the entire object.
		if !isRoot {
			for k := range d {
				if len(k) > maxFieldNameLength {
					if t.log != nil {
						t.log.Warnf("Field name exceeds %d chars (len=%d) in nested object. Stringifying parent object [db=%s, collection=%s, _id=%v]",
							maxFieldNameLength, len(k), dbName, collName, docID)
					}
					jsonBytes, err := json.Marshal(d)
					if err != nil {
						if t.log != nil {
							t.log.Errorf("Failed to stringify object with long field name [db=%s, collection=%s, _id=%v]: %v",
								dbName, collName, docID, err)
						}
						return nil, fmt.Errorf("failed to stringify object with long field name: %w", err)
					}
					return string(jsonBytes), nil
				}
			}
		}

		result := make(bson.M, len(d))

		for k, v := range d {
			// Remove empty field names (Firestore does not support them)
			if k == "" {
				if t.log != nil {
					t.log.Warnf("Removed empty field name from document [db=%s, collection=%s, _id=%v]",
						dbName, collName, docID)
				}
				continue
			}

			transformedValue, err := t.transformFieldNamesRecursive(v, dbName, collName, docID, false)
			if err != nil {
				return nil, err
			}
			result[k] = transformedValue
		}
		return result, nil

	case map[string]interface{}:
		// For nested objects: if any immediate key exceeds maxFieldNameLength,
		// stringify the entire object.
		if !isRoot {
			for k := range d {
				if len(k) > maxFieldNameLength {
					if t.log != nil {
						t.log.Warnf("Field name exceeds %d chars (len=%d) in nested object. Stringifying parent object [db=%s, collection=%s, _id=%v]",
							maxFieldNameLength, len(k), dbName, collName, docID)
					}
					jsonBytes, err := json.Marshal(d)
					if err != nil {
						if t.log != nil {
							t.log.Errorf("Failed to stringify object with long field name [db=%s, collection=%s, _id=%v]: %v",
								dbName, collName, docID, err)
						}
						return nil, fmt.Errorf("failed to stringify object with long field name: %w", err)
					}
					return string(jsonBytes), nil
				}
			}
		}

		result := make(map[string]interface{}, len(d))

		for k, v := range d {
			// Remove empty field names (Firestore does not support them)
			if k == "" {
				if t.log != nil {
					t.log.Warnf("Removed empty field name from document [db=%s, collection=%s, _id=%v]",
						dbName, collName, docID)
				}
				continue
			}

			transformedValue, err := t.transformFieldNamesRecursive(v, dbName, collName, docID, false)
			if err != nil {
				return nil, err
			}
			result[k] = transformedValue
		}
		return result, nil

	case []interface{}:
		result := make([]interface{}, len(d))
		for i, item := range d {
			transformedValue, err := t.transformFieldNamesRecursive(item, dbName, collName, docID, false)
			if err != nil {
				return nil, err
			}
			result[i] = transformedValue
		}
		return result, nil

	case bson.A:
		result := make(bson.A, len(d))
		for i, item := range d {
			transformedValue, err := t.transformFieldNamesRecursive(item, dbName, collName, docID, false)
			if err != nil {
				return nil, err
			}
			result[i] = transformedValue
		}
		return result, nil

	default:
		// Primitive types (string, int, float, bool, ObjectID, etc.) - return as-is
		return doc, nil
	}
}

// bsonDToMap converts a bson.D to a map for JSON marshaling.
// bson.D is an ordered slice of key-value pairs which json.Marshal handles differently.
func bsonDToMap(d bson.D) map[string]interface{} {
	result := make(map[string]interface{}, len(d))
	for _, elem := range d {
		result[elem.Key] = bsonValueToInterface(elem.Value)
	}
	return result
}

// bsonValueToInterface recursively converts bson types to standard Go types for JSON marshaling.
func bsonValueToInterface(v interface{}) interface{} {
	switch val := v.(type) {
	case bson.D:
		return bsonDToMap(val)
	case bson.M:
		result := make(map[string]interface{}, len(val))
		for k, v := range val {
			result[k] = bsonValueToInterface(v)
		}
		return result
	case bson.A:
		result := make([]interface{}, len(val))
		for i, item := range val {
			result[i] = bsonValueToInterface(item)
		}
		return result
	case []interface{}:
		result := make([]interface{}, len(val))
		for i, item := range val {
			result[i] = bsonValueToInterface(item)
		}
		return result
	default:
		return v
	}
}

// TransformBatch applies Transform to each document in a batch.
// Extracts _id from each document for logging context.
func (t *FieldTransformer) TransformBatch(batch []interface{}, dbName, collName string) ([]interface{}, error) {
	if !t.enableFieldTransformations {
		return batch, nil
	}
	result := make([]interface{}, len(batch))
	for i, doc := range batch {
		docID := extractDocID(doc)
		transformed, err := t.Transform(doc, dbName, collName, docID)
		if err != nil {
			return nil, err
		}
		result[i] = transformed
	}
	return result, nil
}
