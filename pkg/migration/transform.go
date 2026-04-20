package migration

import (
	"strings"

	"github.com/gsbingo17/mongodb-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
)

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

// renameFieldName renames a field name if it matches the __*__ pattern.
// Fields like __name__ are not supported by Firestore, so we strip one underscore
// from each side: __name__ → _name_
func renameFieldName(name string) string {
	if len(name) >= 5 && strings.HasPrefix(name, "__") && strings.HasSuffix(name, "__") {
		return "_" + name[2:len(name)-2] + "_"
	}
	return name
}

// TransformFieldNames recursively walks a document and renames any field
// matching the __*__ pattern to _*_ (strip one underscore from each side).
// Supports bson.D, bson.M, map[string]interface{}, and arrays.
// Logs each field rename at Info level with db, collection, and document ID context.
func TransformFieldNames(doc interface{}, log *logger.Logger, dbName, collName string, docID interface{}) interface{} {
	if doc == nil {
		return nil
	}

	switch d := doc.(type) {
	case bson.D:
		result := make(bson.D, len(d))
		for i, elem := range d {
			newKey := renameFieldName(elem.Key)
			if newKey != elem.Key && log != nil {
				log.Infof("Renamed field \"%s\" to \"%s\" in document [db=%s, collection=%s, _id=%v]",
					elem.Key, newKey, dbName, collName, docID)
			}
			result[i] = bson.E{
				Key:   newKey,
				Value: TransformFieldNames(elem.Value, log, dbName, collName, docID),
			}
		}
		return result

	case bson.M:
		result := make(bson.M, len(d))
		for k, v := range d {
			newKey := renameFieldName(k)
			if newKey != k && log != nil {
				log.Infof("Renamed field \"%s\" to \"%s\" in document [db=%s, collection=%s, _id=%v]",
					k, newKey, dbName, collName, docID)
			}
			result[newKey] = TransformFieldNames(v, log, dbName, collName, docID)
		}
		return result

	case map[string]interface{}:
		result := make(map[string]interface{}, len(d))
		for k, v := range d {
			newKey := renameFieldName(k)
			if newKey != k && log != nil {
				log.Infof("Renamed field \"%s\" to \"%s\" in document [db=%s, collection=%s, _id=%v]",
					k, newKey, dbName, collName, docID)
			}
			result[newKey] = TransformFieldNames(v, log, dbName, collName, docID)
		}
		return result

	case []interface{}:
		result := make([]interface{}, len(d))
		for i, item := range d {
			result[i] = TransformFieldNames(item, log, dbName, collName, docID)
		}
		return result

	case bson.A:
		result := make(bson.A, len(d))
		for i, item := range d {
			result[i] = TransformFieldNames(item, log, dbName, collName, docID)
		}
		return result

	default:
		// Primitive types (string, int, float, bool, ObjectID, etc.) - return as-is
		return doc
	}
}

// TransformBatch applies TransformFieldNames to each document in a batch.
// Extracts _id from each document for logging context.
func TransformBatch(batch []interface{}, log *logger.Logger, dbName, collName string) []interface{} {
	result := make([]interface{}, len(batch))
	for i, doc := range batch {
		docID := extractDocID(doc)
		result[i] = TransformFieldNames(doc, log, dbName, collName, docID)
	}
	return result
}
