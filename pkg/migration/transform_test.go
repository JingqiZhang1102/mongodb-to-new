package migration

import (
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/gsbingo17/mongodb-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// helper function to assert success in non-collision tests
func mustTransform(t *testing.T, doc interface{}, log *logger.Logger, dbName, collName string, docID interface{}) interface{} {
	t.Helper()
	res, err := TransformFieldNames(doc, log, dbName, collName, docID)
	if err != nil {
		t.Fatalf("TransformFieldNames failed unexpectedly: %v", err)
	}
	return res
}

// helper function to assert batch success in non-collision tests
func mustTransformBatch(t *testing.T, batch []interface{}, log *logger.Logger, dbName, collName string) []interface{} {
	t.Helper()
	res, err := TransformBatch(batch, log, dbName, collName)
	if err != nil {
		t.Fatalf("TransformBatch failed unexpectedly: %v", err)
	}
	return res
}

func TestTransformRenameFieldNames(t *testing.T) {
	log := logger.New()

	// Define table-driven test cases for clear visibility of input vs expected output
	tests := []struct {
		name     string
		input    interface{}
		expected interface{}
	}{
		{
			name: "bson.M mapping: __name__ to _name_ and collapse ___ to _",
			input: bson.M{
				"__name__": "alice",
				"___":      "triple-underscore",
				"normal":   "value",
				"_____3_______": "3	underscores",
			},
			expected: bson.M{
				"_name_": "alice",
				"_":      "triple-underscore",
				"normal": "value",
				"_3_":    "3	underscores",
			},
		},
		{
			name: "map[string]interface{} mapping: __height__ to _height_",
			input: map[string]interface{}{
				"__height__": 180,
			},
			expected: map[string]interface{}{
				"_height_": 180,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			output := mustTransform(t, tc.input, log, "db", "coll", "id")
			if diff := cmp.Diff(tc.expected, output); diff != "" {
				t.Errorf("\n[FAIL] %s mismatch (-want +got):\n%s", tc.name, diff)
			}
		})
	}
}

func TestTransformBsonDOrderedRenaming(t *testing.T) {
	log := logger.New()

	// bson.D needs individual element verification to maintain precise field ordering assertion
	input := bson.D{
		{Key: "__age__", Value: 30},
		{Key: "____", Value: "four-underscores"},
	}

	expected := bson.D{
		{Key: "_age_", Value: 30},
		{Key: "_", Value: "four-underscores"},
	}

	output := mustTransform(t, input, log, "db", "coll", "id").(bson.D)

	if diff := cmp.Diff(expected, output); diff != "" {
		t.Errorf("\n[FAIL] bson.D Ordered Renaming mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformStringifyLongNestedKeys(t *testing.T) {
	log := logger.New()
	longKey := strings.Repeat("a", 1001)

	// 1. Root-level long key: Must remain unmodified (warnings only, no stringification)
	rootInput := bson.M{longKey: "root-long"}
	rootExpected := bson.M{longKey: "root-long"}
	rootOutput := mustTransform(t, rootInput, log, "db", "coll", "id")

	if diff := cmp.Diff(rootExpected, rootOutput); diff != "" {
		t.Errorf("\n[FAIL] Root-level long key handling mismatch (-want +got):\n%s", diff)
	}

	// 2. Nested document with long key: Entire sub-document should be stringified to JSON
	nestedInput := bson.M{
		"nested": bson.M{
			longKey: "nested-long",
		},
	}
	nestedOutput := mustTransform(t, nestedInput, log, "db", "coll", "id").(bson.M)

	expectedVal, ok := nestedOutput["nested"].(string)
	if !ok {
		t.Fatalf("\n[FAIL] Long key nested stringify did not produce JSON string, got type: %T", nestedOutput["nested"])
	}

	var decoded bson.M
	if err := json.Unmarshal([]byte(expectedVal), &decoded); err != nil {
		t.Fatalf("\n[FAIL] Failed to unmarshal stringified nested object: %v", err)
	}

	if decoded[longKey] != "nested-long" {
		t.Errorf("\n[FAIL] Decoded nested stringify does not contain expected key/value, got: %v", decoded)
	}

	// 3. Nested bson.D with long key: Entire sub-document should be stringified to JSON
	nestedDInput := bson.D{
		{Key: "nested", Value: bson.D{
			{Key: longKey, Value: "nested-long-d"},
		}},
	}
	nestedDOutput := mustTransform(t, nestedDInput, log, "db", "coll", "id").(bson.D)

	var nestedDVal string
	for _, elem := range nestedDOutput {
		if elem.Key == "nested" {
			nestedDVal = elem.Value.(string)
		}
	}

	var decodedD bson.M
	if err := json.Unmarshal([]byte(nestedDVal), &decodedD); err != nil {
		t.Fatalf("\n[FAIL] Failed to unmarshal stringified nested bson.D object: %v", err)
	}

	if decodedD[longKey] != "nested-long-d" {
		t.Errorf("\n[FAIL] Decoded nested bson.D stringify does not contain expected key/value, got: %v", decodedD)
	}

	// 4. Nested map[string]interface{} with long key: Entire sub-document should be stringified to JSON
	nestedMapInput := bson.M{
		"nested": map[string]interface{}{
			longKey: "nested-long-map",
		},
	}
	nestedMapOutput := mustTransform(t, nestedMapInput, log, "db", "coll", "id").(bson.M)

	nestedMapVal, ok := nestedMapOutput["nested"].(string)
	if !ok {
		t.Fatalf("\n[FAIL] Long key nested map did not produce JSON string, got type: %T", nestedMapOutput["nested"])
	}

	var decodedMap bson.M
	if err := json.Unmarshal([]byte(nestedMapVal), &decodedMap); err != nil {
		t.Fatalf("\n[FAIL] Failed to unmarshal stringified nested map object: %v", err)
	}

	if decodedMap[longKey] != "nested-long-map" {
		t.Errorf("\n[FAIL] Decoded nested map stringify does not contain expected key/value, got: %v", decodedMap)
	}
}

func TestTransformArraysAndBatches(t *testing.T) {
	log := logger.New()

	// 1. Standard slice: []interface{}
	arrayInput := bson.M{
		"arr": []interface{}{
			bson.M{"__elem__": "val"},
			"regular-string",
		},
	}
	arrayExpected := bson.M{
		"arr": []interface{}{
			bson.M{"_elem_": "val"},
			"regular-string",
		},
	}
	arrayOutput := mustTransform(t, arrayInput, log, "db", "coll", "id")

	if diff := cmp.Diff(arrayExpected, arrayOutput); diff != "" {
		t.Errorf("\n[FAIL] []interface{} Array processing mismatch (-want +got):\n%s", diff)
	}

	// 2. BSON array: bson.A
	bsonAInput := bson.M{
		"arr": bson.A{
			bson.M{"__elem__": "val"},
			"regular-string",
		},
	}
	bsonAExpected := bson.M{
		"arr": bson.A{
			bson.M{"_elem_": "val"},
			"regular-string",
		},
	}
	bsonAOutput := mustTransform(t, bsonAInput, log, "db", "coll", "id")

	if diff := cmp.Diff(bsonAExpected, bsonAOutput); diff != "" {
		t.Errorf("\n[FAIL] bson.A Array processing mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformObjectIDPreserved(t *testing.T) {
	log := logger.New()
	oid := primitive.NewObjectID()
	nestedOid := primitive.NewObjectID()

	doc := bson.M{
		"_id":  oid,
		"name": "object_id_preservation",
		"data": bson.M{
			"nested_id": nestedOid,
		},
	}
	expected := bson.M{
		"_id":  oid,
		"name": "object_id_preservation",
		"data": bson.M{
			"nested_id": nestedOid,
		},
	}

	res := mustTransform(t, doc, log, "db", "coll", oid)
	if diff := cmp.Diff(expected, res); diff != "" {
		t.Errorf("\n[FAIL] ObjectID preservation mismatch (-want +got):\n%s", diff)
	}
}


func TestTransformPositiveInfinity(t *testing.T) {
	log := logger.New()
	doc := bson.M{
		"_id":         primitive.NewObjectID(),
		"name":        "positive_infinity_value",
		"description": "A document storing a floating-point Positive Infinity value.",
		"data":        math.Inf(1),
	}
	res := mustTransform(t, doc, log, "db", "coll", "id").(bson.M)
	if res["data"] != math.Inf(1) {
		t.Errorf("expected Positive Infinity to be preserved, got: %v", res["data"])
	}
}

func TestTransformNegativeInfinity(t *testing.T) {
	log := logger.New()
	doc := bson.M{
		"_id":         primitive.NewObjectID(),
		"name":        "negative_infinity_value",
		"description": "A document storing a floating-point Negative Infinity value.",
		"data":        math.Inf(-1),
	}
	res := mustTransform(t, doc, log, "db", "coll", "id").(bson.M)
	if res["data"] != math.Inf(-1) {
		t.Errorf("expected Negative Infinity to be preserved, got: %v", res["data"])
	}
}

func TestTransformNaNValue(t *testing.T) {
	log := logger.New()
	doc := bson.M{
		"_id":         primitive.NewObjectID(),
		"name":        "nan_value",
		"description": "A document storing a floating-point Not-a-Number (NaN) value.",
		"data":        math.NaN(),
	}
	res := mustTransform(t, doc, log, "db", "coll", "id").(bson.M)
	val, ok := res["data"].(float64)
	if !ok || !math.IsNaN(val) {
		t.Errorf("expected NaN to be preserved, got: %v", res["data"])
	}
}

func TestTransformCase1StandardDoubleUnderscoredKey(t *testing.T) {
	log := logger.New()
	doc := bson.M{
		"name":        "Case 1: Standard double-underscored key",
		"description": "A standard key matching the '__my_key__' format. Expected rename: '__my_key'",
		"data": bson.M{
			"__my_key__": "value_1",
		},
	}
	expected := bson.M{
		"name":        "Case 1: Standard double-underscored key",
		"description": "A standard key matching the '__my_key__' format. Expected rename: '__my_key'",
		"data": bson.M{
			"_my_key_": "value_1",
		},
	}
	res := mustTransform(t, doc, log, "db", "coll", "id")
	if diff := cmp.Diff(expected, res); diff != "" {
		t.Errorf("\n[FAIL] Case 1 mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformCase2StandardRegularKey(t *testing.T) {
	log := logger.New()
	doc := bson.M{
		"name":        "Case 2: Standard regular key (No transform)",
		"description": "A clean regular string key without underscores. Expected: Unchanged ('my_key')",
		"data": bson.M{
			"my_key": "value_2",
		},
	}
	expected := bson.M{
		"name":        "Case 2: Standard regular key (No transform)",
		"description": "A clean regular string key without underscores. Expected: Unchanged ('my_key')",
		"data": bson.M{
			"my_key": "value_2",
		},
	}
	res := mustTransform(t, doc, log, "db", "coll", "id")
	if diff := cmp.Diff(expected, res); diff != "" {
		t.Errorf("\n[FAIL] Case 2 mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformCase3NestedObjectWithMatchingKeys(t *testing.T) {
	log := logger.New()
	doc := bson.M{
		"name":        "Case 3: Nested object with matching keys",
		"description": "Recursively matches keys at multiple object levels. Expected: Both renamed to '__outer' and '__inner'",
		"data": bson.M{
			"__outer__": bson.M{
				"__inner__": "nested_value",
			},
		},
	}
	expected := bson.M{
		"name":        "Case 3: Nested object with matching keys",
		"description": "Recursively matches keys at multiple object levels. Expected: Both renamed to '__outer' and '__inner'",
		"data": bson.M{
			"_outer_": bson.M{
				"_inner_": "nested_value",
			},
		},
	}
	res := mustTransform(t, doc, log, "db", "coll", "id")
	if diff := cmp.Diff(expected, res); diff != "" {
		t.Errorf("\n[FAIL] Case 3 mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformCase4MixedRegularAndDoubleUnderscoredKeys(t *testing.T) {
	log := logger.New()
	doc := bson.M{
		"name":        "Case 4: Mixed regular and double-underscored keys",
		"description": "Ensures regular keys are left untouched while double-underscored keys in the same object are renamed.",
		"data": bson.M{
			"__my_key__":  "will_rename",
			"regular_key": "will_not_change",
		},
	}
	expected := bson.M{
		"name":        "Case 4: Mixed regular and double-underscored keys",
		"description": "Ensures regular keys are left untouched while double-underscored keys in the same object are renamed.",
		"data": bson.M{
			"_my_key_":    "will_rename",
			"regular_key": "will_not_change",
		},
	}
	res := mustTransform(t, doc, log, "db", "coll", "id")
	if diff := cmp.Diff(expected, res); diff != "" {
		t.Errorf("\n[FAIL] Case 4 mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformCase5ArrayContainingMatchingObjects(t *testing.T) {
	log := logger.New()
	doc := bson.M{
		"name":        "Case 5: Array containing matching objects",
		"description": "Recurses into array elements to transform keys. Expected: Both '__id__' keys renamed to '__id'",
		"data": bson.M{
			"items": []interface{}{
				bson.M{"__id__": 1, "name": "item_a"},
				bson.M{"__id__": 2, "name": "item_b"},
			},
		},
	}
	expected := bson.M{
		"name":        "Case 5: Array containing matching objects",
		"description": "Recurses into array elements to transform keys. Expected: Both '__id__' keys renamed to '__id'",
		"data": bson.M{
			"items": []interface{}{
				bson.M{"_id_": 1, "name": "item_a"},
				bson.M{"_id_": 2, "name": "item_b"},
			},
		},
	}
	res := mustTransform(t, doc, log, "db", "coll", "id")
	if diff := cmp.Diff(expected, res); diff != "" {
		t.Errorf("\n[FAIL] Case 5 mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformCase6DeeplyNestedMapWithMixedKeyStyles(t *testing.T) {
	log := logger.New()
	doc := bson.M{
		"name":        "Case 6: Deeply nested map with mixed key styles",
		"description": "A deep object graph mixing regular and double-underscored structural keys.",
		"data": bson.M{
			"__root__": bson.M{
				"regular_level": bson.M{
					"__leaf__": "deep_value",
				},
			},
		},
	}
	expected := bson.M{
		"name":        "Case 6: Deeply nested map with mixed key styles",
		"description": "A deep object graph mixing regular and double-underscored structural keys.",
		"data": bson.M{
			"_root_": bson.M{
				"regular_level": bson.M{
					"_leaf_": "deep_value",
				},
			},
		},
	}
	res := mustTransform(t, doc, log, "db", "coll", "id")
	if diff := cmp.Diff(expected, res); diff != "" {
		t.Errorf("\n[FAIL] Case 6 mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformCase7MixedTypesInsideAnArray(t *testing.T) {
	log := logger.New()
	doc := bson.M{
		"name":        "Case 7: Mixed types inside an array",
		"description": "Ensures non-object types inside arrays (strings, numbers) are bypassed safely.",
		"data": bson.M{
			"__list__": []interface{}{
				"plain_string",
				123,
				bson.M{"__nested_key__": "value_7"},
			},
		},
	}
	expected := bson.M{
		"name":        "Case 7: Mixed types inside an array",
		"description": "Ensures non-object types inside arrays (strings, numbers) are bypassed safely.",
		"data": bson.M{
			"_list_": []interface{}{
				"plain_string",
				123,
				bson.M{"_nested_key_": "value_7"},
			},
		},
	}
	res := mustTransform(t, doc, log, "db", "coll", "id")
	if diff := cmp.Diff(expected, res); diff != "" {
		t.Errorf("\n[FAIL] Case 7 mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformCase8NegativeTestLeadingUnderscoresOnly(t *testing.T) {
	log := logger.New()
	doc := bson.M{
		"name":        "Case 8: Negative Test - Leading underscores only",
		"description": "A key starting with '__' but not ending with it. Expected: Unchanged ('__left_only')",
		"data": bson.M{
			"__left_only": "value_8",
		},
	}
	expected := bson.M{
		"name":        "Case 8: Negative Test - Leading underscores only",
		"description": "A key starting with '__' but not ending with it. Expected: Unchanged ('__left_only')",
		"data": bson.M{
			"__left_only": "value_8",
		},
	}
	res := mustTransform(t, doc, log, "db", "coll", "id")
	if diff := cmp.Diff(expected, res); diff != "" {
		t.Errorf("\n[FAIL] Case 8 mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformCase9NegativeTestTrailingUnderscoresOnly(t *testing.T) {
	log := logger.New()
	doc := bson.M{
		"name":        "Case 9: Negative Test - Trailing underscores only",
		"description": "A key ending with '__' but not starting with it. Expected: Unchanged ('right_only__')",
		"data": bson.M{
			"right_only__": "value_9",
		},
	}
	expected := bson.M{
		"name":        "Case 9: Negative Test - Trailing underscores only",
		"description": "A key ending with '__' but not starting with it. Expected: Unchanged ('right_only__')",
		"data": bson.M{
			"right_only__": "value_9",
		},
	}
	res := mustTransform(t, doc, log, "db", "coll", "id")
	if diff := cmp.Diff(expected, res); diff != "" {
		t.Errorf("\n[FAIL] Case 9 mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformCase10EmptyAndNullValuesWithMatchingKeys(t *testing.T) {
	log := logger.New()
	doc := bson.M{
		"name":        "Case 10: Empty and null values with matching keys",
		"description": "Ensures the recursive step doesn't crash on null properties or empty objects.",
		"data": bson.M{
			"__null_key__":     nil,
			"__empty_object__": bson.M{},
		},
	}
	expected := bson.M{
		"name":        "Case 10: Empty and null values with matching keys",
		"description": "Ensures the recursive step doesn't crash on null properties or empty objects.",
		"data": bson.M{
			"_null_key_":     nil,
			"_empty_object_": bson.M{},
		},
	}
	res := mustTransform(t, doc, log, "db", "coll", "id")
	if diff := cmp.Diff(expected, res); diff != "" {
		t.Errorf("\n[FAIL] Case 10 mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformEmptyFieldKeys(t *testing.T) {
	log := logger.New()

	// 1. bson.M: empty key should be omitted
	docM := bson.M{
		"":     "empty-key-value",
		"keep": "keep-value",
	}
	expectedM := bson.M{
		"keep": "keep-value",
	}
	resM := mustTransform(t, docM, log, "db", "coll", "id")
	if diff := cmp.Diff(expectedM, resM); diff != "" {
		t.Errorf("expected empty key in bson.M to be removed, mismatch (-want +got):\n%s", diff)
	}

	// 2. bson.D: empty key should be omitted
	docD := bson.D{
		{Key: "", Value: "empty-key-val"},
		{Key: "keep", Value: "keep-val"},
	}
	expectedD := bson.D{
		{Key: "keep", Value: "keep-val"},
	}
	resD := mustTransform(t, docD, log, "db", "coll", "id")
	if diff := cmp.Diff(expectedD, resD); diff != "" {
		t.Errorf("expected empty key in bson.D to be removed, mismatch (-want +got):\n%s", diff)
	}

	// 3. map[string]interface{}: empty key should be omitted
	docMap := map[string]interface{}{
		"":     "empty-map-val",
		"keep": "keep-map-val",
	}
	expectedMap := map[string]interface{}{
		"keep": "keep-map-val",
	}
	resMap := mustTransform(t, docMap, log, "db", "coll", "id")
	if diff := cmp.Diff(expectedMap, resMap); diff != "" {
		t.Errorf("expected empty key in map to be removed, mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformExtractDocID(t *testing.T) {
	log := logger.New()

	// 1. bson.D with _id key
	docD := bson.D{
		{Key: "_id", Value: "d_id"},
		{Key: "other", Value: "value"},
	}
	batchD := []interface{}{docD}
	resD := mustTransformBatch(t, batchD, log, "db", "coll")
	if len(resD) != 1 {
		t.Fatal("TransformBatch failed")
	}

	// 2. bson.M with _id key
	docM := bson.M{
		"_id": "m_id",
	}
	batchM := []interface{}{docM}
	resM := mustTransformBatch(t, batchM, log, "db", "coll")
	if len(resM) != 1 {
		t.Fatal("TransformBatch failed")
	}

	// 3. map[string]interface{} with _id key
	docMap := map[string]interface{}{
		"_id": "map_id",
	}
	batchMap := []interface{}{docMap}
	resMap := mustTransformBatch(t, batchMap, log, "db", "coll")
	if len(resMap) != 1 {
		t.Fatal("TransformBatch failed")
	}

	// 4. Unsupported type or no _id
	batchUnsupported := []interface{}{
		"string_type",
		bson.D{{Key: "no_id", Value: "val"}},
	}
	resUnsupported := mustTransformBatch(t, batchUnsupported, log, "db", "coll")
	if len(resUnsupported) != 2 {
		t.Fatal("TransformBatch failed")
	}
}

func TestTransformCollisionOfRenamedFields(t *testing.T) {
	log := logger.New()

	// 1. bson.M collision: __name__ renames to _name_, which collides with existing _name_
	docM := bson.M{
		"__name__": "alice",
		"_name_":   "bob",
	}
	_, err := TransformFieldNames(docM, log, "db", "coll", "id")
	if err == nil {
		t.Error("expected collision error for bson.M, got nil")
	} else if !strings.Contains(err.Error(), "collision detected") {
		t.Errorf("expected collision detected error message, got: %v", err)
	}

	// 2. bson.D collision: duplicate keys now explicitly trigger collision errors
	docD := bson.D{
		{Key: "__name__", Value: "alice"},
		{Key: "_name_", Value: "bob"},
	}
	_, err = TransformFieldNames(docD, log, "db", "coll", "id")
	if err == nil {
		t.Error("expected collision error for bson.D, got nil")
	} else if !strings.Contains(err.Error(), "collision detected") {
		t.Errorf("expected collision detected error message, got: %v", err)
	}
}

func TestTransformUnderscoreCollapseCollision(t *testing.T) {
	log := logger.New()

	// Collapsing '___' and '__' should both result in a single underscore '_'
	docM := bson.M{
		"___": "value_three",
		"__":  "value_two",
	}
	_, err := TransformFieldNames(docM, log, "db", "coll", "id")
	if err == nil {
		t.Error("expected collapse collision error, got nil")
	} else if !strings.Contains(err.Error(), "collision detected") {
		t.Errorf("expected collision detected error message, got: %v", err)
	}
}

func TestTransformStringifyUnsupportedJSONValue(t *testing.T) {
	log := logger.New()
	longKey := strings.Repeat("a", 1001)

	unsupportedChan := make(chan int)

	doc := bson.M{
		"nested": bson.M{
			longKey:       "long-val",
			"invalid_val": unsupportedChan,
		},
	}

	_, err := TransformFieldNames(doc, log, "db", "coll", "id")
	if err == nil {
		t.Error("expected error when stringifying unsupported JSON value in nested object with long keys, got nil")
	} else if !strings.Contains(err.Error(), "failed to stringify") {
		t.Errorf("expected failed to stringify error, got: %v", err)
	}
}
