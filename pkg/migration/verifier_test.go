package migration

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestCompareDocuments_Matches(t *testing.T) {
	nowTime := time.Now().Truncate(time.Millisecond)

	tests := []struct {
		name      string
		sourceDoc interface{}
		targetDoc interface{}
	}{
		{
			name: "exact same documents (bson.D)",
			sourceDoc: bson.D{
				{Key: "name", Value: "test"},
				{Key: "val", Value: int32(42)},
			},
			targetDoc: bson.D{
				{Key: "name", Value: "test"},
				{Key: "val", Value: int32(42)},
			},
		},
		{
			name: "numerical type equivalencies (int32 vs int64 vs float64)",
			sourceDoc: bson.D{
				{Key: "int_to_float", Value: int32(42)},
				{Key: "int_to_int64", Value: int32(100)},
				{Key: "float_to_int64", Value: float64(500.0)},
			},
			targetDoc: bson.D{
				{Key: "int_to_float", Value: float64(42.0)},
				{Key: "int_to_int64", Value: int64(100)},
				{Key: "float_to_int64", Value: int64(500)},
			},
		},
		{
			name: "nested maps and array matching",
			sourceDoc: bson.D{
				{Key: "tags", Value: bson.A{"a", "b", int32(3)}},
				{Key: "address", Value: bson.D{
					{Key: "city", Value: "NYC"},
					{Key: "zip", Value: int64(10001)},
				}},
			},
			targetDoc: bson.D{
				{Key: "tags", Value: []interface{}{"a", "b", float64(3.0)}},
				{Key: "address", Value: bson.M{
					"city": "NYC",
					"zip":  int32(10001),
				}},
			},
		},
		{
			name: "date formats matching",
			sourceDoc: bson.D{
				{Key: "created_at", Value: primitive.NewDateTimeFromTime(nowTime)},
			},
			targetDoc: bson.D{
				{Key: "created_at", Value: nowTime},
			},
		},
		{
			name: "binary/byte slice matching",
			sourceDoc: bson.D{
				{Key: "data", Value: primitive.Binary{Data: []byte{1, 2, 3}}},
			},
			targetDoc: bson.D{
				{Key: "data", Value: []byte{1, 2, 3}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matches, diff := CompareDocuments(tt.sourceDoc, tt.targetDoc)
			if !matches {
				t.Errorf("CompareDocuments() unexpectedly returned mismatch: %s", diff)
			}
		})
	}
}

func TestCompareDocuments_Mismatches(t *testing.T) {
	tests := []struct {
		name      string
		sourceDoc interface{}
		targetDoc interface{}
		wantDiff  string
	}{
		{
			name: "simple value mismatch",
			sourceDoc: bson.D{
				{Key: "name", Value: "alice"},
			},
			targetDoc: bson.D{
				{Key: "name", Value: "bob"},
			},
			wantDiff: "Field 'name' mismatch: expected alice (string), got bob (string)",
		},
		{
			name: "type mismatch (non-numeric)",
			sourceDoc: bson.D{
				{Key: "name", Value: "123"},
			},
			targetDoc: bson.D{
				{Key: "name", Value: int32(123)},
			},
			wantDiff: "Field 'name' mismatch: expected 123 (string), got 123 (int32)",
		},
		{
			name: "missing field in target",
			sourceDoc: bson.D{
				{Key: "name", Value: "alice"},
				{Key: "age", Value: int32(30)},
			},
			targetDoc: bson.D{
				{Key: "name", Value: "alice"},
			},
			wantDiff: "Field 'age' missing in target document",
		},
		{
			name: "extra field in target",
			sourceDoc: bson.D{
				{Key: "name", Value: "alice"},
			},
			targetDoc: bson.D{
				{Key: "name", Value: "alice"},
				{Key: "extra", Value: "boom"},
			},
			wantDiff: "Extra field 'extra' in target document",
		},
		{
			name: "nested subdocument value mismatch",
			sourceDoc: bson.D{
				{Key: "address", Value: bson.D{
					{Key: "zip", Value: "10001"},
				}},
			},
			targetDoc: bson.D{
				{Key: "address", Value: bson.D{
					{Key: "zip", Value: "20002"},
				}},
			},
			wantDiff: "Field 'address.zip' mismatch: expected 10001 (string), got 20002 (string)",
		},
		{
			name: "array length mismatch",
			sourceDoc: bson.D{
				{Key: "vals", Value: bson.A{1, 2, 3}},
			},
			targetDoc: bson.D{
				{Key: "vals", Value: bson.A{1, 2}},
			},
			wantDiff: "Field 'vals' array length mismatch: expected 3 elements, got 2",
		},
		{
			name: "array item value mismatch",
			sourceDoc: bson.D{
				{Key: "vals", Value: bson.A{1, 2, 3}},
			},
			targetDoc: bson.D{
				{Key: "vals", Value: bson.A{1, 99, 3}},
			},
			wantDiff: "Field 'vals[1]' numeric mismatch: expected 2, got 99",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matches, diff := CompareDocuments(tt.sourceDoc, tt.targetDoc)
			if matches {
				t.Fatalf("CompareDocuments() unexpectedly returned match, wanted mismatch with diff: %q", tt.wantDiff)
			}
			if diff != tt.wantDiff {
				t.Errorf("CompareDocuments() diff = %q, want %q", diff, tt.wantDiff)
			}
		})
	}
}
