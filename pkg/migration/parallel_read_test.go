package migration

import (
	"fmt"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func TestBuildPartitionFilters_StringIDs(t *testing.T) {
	sampledIDs := make([]interface{}, 1000)
	for i := 0; i < 1000; i++ {
		sampledIDs[i] = fmt.Sprintf("id_%04d", i)
	}

	partitionCount := 8
	filters := buildPartitionFilters(sampledIDs, partitionCount)

	if len(filters) != partitionCount {
		t.Fatalf("expected %d filters, got %d", partitionCount, len(filters))
	}

	// step = 1000 / 8 = 125
	// Boundaries should be at: 125, 250, 375, 500, 625, 750, 875

	expectedEnd0 := "id_0125"
	filter0 := filters[0]
	val0, err := getBSONValue(filter0, "_id", "$lt")
	if err != nil {
		t.Fatalf("failed to parse filter 0: %v", err)
	}
	if val0 != expectedEnd0 {
		t.Errorf("expected first partition upper bound %q, got %q", expectedEnd0, val0)
	}

	expectedStart1 := "id_0125"
	expectedEnd1 := "id_0250"
	filter1 := filters[1]
	start1, err := getBSONValue(filter1, "_id", "$gte")
	if err != nil {
		t.Fatalf("failed to parse start of filter 1: %v", err)
	}
	end1, err := getBSONValue(filter1, "_id", "$lt")
	if err != nil {
		t.Fatalf("failed to parse end of filter 1: %v", err)
	}
	if start1 != expectedStart1 || end1 != expectedEnd1 {
		t.Errorf("expected partition 1 bounds [%q, %q), got [%q, %q)", expectedStart1, expectedEnd1, start1, end1)
	}

	expectedStart7 := "id_0875"
	filter7 := filters[7]
	start7, err := getBSONValue(filter7, "_id", "$gte")
	if err != nil {
		t.Fatalf("failed to parse filter 7: %v", err)
	}
	if start7 != expectedStart7 {
		t.Errorf("expected last partition lower bound %q, got %q", expectedStart7, start7)
	}
}

func TestBuildPartitionFilters_NumericIDs(t *testing.T) {
	sampledIDs := make([]interface{}, 12)
	for i := 0; i < 12; i++ {
		sampledIDs[i] = i * 10
	}

	partitionCount := 3
	filters := buildPartitionFilters(sampledIDs, partitionCount)

	if len(filters) != partitionCount {
		t.Fatalf("expected %d filters, got %d", partitionCount, len(filters))
	}

	// step = 12 / 3 = 4
	// Boundaries should be at: sampledIDs[4] (40), sampledIDs[8] (80)

	// Partition 0: < 40
	filter0 := filters[0]
	val0, err := getBSONValue(filter0, "_id", "$lt")
	if err != nil {
		t.Fatal(err)
	}
	if val0 != 40 {
		t.Errorf("expected partition 0 upper bound 40, got %v", val0)
	}

	// Partition 1: [40, 80)
	filter1 := filters[1]
	start1, err := getBSONValue(filter1, "_id", "$gte")
	if err != nil {
		t.Fatal(err)
	}
	end1, err := getBSONValue(filter1, "_id", "$lt")
	if err != nil {
		t.Fatal(err)
	}
	if start1 != 40 || end1 != 80 {
		t.Errorf("expected partition 1 bounds [40, 80), got [%v, %v)", start1, end1)
	}

	// Partition 2: >= 80
	filter2 := filters[2]
	start2, err := getBSONValue(filter2, "_id", "$gte")
	if err != nil {
		t.Fatal(err)
	}
	if start2 != 80 {
		t.Errorf("expected partition 2 lower bound 80, got %v", start2)
	}
}

// Helper helper to extract value from BSON filter
func getBSONValue(filter bson.D, key, op string) (interface{}, error) {
	for _, elem := range filter {
		if elem.Key == key {
			subD, ok := elem.Value.(bson.D)
			if !ok {
				return nil, fmt.Errorf("value for key %q is not bson.D", key)
			}
			for _, subElem := range subD {
				if subElem.Key == op {
					return subElem.Value, nil
				}
			}
			return nil, fmt.Errorf("operator %q not found inside key %q", op, key)
		}
	}
	return nil, fmt.Errorf("key %q not found in filter", key)
}
