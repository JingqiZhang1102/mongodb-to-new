package migration

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestExtractEventTime(t *testing.T) {
	nowSec := time.Now().Unix()
	expectedTime := time.Unix(nowSec, 0)

	tests := []struct {
		name     string
		event    bson.M
		wantZero bool
	}{
		{
			name: "primitive.Timestamp clusterTime",
			event: bson.M{
				"clusterTime": primitive.Timestamp{T: uint32(nowSec), I: 1},
			},
			wantZero: false,
		},
		{
			name: "primitive.DateTime wallTime",
			event: bson.M{
				"wallTime": primitive.NewDateTimeFromTime(expectedTime),
			},
			wantZero: false,
		},
		{
			name: "nested/interface primitive.Timestamp clusterTime",
			event: bson.M{
				"clusterTime": interface{}(primitive.Timestamp{T: uint32(nowSec), I: 1}),
			},
			wantZero: false,
		},
		{
			name:     "empty event",
			event:    bson.M{},
			wantZero: true,
		},
		{
			name: "invalid types",
			event: bson.M{
				"clusterTime": "not-a-timestamp",
				"wallTime":    12345,
			},
			wantZero: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractEventTime(tt.event)
			if tt.wantZero {
				if !got.IsZero() {
					t.Errorf("ExtractEventTime() = %v, want zero time", got)
				}
			} else {
				if got.IsZero() {
					t.Fatalf("ExtractEventTime() returned zero time, want %v", expectedTime)
				}
				if got.Unix() != nowSec {
					t.Errorf("ExtractEventTime() = %v, want %v", got, expectedTime)
				}
			}
		})
	}
}
