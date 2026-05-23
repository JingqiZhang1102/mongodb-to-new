package migration

import (
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// ExtractEventTime extracts the event timestamp from clusterTime or wallTime fields in a BSON change event
func ExtractEventTime(event bson.M) time.Time {
	if ct, ok := event["clusterTime"].(primitive.Timestamp); ok {
		return time.Unix(int64(ct.T), 0)
	}
	if wt, ok := event["wallTime"].(primitive.DateTime); ok {
		return wt.Time()
	}
	if ctVal, exists := event["clusterTime"]; exists {
		if ctValTS, ok := ctVal.(primitive.Timestamp); ok {
			return time.Unix(int64(ctValTS.T), 0)
		}
	}
	return time.Time{}
}
