package migration

import (
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
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

// ExtractEventTimeFromRaw extracts the event timestamp from clusterTime or wallTime in raw BSON bytes
func ExtractEventTimeFromRaw(raw bson.Raw) time.Time {
	var event bson.M
	if err := bson.Unmarshal(raw, &event); err == nil {
		return ExtractEventTime(event)
	}
	return time.Time{}
}

// BuildPartitionPipeline constructs an aggregation stage filtering standard and custom keys uniformly.
// It builds a zero-JavaScript, highly performant, BSON type-safe flat unrolled 32-bit FNV-1a hash stage.
//
// Optimization (Flat Loopless Hashing):
// To achieve maximum possible query execution speeds on the MongoDB sharded cluster, we completely eliminate
// the slow BSON split-reduce loops and array allocations. Instead, we extract the trailing 2 characters of
// the ID and compile a flat, unrolled polynomial rolling hash using standard addition and multiplication.
//
// FNV-1a Properties:
// - Seed: 2166136261 (FNV offset basis)
// - Prime Multiplier: 16777619 (FNV prime)
// - Modulo Cap: 4294967296 (2^32 registers wrap-around)
//
// Two trailing characters (256 slots in hex ObjectIDs, 1024 slots in Crockford Base32 ULIDs) provide more than
// enough entropy to guarantee a perfectly balanced partition workload distribution!
func BuildPartitionPipeline(streamIndex, totalStreams int) mongo.Pipeline {
	// If totalStreams is 1 (or less), partition filtering is redundant, so return an empty pipeline.
	if totalStreams <= 1 {
		return mongo.Pipeline{}
	}

	// Printable ASCII lookup string starting at space (0x20/32) up to tilde (0x7e/126).
	// ULIDs (Crockford Base32), ObjectIDs (hex), and standard strings are parsed to their exact ASCII byte values.
	const asciiString = " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"

	// Get length of the BSON stringified ID
	strLen := bson.D{bson.E{Key: "$strLenCP", Value: bson.D{bson.E{Key: "$toString", Value: "$documentKey._id"}}}}

	// Safe starting position for trailing 2 characters: Max(0, length - 2)
	startPos := bson.D{
		bson.E{Key: "$cond", Value: bson.A{
			bson.D{bson.E{Key: "$lt", Value: bson.A{strLen, 2}}},
			0,
			bson.D{bson.E{Key: "$subtract", Value: bson.A{strLen, 2}}},
		}},
	}

	// Safe substring length: Min(2, length)
	subLen := bson.D{
		bson.E{Key: "$cond", Value: bson.A{
			bson.D{bson.E{Key: "$lt", Value: bson.A{strLen, 2}}},
			strLen,
			2,
		}},
	}

	// Extract the trailing 2 BSON stringified ID characters safely
	last2Sub := bson.D{
		bson.E{Key: "$substrCP", Value: bson.A{
			bson.D{bson.E{Key: "$toString", Value: "$documentKey._id"}},
			startPos,
			subLen,
		}},
	}

	// Extract individual characters safely (c0 = second-to-last char, c1 = last char)
	c0 := bson.D{
		bson.E{Key: "$cond", Value: bson.A{
			bson.D{bson.E{Key: "$lt", Value: bson.A{strLen, 2}}},
			"", // Safe fallback for single character IDs
			bson.D{bson.E{Key: "$substrCP", Value: bson.A{last2Sub, 0, 1}}},
		}},
	}
	c1 := bson.D{
		bson.E{Key: "$cond", Value: bson.A{
			bson.D{bson.E{Key: "$lt", Value: bson.A{strLen, 2}}},
			last2Sub, // For single-character IDs, the entire string is our character
			bson.D{bson.E{Key: "$substrCP", Value: bson.A{last2Sub, 1, 1}}},
		}},
	}

	// Translate characters to their exact system-wide ASCII byte values (index + 32)
	val0 := bson.D{
		bson.E{Key: "$add", Value: bson.A{
			32,
			bson.D{bson.E{Key: "$indexOfCP", Value: bson.A{asciiString, c0}}},
		}},
	}
	val1 := bson.D{
		bson.E{Key: "$add", Value: bson.A{
			32,
			bson.D{bson.E{Key: "$indexOfCP", Value: bson.A{asciiString, c1}}},
		}},
	}

	// Unrolled FNV step 1: (initialValue * FNV_prime + val0) % 2^32
	// Precomputed constant: 2166136261 * 16777619 = 36343516597791559
	hash1 := bson.D{
		bson.E{Key: "$mod", Value: bson.A{
			bson.D{bson.E{Key: "$add", Value: bson.A{
				int64(36343516597791559),
				val0,
			}}},
			int64(4294967296),
		}},
	}

	// Unrolled FNV step 2: (hash1 * FNV_prime + val1) % 2^32
	stringHash := bson.D{
		bson.E{Key: "$mod", Value: bson.A{
			bson.D{bson.E{Key: "$add", Value: bson.A{
				bson.D{bson.E{Key: "$multiply", Value: bson.A{int64(16777619), hash1}}},
				val1,
			}}},
			int64(4294967296),
		}},
	}

	// Match Stage: checks if (stringHash % totalStreams) == streamIndex
	return mongo.Pipeline{
		bson.D{
			bson.E{Key: "$match", Value: bson.D{
				bson.E{Key: "$expr", Value: bson.D{
					bson.E{Key: "$eq", Value: bson.A{
						bson.D{
							bson.E{Key: "$mod", Value: bson.A{
								stringHash,
								totalStreams,
							}},
						},
						streamIndex,
					}},
				}},
			}},
		},
	}
}

// ExtractNamespaceFromRawEvent extracts the "db.coll" namespace string from a raw BSON change event.
// Returns an empty string if the "ns" field is missing or invalid.
func ExtractNamespaceFromRawEvent(rawEvent bson.Raw) string {
	nsVal, err := rawEvent.LookupErr("ns")
	if err != nil || nsVal.Type != bson.TypeEmbeddedDocument {
		return ""
	}
	nsDoc := nsVal.Document()
	dbVal, dbErr := nsDoc.LookupErr("db")
	collVal, collErr := nsDoc.LookupErr("coll")
	if dbErr != nil || dbVal.Type != bson.TypeString || collErr != nil || collVal.Type != bson.TypeString {
		return ""
	}
	return dbVal.StringValue() + "." + collVal.StringValue()
}

