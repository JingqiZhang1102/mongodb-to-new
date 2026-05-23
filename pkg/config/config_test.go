package config

import (
	"os"
	"testing"
)

func TestGetMaxWorkersForLive(t *testing.T) {
	cfg := &Config{
		IncrementalWorkerCount:  8,
		ConcurrentCollections:   4,
		InitialMigrationWorkers: 8,
	}

	// Under live-only mode, it should strictly return c.IncrementalWorkerCount (8)
	if maxWorkers := cfg.GetMaxWorkersForLive("live-only"); maxWorkers != 8 {
		t.Errorf("Expected GetMaxWorkersForLive(\"live-only\") to be 8, got %d", maxWorkers)
	}

	// Under migrate mode, it should return the max of IncrementalWorkerCount (8) and ConcurrentCollections * InitialMigrationWorkers (4 * 8 = 32)
	if maxWorkers := cfg.GetMaxWorkersForLive("migrate"); maxWorkers != 32 {
		t.Errorf("Expected GetMaxWorkersForLive(\"migrate\") to be 32, got %d", maxWorkers)
	}

	// Under live mode, it should also return 32
	if maxWorkers := cfg.GetMaxWorkersForLive("live"); maxWorkers != 32 {
		t.Errorf("Expected GetMaxWorkersForLive(\"live\") to be 32, got %d", maxWorkers)
	}

	// Edge case: if IncrementalWorkerCount is larger than initial migration peak
	cfg2 := &Config{
		IncrementalWorkerCount:  64,
		ConcurrentCollections:   4,
		InitialMigrationWorkers: 8,
	}

	// Should return IncrementalWorkerCount (64) because it is larger than 4 * 8 = 32
	if maxWorkers := cfg2.GetMaxWorkersForLive("live"); maxWorkers != 64 {
		t.Errorf("Expected GetMaxWorkersForLive(\"live\") with large IncrementalWorkerCount to be 64, got %d", maxWorkers)
	}
}

func TestLoadConfigStrict(t *testing.T) {
	// Create a temporary file helper
	writeTempFile := func(t *testing.T, content string) string {
		tmpFile, err := os.CreateTemp("", "config_test_*.json")
		if err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}
		defer tmpFile.Close()
		if _, err := tmpFile.WriteString(content); err != nil {
			t.Fatalf("Failed to write temp file: %v", err)
		}
		return tmpFile.Name()
	}

	// 1. Test a perfectly valid config file
	validJSON := `{
		"databasePairs": [
			{
				"source": {
					"connectionString": "mongodb://localhost:27017",
					"database": "source_db"
				},
				"target": {
					"connectionString": "mongodb://localhost:27018",
					"database": "target_db"
				}
			}
		],
		"saveThreshold": 150,
		"checkpointIntervalMinutes": 10
	}`
	validFile := writeTempFile(t, validJSON)
	defer os.Remove(validFile)

	cfg, err := LoadConfig(validFile)
	if err != nil {
		t.Errorf("Expected valid config to load successfully, got error: %v", err)
	}
	if cfg.SaveThreshold != 150 {
		t.Errorf("Expected SaveThreshold to be 150, got %d", cfg.SaveThreshold)
	}
	if cfg.CheckpointIntervalMinutes != 10 {
		t.Errorf("Expected CheckpointIntervalMinutes to be 10, got %d", cfg.CheckpointIntervalMinutes)
	}

	// 2. Test an invalid config file containing unrecognized keys
	invalidJSON := `{
		"databasePairs": [
			{
				"source": {
					"connectionString": "mongodb://localhost:27017",
					"database": "source_db"
				},
				"target": {
					"connectionString": "mongodb://localhost:27018",
					"database": "target_db"
				}
			}
		],
		"unrecognizedFieldKey": "oops",
		"saveThreshold": 150
	}`
	invalidFile := writeTempFile(t, invalidJSON)
	defer os.Remove(invalidFile)

	_, err = LoadConfig(invalidFile)
	if err == nil {
		t.Error("Expected LoadConfig to fail with unrecognized field key 'unrecognizedFieldKey', but it succeeded without errors")
	}
}
