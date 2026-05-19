package migration

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGetResumeTokenPath(t *testing.T) {
	expected := "resumeToken-testDB-testColl.json"
	got := GetResumeTokenPath("testDB", "testColl")
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestResumeTokenLifecycle(t *testing.T) {
	tmpDir := t.TempDir()
	tokenPath := filepath.Join(tmpDir, "resume-token.json")

	// 1. Load non-existent token (should create an empty file and return nil)
	token, err := LoadResumeToken(tokenPath)
	if err != nil {
		t.Fatalf("unexpected error loading non-existent token: %v", err)
	}
	if token != nil {
		t.Errorf("expected loaded token to be nil, got %v", token)
	}

	// 2. Save and load string token
	err = SaveResumeToken(tokenPath, "my_simple_token_data")
	if err != nil {
		t.Fatalf("failed to save string token: %v", err)
	}

	token, err = LoadResumeToken(tokenPath)
	if err != nil {
		t.Fatalf("failed to load saved string token: %v", err)
	}
	m, ok := token.(map[string]interface{})
	if !ok {
		t.Fatalf("expected loaded token to be map, got %T", token)
	}
	if m["_data"] != "my_simple_token_data" {
		t.Errorf("expected _data to be %q, got %q", "my_simple_token_data", m["_data"])
	}

	// 3. Save and load map token
	mapToken := map[string]interface{}{
		"_data": "map_token_data_123",
	}
	err = SaveResumeToken(tokenPath, mapToken)
	if err != nil {
		t.Fatalf("failed to save map token: %v", err)
	}

	token, err = LoadResumeToken(tokenPath)
	if err != nil {
		t.Fatalf("failed to load map token: %v", err)
	}
	m, ok = token.(map[string]interface{})
	if !ok {
		t.Fatalf("expected loaded token to be map, got %T", token)
	}
	if m["_data"] != "map_token_data_123" {
		t.Errorf("expected _data to be %q, got %q", "map_token_data_123", m["_data"])
	}

	// 4. Save and load string with prefix format
	err = SaveResumeToken(tokenPath, "map[_data:prefix_format_data]")
	if err != nil {
		t.Fatalf("failed to save prefixed token: %v", err)
	}

	token, err = LoadResumeToken(tokenPath)
	if err != nil {
		t.Fatalf("failed to load prefixed token: %v", err)
	}
	m, ok = token.(map[string]interface{})
	if !ok {
		t.Fatalf("expected loaded token to be map, got %T", token)
	}
	if m["_data"] != "prefix_format_data" {
		t.Errorf("expected _data to be %q, got %q", "prefix_format_data", m["_data"])
	}
}

func TestResumeTokenBackupAndRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	tokenPath := filepath.Join(tmpDir, "resume-backup.json")

	// 1. Save tokens repeatedly to trigger backup rotation (1 to 11)
	for i := 1; i <= 12; i++ {
		err := SaveResumeToken(tokenPath, map[string]interface{}{
			"_data": string(rune('A' + i)), // A, B, C...
		})
		if err != nil {
			t.Fatalf("failed to save token iteration %d: %v", i, err)
		}
	}

	// Verify backups .1 to .10 exist
	for i := 1; i <= 10; i++ {
		var backupPath string
		if i == 10 {
			backupPath = tokenPath + ".10"
		} else {
			backupPath = tokenPath + "." + string(rune('0'+i))
		}
		if _, err := os.Stat(backupPath); os.IsNotExist(err) {
			t.Errorf("expected backup %s to exist", backupPath)
		}
	}

	// 2. Corrupt main token file
	err := os.WriteFile(tokenPath, []byte("invalid_json"), 0644)
	if err != nil {
		t.Fatalf("failed to corrupt token file: %v", err)
	}

	// 3. Load token (should automatically recover from backup .1)
	token, err := LoadResumeToken(tokenPath)
	if err != nil {
		t.Fatalf("failed to load and restore backup token: %v", err)
	}
	m, ok := token.(map[string]interface{})
	if !ok {
		t.Fatalf("expected loaded token to be map, got %T", token)
	}
	// The most recent backup (.1) should correspond to the 11th write (iteration 11)
	// iteration 12 is the corrupt main file
	if m["_data"] == "" {
		t.Errorf("expected non-empty recovered _data")
	}

	// 4. Delete all tokens
	err = DeleteResumeToken(tokenPath)
	if err != nil {
		t.Fatalf("failed to delete token and backups: %v", err)
	}

	// Confirm clean deletion of everything
	if _, err := os.Stat(tokenPath); !os.IsNotExist(err) {
		t.Errorf("expected main token file to be deleted")
	}
	for i := 1; i <= 10; i++ {
		backupPath := tokenPath + "." + string(rune('0'+i))
		if i == 10 {
			backupPath = tokenPath + ".10"
		}
		if _, err := os.Stat(backupPath); !os.IsNotExist(err) {
			t.Errorf("expected backup file %s to be deleted", backupPath)
		}
	}
}
