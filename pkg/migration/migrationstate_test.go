package migration

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMigrationStateLifecycle(t *testing.T) {
	tmpDir := t.TempDir()
	stateFilePath := filepath.Join(tmpDir, "initialMigrationState-test.json")

	// 1. Load non-existent state (should return nil, nil)
	state, err := LoadInitialMigrationState(stateFilePath)
	if err != nil {
		t.Fatalf("unexpected error loading non-existent state: %v", err)
	}
	if state != nil {
		t.Errorf("expected state to be nil, got %v", state)
	}

	// 2. Save incomplete (inprogress) state
	err = SaveInitialMigrationState(stateFilePath, StatusInProgress, 0)
	if err != nil {
		t.Fatalf("failed to save inprogress state: %v", err)
	}

	// Load and verify inprogress state
	state, err = LoadInitialMigrationState(stateFilePath)
	if err != nil {
		t.Fatalf("failed to load inprogress state: %v", err)
	}
	if state.IsCompleted() {
		t.Errorf("expected IsCompleted to be false")
	}
	if state.Status != StatusInProgress {
		t.Errorf("expected Status to be StatusInProgress, got %q", state.Status)
	}
	if state.FailedCount != 0 {
		t.Errorf("expected FailedCount to be 0, got %d", state.FailedCount)
	}
	if !state.CompletedAt.IsZero() {
		t.Errorf("expected CompletedAt to be zero for inprogress state")
	}

	// 3. Save completed state without failures
	err = SaveInitialMigrationState(stateFilePath, StatusCompleted, 0)
	if err != nil {
		t.Fatalf("failed to save completed state: %v", err)
	}

	// Load and verify completed state without failures
	state, err = LoadInitialMigrationState(stateFilePath)
	if err != nil {
		t.Fatalf("failed to load completed state: %v", err)
	}
	if !state.IsCompleted() {
		t.Errorf("expected IsCompleted to be true")
	}
	if state.Status != StatusCompleted {
		t.Errorf("expected Status to be StatusCompleted, got %q", state.Status)
	}
	if state.FailedCount != 0 {
		t.Errorf("expected FailedCount to be 0, got %d", state.FailedCount)
	}
	if state.CompletedAt.IsZero() {
		t.Errorf("expected CompletedAt to be non-zero for completed state")
	}

	// 4. Save completed state with failures
	err = SaveInitialMigrationState(stateFilePath, StatusCompletedWithFailures, 42)
	if err != nil {
		t.Fatalf("failed to save completed with failures state: %v", err)
	}

	// Load and verify completed state with failures
	state, err = LoadInitialMigrationState(stateFilePath)
	if err != nil {
		t.Fatalf("failed to load completed with failures state: %v", err)
	}
	if !state.IsCompleted() {
		t.Errorf("expected IsCompleted to be true")
	}
	if state.Status != StatusCompletedWithFailures {
		t.Errorf("expected Status to be StatusCompletedWithFailures, got %q", state.Status)
	}
	if state.FailedCount != 42 {
		t.Errorf("expected FailedCount to be 42, got %d", state.FailedCount)
	}
	if state.CompletedAt.IsZero() {
		t.Errorf("expected CompletedAt to be non-zero for completed state")
	}

	// 5. Delete non-existent file (should not error)
	nonExistentPath := filepath.Join(tmpDir, "does-not-exist.json")
	err = DeleteInitialMigrationState(nonExistentPath)
	if err != nil {
		t.Errorf("unexpected error deleting non-existent file: %v", err)
	}

	// 6. Delete existing file and verify it is gone
	err = DeleteInitialMigrationState(stateFilePath)
	if err != nil {
		t.Fatalf("failed to delete existing state file: %v", err)
	}
	if _, err := os.Stat(stateFilePath); !os.IsNotExist(err) {
		t.Errorf("expected state file to be deleted from disk, but it still exists")
	}
}
