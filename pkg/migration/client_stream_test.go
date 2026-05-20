package migration

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gsbingo17/mongodb-migration/pkg/config"
)

func TestStartReplicationSafetyInvariants(t *testing.T) {
	r := &ClientLevelReplicator{}
	ctx := context.TODO()

	// Case 1: initialMigrationState is nil, but globalResumeToken is NOT nil (fresh run violation)
	err := r.StartReplication(ctx, "some-resume-token", "path", nil, "state-path", config.DatabasePair{}, false, nil)
	if err == nil {
		t.Errorf("expected error when state is nil but resume token exists")
	} else if !strings.Contains(err.Error(), "safety violation: initial migration state file does not exist") {
		t.Errorf("unexpected error message: %v", err)
	}

	// Case 2: initialMigrationState is Completed, but globalResumeToken is nil (resumption violation)
	completedState := &InitialMigrationState{Status: StatusCompleted}
	err = r.StartReplication(ctx, nil, "path", completedState, "state-path", config.DatabasePair{}, false, nil)
	if err == nil {
		t.Errorf("expected error when state is completed but resume token is nil")
	} else if !strings.Contains(err.Error(), "safety violation: initial migration state is marked as completed") {
		t.Errorf("unexpected error message: %v", err)
	}

	// Case 3: initialMigrationState is CompletedWithFailures (abort run)
	failedState := &InitialMigrationState{Status: StatusCompletedWithFailures}
	err = r.StartReplication(ctx, "some-resume-token", "path", failedState, "state-path", config.DatabasePair{}, false, nil)
	if err == nil {
		t.Errorf("expected error when state is CompletedWithFailures")
	} else if !strings.Contains(err.Error(), "cannot start replication: initial migration completed with failures in a previous run") {
		t.Errorf("unexpected error message: %v", err)
	}

	// Case 4: initialMigrationState is Skipped, but globalResumeToken is nil (resumption violation for live-only)
	skippedState := &InitialMigrationState{Status: StatusSkipped}
	err = r.StartReplication(ctx, nil, "path", skippedState, "state-path", config.DatabasePair{}, true, nil)
	if err == nil {
		t.Errorf("expected error when state is skipped but resume token is nil")
	} else if !strings.Contains(err.Error(), "safety violation: initial migration state is marked as skipped") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestDeleteInitialMigrationState(t *testing.T) {
	tmpDir := t.TempDir()
	stateFilePath := filepath.Join(tmpDir, "initialMigrationState-delete-test.json")

	// 1. Delete non-existent file (should not error)
	err := DeleteInitialMigrationState(stateFilePath)
	if err != nil {
		t.Fatalf("unexpected error deleting non-existent file: %v", err)
	}

	// 2. Save state, then verify it exists and has correct properties
	err = SaveInitialMigrationState(stateFilePath, StatusCompletedWithFailures, 5)
	if err != nil {
		t.Fatalf("failed to save state: %v", err)
	}
	if _, err := os.Stat(stateFilePath); os.IsNotExist(err) {
		t.Fatalf("expected state file to exist on disk")
	}

	loaded, err := LoadInitialMigrationState(stateFilePath)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}
	if !loaded.IsCompleted() {
		t.Errorf("expected IsCompleted to be true")
	}
	if loaded.Status != StatusCompletedWithFailures {
		t.Errorf("expected Status to be StatusCompletedWithFailures, got %q", loaded.Status)
	}
	if loaded.FailedCount != 5 {
		t.Errorf("expected FailedCount to be 5, got %d", loaded.FailedCount)
	}

	// 3. Delete existing file
	err = DeleteInitialMigrationState(stateFilePath)
	if err != nil {
		t.Fatalf("unexpected error deleting existing file: %v", err)
	}

	// 4. Verify it is deleted from disk
	if _, err := os.Stat(stateFilePath); !os.IsNotExist(err) {
		t.Errorf("expected state file to be deleted from disk, but it still exists")
	}
}
