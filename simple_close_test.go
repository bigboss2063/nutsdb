package nutsdb

import (
	"testing"
	"path/filepath"
	"os"
	"fmt"
)

func TestDB_SimpleClose(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "test-simple-close")
	defer os.RemoveAll(dir)

	opts := DefaultOptions
	opts.Dir = dir

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	fmt.Printf("Before Close: Status=%d\n", db.statusManager.Status())

	// Close explicitly
	err = db.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	fmt.Printf("After Close: Status=%d\n", db.statusManager.Status())

	// Check IsClose
	if !db.IsClose() {
		t.Error("IsClose should return true after Close")
	}
}
