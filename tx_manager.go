package nutsdb

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// txManager manages all active transactions
// Implements Component interface for lifecycle management
type txManager struct {
	db            *DB
	statusManager *StatusManager

	// Active transaction tracking
	activeTxs     sync.Map // map[uint64]*Tx
	activeTxCount atomic.Int64

	// Configuration
	maxActiveTxs int64

	// Running state
	running atomic.Bool
	mu      sync.RWMutex
}

// newTxManager creates a new txManager
func newTxManager(db *DB, sm *StatusManager) *txManager {
	return &txManager{
		db:            db,
		statusManager: sm,
		maxActiveTxs:  0, // 0 = unlimited
	}
}

// Name returns the component name
func (tm *txManager) Name() string {
	return "TransactionManager"
}

// Start starts the txManager
// Implements Component interface
func (tm *txManager) Start(ctx context.Context) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if tm.running.Load() {
		return fmt.Errorf("txManager already running")
	}

	// Initialize active transaction count
	tm.activeTxCount.Store(0)

	// Mark as running
	tm.running.Store(true)

	return nil
}

// Stop stops the txManager
// Waits for all active transactions to complete or timeout
// Implements Component interface
func (tm *txManager) Stop(timeout time.Duration) error {
	tm.mu.Lock()
	if !tm.running.Load() {
		tm.mu.Unlock()
		return nil
	}
	tm.mu.Unlock()

	// Wait for all active transactions to complete
	tm.WaitForActiveTxs(timeout)

	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Mark as stopped
	tm.running.Store(false)

	return nil
}

// BeginTx creates a new transaction
// Checks database status, rejects new transactions if database is closing or closed
// needLock: whether to acquire db.mu lock (WriteBatch's tx already has the lock)
func (tm *txManager) BeginTx(writable bool, acquireLock bool) (*Tx, error) {
	// Check if txManager is running
	if !tm.running.Load() {
		return nil, ErrDBClosed
	}

	// Check database status
	status := tm.statusManager.Status()
	if status == StatusClosing || status == StatusClosed {
		return nil, ErrDBClosed
	}

	// Check if max active transactions exceeded
	if tm.maxActiveTxs > 0 && tm.activeTxCount.Load() >= tm.maxActiveTxs {
		return nil, fmt.Errorf("too many active transactions: %d (max: %d)", tm.activeTxCount.Load(), tm.maxActiveTxs)
	}

	// Create transaction
	tx, err := newTx(tm.db, writable)
	if err != nil {
		return nil, err
	}

	// Only acquire lock if needed
	if acquireLock {
		tx.lock()
	} else {
		tx.lockAcquired = false
	}

	// Register transaction
	if err := tm.RegisterTx(tx); err != nil {
		return nil, err
	}

	return tx, nil
}

// RegisterTx registers a transaction to the active transaction list
func (tm *txManager) RegisterTx(tx *Tx) error {
	if tx == nil {
		return fmt.Errorf("cannot register nil transaction")
	}

	// Check database status again (double check)
	status := tm.statusManager.Status()
	if status == StatusClosing || status == StatusClosed {
		return ErrDBClosed
	}

	// Register transaction
	tm.activeTxs.Store(tx.id, tx)
	tm.activeTxCount.Add(1)

	return nil
}

// UnregisterTx unregisters a transaction from the active transaction list
func (tm *txManager) UnregisterTx(txID uint64) {
	if _, loaded := tm.activeTxs.LoadAndDelete(txID); loaded {
		tm.activeTxCount.Add(-1)
	}
}

// GetActiveTxCount returns the current number of active transactions
func (tm *txManager) GetActiveTxCount() int64 {
	return tm.activeTxCount.Load()
}

// WaitForActiveTxs waits for all active transactions to complete or timeout
func (tm *txManager) WaitForActiveTxs(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		count := tm.activeTxCount.Load()
		if count == 0 {
			return nil
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for %d active transactions to complete", count)
		}

		select {
		case <-ticker.C:
		case <-time.After(time.Until(deadline)):
			return fmt.Errorf("timeout waiting for %d active transactions to complete", tm.activeTxCount.Load())
		}
	}
}

// SetMaxActiveTxs sets the maximum number of active transactions
// 0 means unlimited
func (tm *txManager) SetMaxActiveTxs(max int64) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.maxActiveTxs = max
}

// GetMaxActiveTxs gets the maximum number of active transactions
func (tm *txManager) GetMaxActiveTxs() int64 {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.maxActiveTxs
}

// GetActiveTxs gets all active transaction IDs (for debugging)
func (tm *txManager) GetActiveTxs() []uint64 {
	txIDs := make([]uint64, 0)
	tm.activeTxs.Range(func(key, value interface{}) bool {
		txIDs = append(txIDs, key.(uint64))
		return true
	})
	return txIDs
}
