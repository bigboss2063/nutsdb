package nutsdb

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// TransactionManager 管理所有活跃事务
// 实现 Component 接口，负责事务的生命周期管理和状态协调
type TransactionManager struct {
	db            *DB
	statusManager *StatusManager

	// 活跃事务跟踪
	activeTxs     sync.Map // map[uint64]*Tx
	activeTxCount atomic.Int64

	// 配置
	maxActiveTxs int64

	// 日志
	logger ComponentLogger

	// 运行状态
	running atomic.Bool
	mu      sync.RWMutex
}

// NewTransactionManager 创建新的 TransactionManager
func NewTransactionManager(db *DB, sm *StatusManager, logger ComponentLogger) *TransactionManager {
	if logger == nil {
		logger = &DefaultComponentLogger{}
	}

	return &TransactionManager{
		db:            db,
		statusManager: sm,
		maxActiveTxs:  0, // 0 表示无限制
		logger:        logger,
	}
}

// Name 返回组件名称
func (tm *TransactionManager) Name() string {
	return "TransactionManager"
}

// Start 启动 TransactionManager
// 实现 Component 接口
func (tm *TransactionManager) Start(ctx context.Context) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if tm.running.Load() {
		return fmt.Errorf("TransactionManager already running")
	}

	tm.logger.Infof("TransactionManager starting")

	// 初始化活跃事务计数
	tm.activeTxCount.Store(0)

	// 标记为运行状态
	tm.running.Store(true)

	tm.logger.Infof("TransactionManager started successfully")

	return nil
}

// Stop 停止 TransactionManager
// 等待所有活跃事务完成或超时
// 实现 Component 接口
func (tm *TransactionManager) Stop(timeout time.Duration) error {
	tm.mu.Lock()
	if !tm.running.Load() {
		tm.mu.Unlock()
		tm.logger.Infof("TransactionManager already stopped")
		return nil
	}
	tm.mu.Unlock()

	tm.logger.Infof("TransactionManager stopping, waiting for %d active transactions", tm.activeTxCount.Load())

	// 等待所有活跃事务完成
	if err := tm.WaitForActiveTxs(timeout); err != nil {
		tm.logger.Warnf("TransactionManager stop timeout: %v", err)
		// 即使超时，也继续停止流程
	}

	tm.mu.Lock()
	defer tm.mu.Unlock()

	// 标记为停止状态
	tm.running.Store(false)

	tm.logger.Infof("TransactionManager stopped successfully")

	return nil
}

// BeginTx 创建新事务
// 检查数据库状态，如果数据库正在关闭或已关闭，则拒绝创建新事务
// needLock: 是否需要获取 db.mu 锁（WriteBatch 的 tx 已经自己获取了锁，不需要再次获取）
func (tm *TransactionManager) BeginTx(writable bool, acquireLock bool) (*Tx, error) {
	// 检查 TransactionManager 是否正在运行
	if !tm.running.Load() {
		return nil, ErrDBClosed
	}

	// 检查数据库状态
	status := tm.statusManager.Status()
	if status == StatusClosing || status == StatusClosed {
		return nil, ErrDBClosed
	}

	// 检查是否超过最大活跃事务数
	if tm.maxActiveTxs > 0 && tm.activeTxCount.Load() >= tm.maxActiveTxs {
		return nil, fmt.Errorf("too many active transactions: %d (max: %d)", tm.activeTxCount.Load(), tm.maxActiveTxs)
	}

	// 创建事务
	tx, err := newTx(tm.db, writable)
	if err != nil {
		return nil, err
	}

	// 只有需要锁时才获取，并设置 lockAcquired 标志
	if acquireLock {
		tx.lock()
	} else {
		tx.lockAcquired = false // Explicitly mark as not locked
	}

	// 注册事务
	if err := tm.RegisterTx(tx); err != nil {
		return nil, err
	}

	return tx, nil
}

// RegisterTx 注册事务到活跃事务列表
func (tm *TransactionManager) RegisterTx(tx *Tx) error {
	if tx == nil {
		return fmt.Errorf("cannot register nil transaction")
	}

	// 再次检查数据库状态（双重检查）
	status := tm.statusManager.Status()
	if status == StatusClosing || status == StatusClosed {
		return ErrDBClosed
	}

	// 注册事务
	tm.activeTxs.Store(tx.id, tx)
	tm.activeTxCount.Add(1)

	tm.logger.Debugf("Transaction %d registered, active count: %d", tx.id, tm.activeTxCount.Load())

	return nil
}

// UnregisterTx 从活跃事务列表中注销事务
func (tm *TransactionManager) UnregisterTx(txID uint64) {
	if _, loaded := tm.activeTxs.LoadAndDelete(txID); loaded {
		tm.activeTxCount.Add(-1)
		tm.logger.Debugf("Transaction %d unregistered, active count: %d", txID, tm.activeTxCount.Load())
	}
}

// GetActiveTxCount 返回当前活跃事务数量
func (tm *TransactionManager) GetActiveTxCount() int64 {
	return tm.activeTxCount.Load()
}

// WaitForActiveTxs 等待所有活跃事务完成或超时
func (tm *TransactionManager) WaitForActiveTxs(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		count := tm.activeTxCount.Load()
		if count == 0 {
			tm.logger.Infof("All active transactions completed")
			return nil
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for %d active transactions to complete", count)
		}

		select {
		case <-ticker.C:
			// 继续等待
		case <-time.After(time.Until(deadline)):
			return fmt.Errorf("timeout waiting for %d active transactions to complete", tm.activeTxCount.Load())
		}
	}
}

// SetMaxActiveTxs 设置最大活跃事务数
// 0 表示无限制
func (tm *TransactionManager) SetMaxActiveTxs(max int64) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.maxActiveTxs = max
}

// GetMaxActiveTxs 获取最大活跃事务数
func (tm *TransactionManager) GetMaxActiveTxs() int64 {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.maxActiveTxs
}

// GetActiveTxs 获取所有活跃事务的 ID 列表（用于调试）
func (tm *TransactionManager) GetActiveTxs() []uint64 {
	txIDs := make([]uint64, 0)
	tm.activeTxs.Range(func(key, value interface{}) bool {
		txIDs = append(txIDs, key.(uint64))
		return true
	})
	return txIDs
}
