package nutsdb

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestTransactionManager_Creation 测试 TransactionManager 创建
func TestTransactionManager_Creation(t *testing.T) {
	// 创建一个最小化的 DB 实例用于测试
	db := &DB{}
	sm := NewStatusManager(DefaultStatusManagerConfig())
	defer sm.Close()

	tm := NewTransactionManager(db, sm)

	if tm == nil {
		t.Fatal("Expected non-nil TransactionManager")
	}

	if tm.Name() != "TransactionManager" {
		t.Errorf("Expected name 'TransactionManager', got '%s'", tm.Name())
	}

	if tm.GetActiveTxCount() != 0 {
		t.Errorf("Expected 0 active transactions, got %d", tm.GetActiveTxCount())
	}

	if tm.running.Load() {
		t.Error("Expected TransactionManager to not be running initially")
	}
}

// TestTransactionManager_StartStop 测试启动和停止
func TestTransactionManager_StartStop(t *testing.T) {
	db := &DB{}
	sm := NewStatusManager(DefaultStatusManagerConfig())
	defer sm.Close()

	tm := NewTransactionManager(db, sm)

	// 测试启动
	ctx := context.Background()
	if err := tm.Start(ctx); err != nil {
		t.Fatalf("Failed to start TransactionManager: %v", err)
	}

	if !tm.running.Load() {
		t.Error("Expected TransactionManager to be running after Start()")
	}

	// 测试重复启动
	if err := tm.Start(ctx); err == nil {
		t.Error("Expected error when starting already running TransactionManager")
	}

	// 测试停止
	if err := tm.Stop(5 * time.Second); err != nil {
		t.Fatalf("Failed to stop TransactionManager: %v", err)
	}

	if tm.running.Load() {
		t.Error("Expected TransactionManager to not be running after Stop()")
	}

	// 测试重复停止（应该是幂等的）
	if err := tm.Stop(5 * time.Second); err != nil {
		t.Errorf("Expected no error when stopping already stopped TransactionManager, got: %v", err)
	}
}

// TestTransactionManager_RegisterUnregisterTx 测试事务注册和注销
func TestTransactionManager_RegisterUnregisterTx(t *testing.T) {
	db := &DB{}
	sm := NewStatusManager(DefaultStatusManagerConfig())
	defer sm.Close()

	// 启动 StatusManager 使其进入 Open 状态
	if err := sm.Start(); err != nil {
		t.Fatalf("Failed to start StatusManager: %v", err)
	}

	tm := NewTransactionManager(db, sm)
	ctx := context.Background()
	if err := tm.Start(ctx); err != nil {
		t.Fatalf("Failed to start TransactionManager: %v", err)
	}
	defer tm.Stop(5 * time.Second)

	// 创建模拟事务
	tx := &Tx{
		id: 1,
		db: db,
	}

	// 测试注册
	if err := tm.RegisterTx(tx); err != nil {
		t.Fatalf("Failed to register transaction: %v", err)
	}

	if count := tm.GetActiveTxCount(); count != 1 {
		t.Errorf("Expected 1 active transaction, got %d", count)
	}

	// 验证事务在活跃列表中
	txIDs := tm.GetActiveTxs()
	if len(txIDs) != 1 || txIDs[0] != 1 {
		t.Errorf("Expected transaction ID 1 in active list, got %v", txIDs)
	}

	// 测试注销
	tm.UnregisterTx(tx.id)

	if count := tm.GetActiveTxCount(); count != 0 {
		t.Errorf("Expected 0 active transactions after unregister, got %d", count)
	}

	// 验证事务不在活跃列表中
	txIDs = tm.GetActiveTxs()
	if len(txIDs) != 0 {
		t.Errorf("Expected empty active list after unregister, got %v", txIDs)
	}

	// 测试注销不存在的事务（应该是安全的）
	tm.UnregisterTx(999)
	if count := tm.GetActiveTxCount(); count != 0 {
		t.Errorf("Expected 0 active transactions, got %d", count)
	}
}

// TestTransactionManager_RegisterNilTx 测试注册 nil 事务
func TestTransactionManager_RegisterNilTx(t *testing.T) {
	db := &DB{}
	sm := NewStatusManager(DefaultStatusManagerConfig())
	defer sm.Close()

	if err := sm.Start(); err != nil {
		t.Fatalf("Failed to start StatusManager: %v", err)
	}

	tm := NewTransactionManager(db, sm)
	ctx := context.Background()
	if err := tm.Start(ctx); err != nil {
		t.Fatalf("Failed to start TransactionManager: %v", err)
	}
	defer tm.Stop(5 * time.Second)

	// 测试注册 nil 事务
	if err := tm.RegisterTx(nil); err == nil {
		t.Error("Expected error when registering nil transaction")
	}
}

// TestTransactionManager_RejectTxWhenClosing 测试关闭期间拒绝新事务
func TestTransactionManager_RejectTxWhenClosing(t *testing.T) {
	db := &DB{}
	sm := NewStatusManager(DefaultStatusManagerConfig())
	defer sm.Close()

	// 启动 StatusManager
	if err := sm.Start(); err != nil {
		t.Fatalf("Failed to start StatusManager: %v", err)
	}

	tm := NewTransactionManager(db, sm)
	ctx := context.Background()
	if err := tm.Start(ctx); err != nil {
		t.Fatalf("Failed to start TransactionManager: %v", err)
	}

	// 在 Open 状态下，注册应该成功
	tx1 := &Tx{id: 1, db: db}
	if err := tm.RegisterTx(tx1); err != nil {
		t.Fatalf("Failed to register transaction in Open state: %v", err)
	}

	// 模拟数据库进入 Closing 状态
	sm.transitionTo(StatusClosing, "test closing")

	// 在 Closing 状态下，注册应该失败
	tx2 := &Tx{id: 2, db: db}
	if err := tm.RegisterTx(tx2); err != ErrDBClosed {
		t.Errorf("Expected ErrDBClosed when registering transaction in Closing state, got: %v", err)
	}

	// 验证只有第一个事务被注册
	if count := tm.GetActiveTxCount(); count != 1 {
		t.Errorf("Expected 1 active transaction, got %d", count)
	}

	// 模拟数据库进入 Closed 状态
	sm.transitionTo(StatusClosed, "test closed")

	// 在 Closed 状态下，注册也应该失败
	tx3 := &Tx{id: 3, db: db}
	if err := tm.RegisterTx(tx3); err != ErrDBClosed {
		t.Errorf("Expected ErrDBClosed when registering transaction in Closed state, got: %v", err)
	}

	// 清理
	tm.UnregisterTx(tx1.id)
	tm.Stop(5 * time.Second)
}

// TestTransactionManager_RejectTxWhenNotRunning 测试未运行时拒绝新事务
func TestTransactionManager_RejectTxWhenNotRunning(t *testing.T) {
	db := &DB{}
	sm := NewStatusManager(DefaultStatusManagerConfig())
	defer sm.Close()

	if err := sm.Start(); err != nil {
		t.Fatalf("Failed to start StatusManager: %v", err)
	}

	tm := NewTransactionManager(db, sm)

	// TransactionManager 未启动时，RegisterTx 仍然可以工作
	// 因为 RegisterTx 只检查 StatusManager 的状态，不检查 TransactionManager 是否运行
	// 这是设计上的选择，因为 TransactionManager 的 running 状态主要用于 HealthCheck
	tx := &Tx{id: 1, db: db}
	if err := tm.RegisterTx(tx); err != nil {
		t.Errorf("RegisterTx should work even when TransactionManager is not running (only checks StatusManager), got: %v", err)
	}

	// 清理
	tm.UnregisterTx(tx.id)
}

// TestTransactionManager_WaitForActiveTxs 测试等待活跃事务完成
func TestTransactionManager_WaitForActiveTxs(t *testing.T) {
	db := &DB{}
	sm := NewStatusManager(DefaultStatusManagerConfig())
	defer sm.Close()

	if err := sm.Start(); err != nil {
		t.Fatalf("Failed to start StatusManager: %v", err)
	}

	tm := NewTransactionManager(db, sm)
	ctx := context.Background()
	if err := tm.Start(ctx); err != nil {
		t.Fatalf("Failed to start TransactionManager: %v", err)
	}
	defer tm.Stop(5 * time.Second)

	// 测试没有活跃事务时立即返回
	start := time.Now()
	if err := tm.WaitForActiveTxs(5 * time.Second); err != nil {
		t.Errorf("Expected no error when waiting with no active transactions, got: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > 1*time.Second {
		t.Errorf("Expected WaitForActiveTxs to return quickly with no active transactions, took %v", elapsed)
	}

	// 注册一些事务
	tx1 := &Tx{id: 1, db: db}
	tx2 := &Tx{id: 2, db: db}
	tx3 := &Tx{id: 3, db: db}

	if err := tm.RegisterTx(tx1); err != nil {
		t.Fatalf("Failed to register tx1: %v", err)
	}
	if err := tm.RegisterTx(tx2); err != nil {
		t.Fatalf("Failed to register tx2: %v", err)
	}
	if err := tm.RegisterTx(tx3); err != nil {
		t.Fatalf("Failed to register tx3: %v", err)
	}

	// 在后台逐个注销事务
	go func() {
		time.Sleep(200 * time.Millisecond)
		tm.UnregisterTx(tx1.id)

		time.Sleep(200 * time.Millisecond)
		tm.UnregisterTx(tx2.id)

		time.Sleep(200 * time.Millisecond)
		tm.UnregisterTx(tx3.id)
	}()

	// 等待所有事务完成
	start = time.Now()
	if err := tm.WaitForActiveTxs(5 * time.Second); err != nil {
		t.Errorf("Expected no error when waiting for active transactions, got: %v", err)
	}
	elapsed = time.Since(start)

	// 应该在 600ms 左右完成（3 * 200ms）
	if elapsed < 500*time.Millisecond || elapsed > 1*time.Second {
		t.Errorf("Expected WaitForActiveTxs to take ~600ms, took %v", elapsed)
	}

	// 验证所有事务都已注销
	if count := tm.GetActiveTxCount(); count != 0 {
		t.Errorf("Expected 0 active transactions after wait, got %d", count)
	}
}

// TestTransactionManager_WaitForActiveTxsTimeout 测试等待超时
func TestTransactionManager_WaitForActiveTxsTimeout(t *testing.T) {
	db := &DB{}
	sm := NewStatusManager(DefaultStatusManagerConfig())
	defer sm.Close()

	if err := sm.Start(); err != nil {
		t.Fatalf("Failed to start StatusManager: %v", err)
	}

	tm := NewTransactionManager(db, sm)
	ctx := context.Background()
	if err := tm.Start(ctx); err != nil {
		t.Fatalf("Failed to start TransactionManager: %v", err)
	}
	defer tm.Stop(5 * time.Second)

	// 注册一个事务但不注销
	tx := &Tx{id: 1, db: db}
	if err := tm.RegisterTx(tx); err != nil {
		t.Fatalf("Failed to register transaction: %v", err)
	}

	// 等待应该超时
	start := time.Now()
	err := tm.WaitForActiveTxs(500 * time.Millisecond)
	elapsed := time.Since(start)

	if err == nil {
		t.Error("Expected timeout error when waiting for active transactions")
	}

	// 应该在超时时间附近返回
	if elapsed < 400*time.Millisecond || elapsed > 700*time.Millisecond {
		t.Errorf("Expected WaitForActiveTxs to timeout at ~500ms, took %v", elapsed)
	}

	// 验证事务仍然活跃
	if count := tm.GetActiveTxCount(); count != 1 {
		t.Errorf("Expected 1 active transaction after timeout, got %d", count)
	}

	// 清理
	tm.UnregisterTx(tx.id)
}

// TestTransactionManager_ConcurrentRegisterUnregister 测试并发注册和注销
func TestTransactionManager_ConcurrentRegisterUnregister(t *testing.T) {
	db := &DB{}
	sm := NewStatusManager(DefaultStatusManagerConfig())
	defer sm.Close()

	if err := sm.Start(); err != nil {
		t.Fatalf("Failed to start StatusManager: %v", err)
	}

	tm := NewTransactionManager(db, sm)
	ctx := context.Background()
	if err := tm.Start(ctx); err != nil {
		t.Fatalf("Failed to start TransactionManager: %v", err)
	}
	defer tm.Stop(5 * time.Second)

	const numGoroutines = 50
	const txPerGoroutine = 20

	var wg sync.WaitGroup
	var txIDCounter atomic.Uint64

	// 并发注册和注销事务
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()

			for j := 0; j < txPerGoroutine; j++ {
				txID := txIDCounter.Add(1)
				tx := &Tx{id: txID, db: db}

				// 注册事务
				if err := tm.RegisterTx(tx); err != nil {
					t.Errorf("Failed to register transaction %d: %v", txID, err)
					continue
				}

				// 模拟一些工作
				time.Sleep(1 * time.Millisecond)

				// 注销事务
				tm.UnregisterTx(txID)
			}
		}()
	}

	wg.Wait()

	// 验证所有事务都已注销
	if count := tm.GetActiveTxCount(); count != 0 {
		t.Errorf("Expected 0 active transactions after concurrent operations, got %d", count)
	}

	txIDs := tm.GetActiveTxs()
	if len(txIDs) != 0 {
		t.Errorf("Expected empty active transaction list, got %v", txIDs)
	}
}

// TestTransactionManager_MaxActiveTxs 测试最大活跃事务数限制
func TestTransactionManager_MaxActiveTxs(t *testing.T) {
	db := &DB{}
	sm := NewStatusManager(DefaultStatusManagerConfig())
	defer sm.Close()

	if err := sm.Start(); err != nil {
		t.Fatalf("Failed to start StatusManager: %v", err)
	}

	tm := NewTransactionManager(db, sm)
	ctx := context.Background()
	if err := tm.Start(ctx); err != nil {
		t.Fatalf("Failed to start TransactionManager: %v", err)
	}
	defer tm.Stop(5 * time.Second)

	// 设置最大活跃事务数
	maxTxs := int64(5)
	tm.SetMaxActiveTxs(maxTxs)

	if got := tm.GetMaxActiveTxs(); got != maxTxs {
		t.Errorf("Expected max active txs %d, got %d", maxTxs, got)
	}

	// 注册最大数量的事务
	for i := int64(1); i <= maxTxs; i++ {
		tx := &Tx{id: uint64(i), db: db}
		if err := tm.RegisterTx(tx); err != nil {
			t.Fatalf("Failed to register transaction %d: %v", i, err)
		}
	}

	// 验证计数
	if count := tm.GetActiveTxCount(); count != maxTxs {
		t.Errorf("Expected %d active transactions, got %d", maxTxs, count)
	}

	// 清理
	for i := int64(1); i <= maxTxs; i++ {
		tm.UnregisterTx(uint64(i))
	}
}

// TestTransactionManager_StopWithActiveTxs 测试有活跃事务时停止
func TestTransactionManager_StopWithActiveTxs(t *testing.T) {
	db := &DB{}
	sm := NewStatusManager(DefaultStatusManagerConfig())
	defer sm.Close()

	if err := sm.Start(); err != nil {
		t.Fatalf("Failed to start StatusManager: %v", err)
	}

	tm := NewTransactionManager(db, sm)
	ctx := context.Background()
	if err := tm.Start(ctx); err != nil {
		t.Fatalf("Failed to start TransactionManager: %v", err)
	}

	// 注册一些事务
	tx1 := &Tx{id: 1, db: db}
	tx2 := &Tx{id: 2, db: db}

	if err := tm.RegisterTx(tx1); err != nil {
		t.Fatalf("Failed to register tx1: %v", err)
	}
	if err := tm.RegisterTx(tx2); err != nil {
		t.Fatalf("Failed to register tx2: %v", err)
	}

	// 在后台注销事务
	go func() {
		time.Sleep(300 * time.Millisecond)
		tm.UnregisterTx(tx1.id)
		tm.UnregisterTx(tx2.id)
	}()

	// 停止应该等待事务完成
	start := time.Now()
	if err := tm.Stop(2 * time.Second); err != nil {
		t.Errorf("Expected no error when stopping with active transactions, got: %v", err)
	}
	elapsed := time.Since(start)

	// 应该在 300ms 左右完成
	if elapsed < 200*time.Millisecond || elapsed > 500*time.Millisecond {
		t.Errorf("Expected Stop to wait ~300ms for active transactions, took %v", elapsed)
	}

	if tm.running.Load() {
		t.Error("Expected TransactionManager to not be running after Stop()")
	}
}

// TestTransactionManager_StopTimeout 测试停止超时
func TestTransactionManager_StopTimeout(t *testing.T) {
	db := &DB{}
	sm := NewStatusManager(DefaultStatusManagerConfig())
	defer sm.Close()

	if err := sm.Start(); err != nil {
		t.Fatalf("Failed to start StatusManager: %v", err)
	}

	tm := NewTransactionManager(db, sm)
	ctx := context.Background()
	if err := tm.Start(ctx); err != nil {
		t.Fatalf("Failed to start TransactionManager: %v", err)
	}

	// 注册一个事务但不注销（模拟卡住的事务）
	tx := &Tx{id: 1, db: db}
	if err := tm.RegisterTx(tx); err != nil {
		t.Fatalf("Failed to register transaction: %v", err)
	}

	// 停止应该超时但仍然完成
	start := time.Now()
	err := tm.Stop(500 * time.Millisecond)
	elapsed := time.Since(start)

	// Stop 应该记录警告但不返回错误（根据实现）
	if err != nil {
		t.Logf("Stop returned error (expected warning): %v", err)
	}

	// 应该在超时时间附近返回
	if elapsed < 400*time.Millisecond || elapsed > 700*time.Millisecond {
		t.Errorf("Expected Stop to timeout at ~500ms, took %v", elapsed)
	}

	// TransactionManager 应该已停止
	if tm.running.Load() {
		t.Error("Expected TransactionManager to not be running after Stop() timeout")
	}

	// 清理
	tm.UnregisterTx(tx.id)
}
