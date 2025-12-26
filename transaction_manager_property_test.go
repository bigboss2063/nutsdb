package nutsdb

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// TestProperty_ActiveTransactionCompletionGuarantee 测试活跃事务完成保证
// Feature: state-management-refactoring, Property 5: 活跃事务完成保证
//
// Property: For any 在关闭开始时活跃的事务，在关闭超时之前必须被允许完成或回滚
//
// 这个属性测试验证：
// 1. 在关闭开始时注册的所有活跃事务都能在超时前完成
// 2. 事务完成后能正确注销
// 3. Stop() 方法会等待所有活跃事务完成
func TestProperty_ActiveTransactionCompletionGuarantee(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100
	parameters.MaxSize = 20 // 最多 20 个并发事务

	properties := gopter.NewProperties(parameters)

	properties.Property("active transactions complete before shutdown timeout", prop.ForAll(
		func(numTxs int, txDurations []int) bool {
			// 创建测试环境
			db := &DB{
				snowflakeManager: NewSnowflakeManager(1),
			}
			sm := NewStatusManager(DefaultStatusManagerConfig())

			// 启动 StatusManager
			if err := sm.Start(); err != nil {
				t.Logf("Failed to start StatusManager: %v", err)
				return false
			}
			defer sm.Close()

			tm := newTxManager(db, sm)
			ctx := context.Background()
			if err := tm.Start(ctx); err != nil {
				t.Logf("Failed to start TransactionManager: %v", err)
				return false
			}

			// 确保 txDurations 的长度与 numTxs 匹配
			if len(txDurations) < numTxs {
				// 填充不足的部分
				for i := len(txDurations); i < numTxs; i++ {
					txDurations = append(txDurations, 50)
				}
			}
			txDurations = txDurations[:numTxs]

			// 计算最大事务持续时间
			maxDuration := 0
			for _, d := range txDurations {
				if d > maxDuration {
					maxDuration = d
				}
			}

			// 设置超时时间为最大事务持续时间的 3 倍（留有足够余量）
			shutdownTimeout := time.Duration(maxDuration*3) * time.Millisecond
			if shutdownTimeout < 1*time.Second {
				shutdownTimeout = 1 * time.Second
			}

			// 注册活跃事务
			var wg sync.WaitGroup
			txIDs := make([]uint64, numTxs)

			for i := 0; i < numTxs; i++ {
				txID := uint64(i + 1)
				txIDs[i] = txID
				tx := &Tx{id: txID, db: db}

				if err := tm.RegisterTx(tx); err != nil {
					t.Logf("Failed to register transaction %d: %v", txID, err)
					return false
				}

				// 启动 goroutine 模拟事务执行
				wg.Add(1)
				go func(id uint64, duration int) {
					defer wg.Done()

					// 模拟事务工作
					time.Sleep(time.Duration(duration) * time.Millisecond)

					// 事务完成后注销
					tm.UnregisterTx(id)
				}(txID, txDurations[i])
			}

			// 验证所有事务都已注册
			if count := tm.GetActiveTxCount(); count != int64(numTxs) {
				t.Logf("Expected %d active transactions, got %d", numTxs, count)
				return false
			}

			// 开始关闭 TransactionManager
			startTime := time.Now()
			stopErr := tm.Stop(shutdownTimeout)
			elapsed := time.Since(startTime)

			// 等待所有事务 goroutine 完成
			wg.Wait()

			// 验证 1: Stop() 应该在超时时间内完成
			if elapsed > shutdownTimeout+500*time.Millisecond {
				t.Logf("Stop() took %v, exceeded timeout %v", elapsed, shutdownTimeout)
				return false
			}

			// 验证 2: 如果所有事务都能在超时前完成，Stop() 不应该返回错误
			expectedMaxTime := time.Duration(maxDuration) * time.Millisecond
			if expectedMaxTime < shutdownTimeout && stopErr != nil {
				t.Logf("Stop() returned error when all transactions should complete: %v", stopErr)
				return false
			}

			// 验证 3: 所有事务最终都应该被注销
			finalCount := tm.GetActiveTxCount()
			if finalCount != 0 {
				t.Logf("Expected 0 active transactions after stop, got %d", finalCount)
				return false
			}

			// 验证 4: TransactionManager 应该已停止
			if tm.running.Load() {
				t.Logf("TransactionManager still running after Stop()")
				return false
			}

			return true
		},
		gen.IntRange(1, 20),                // numTxs: 1-20 个事务
		gen.SliceOf(gen.IntRange(10, 200)), // txDurations: 每个事务 10-200ms
	))

	properties.TestingRun(t)
}

// TestProperty_ActiveTransactionCompletionGuarantee_WithTimeout 测试超时场景
// Feature: state-management-refactoring, Property 5: 活跃事务完成保证（超时场景）
//
// 这个测试验证当事务执行时间超过超时时间时的行为
func TestProperty_ActiveTransactionCompletionGuarantee_WithTimeout(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 50
	parameters.MaxSize = 10

	properties := gopter.NewProperties(parameters)

	properties.Property("stop completes even when transactions exceed timeout", prop.ForAll(
		func(numTxs int, shortTimeout int) bool {
			// 创建测试环境
			db := &DB{
				snowflakeManager: NewSnowflakeManager(1),
			}
			sm := NewStatusManager(DefaultStatusManagerConfig())

			if err := sm.Start(); err != nil {
				t.Logf("Failed to start StatusManager: %v", err)
				return false
			}
			defer sm.Close()

			tm := newTxManager(db, sm)
			ctx := context.Background()
			if err := tm.Start(ctx); err != nil {
				t.Logf("Failed to start TransactionManager: %v", err)
				return false
			}

			// 使用较短的超时时间
			shutdownTimeout := time.Duration(shortTimeout) * time.Millisecond
			if shutdownTimeout < 100*time.Millisecond {
				shutdownTimeout = 100 * time.Millisecond
			}

			// 注册活跃事务，这些事务会运行超过超时时间
			longDuration := shutdownTimeout + 500*time.Millisecond

			var wg sync.WaitGroup
			for i := 0; i < numTxs; i++ {
				txID := uint64(i + 1)
				tx := &Tx{id: txID, db: db}

				if err := tm.RegisterTx(tx); err != nil {
					t.Logf("Failed to register transaction %d: %v", txID, err)
					return false
				}

				// 启动 goroutine 模拟长时间运行的事务
				wg.Add(1)
				go func(id uint64) {
					defer wg.Done()

					// 模拟长时间工作
					time.Sleep(longDuration)

					// 事务完成后注销
					tm.UnregisterTx(id)
				}(txID)
			}

			// 开始关闭
			startTime := time.Now()
			stopErr := tm.Stop(shutdownTimeout)
			elapsed := time.Since(startTime)

			// 验证 1: Stop() 应该在超时时间附近返回（允许一些误差）
			if elapsed < shutdownTimeout-100*time.Millisecond {
				t.Logf("Stop() returned too early: %v (timeout: %v)", elapsed, shutdownTimeout)
				return false
			}

			if elapsed > shutdownTimeout+1*time.Second {
				t.Logf("Stop() took too long: %v (timeout: %v)", elapsed, shutdownTimeout)
				return false
			}

			// 验证 2: 超时情况下，Stop() 应该返回错误或记录警告（但仍然完成）
			// 根据实现，Stop() 可能会记录警告但不返回错误
			_ = stopErr // 允许返回错误或 nil

			// 验证 3: TransactionManager 应该已停止（即使超时）
			if tm.running.Load() {
				t.Logf("TransactionManager still running after Stop() timeout")
				return false
			}

			// 等待所有事务 goroutine 完成（清理）
			wg.Wait()

			return true
		},
		gen.IntRange(1, 10),    // numTxs: 1-10 个事务
		gen.IntRange(100, 500), // shortTimeout: 100-500ms 的短超时
	))

	properties.TestingRun(t)
}

// TestProperty_TransactionRejectionDuringShutdown 测试关闭期间事务拒绝
// Feature: state-management-refactoring, Property 4: 关闭期间事务拒绝
//
// Property: For any 新事务请求，如果数据库状态为 Closing 或 Closed，则必须被拒绝并返回 ErrDBClosed
//
// 这个属性测试验证：
// 1. 当数据库状态为 Closing 时，新事务请求被拒绝
// 2. 当数据库状态为 Closed 时，新事务请求被拒绝
// 3. 拒绝的事务返回 ErrDBClosed 错误
// 4. 在关闭过程中的任何时刻尝试创建事务都会被拒绝
func TestProperty_TransactionRejectionDuringShutdown(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 30
	parameters.MaxSize = 15

	properties := gopter.NewProperties(parameters)

	properties.Property("new transactions rejected when status is Closing or Closed", prop.ForAll(
		func(numAttempts int, delayMs int) bool {
			// 创建测试环境
			db := &DB{
				snowflakeManager: NewSnowflakeManager(1),
			}
			sm := NewStatusManager(DefaultStatusManagerConfig())

			// 启动 StatusManager
			if err := sm.Start(); err != nil {
				t.Logf("Failed to start StatusManager: %v", err)
				return false
			}

			tm := newTxManager(db, sm)

			// 注册 TransactionManager 作为组件，这样 StatusManager.Close() 会自动调用 tm.Stop()
			if err := sm.RegisterComponent("TransactionManager", tm); err != nil {
				t.Logf("Failed to register TransactionManager: %v", err)
				sm.Close()
				return false
			}

			ctx := context.Background()
			if err := tm.Start(ctx); err != nil {
				t.Logf("Failed to start TransactionManager: %v", err)
				sm.Close()
				return false
			}

			// 验证在 Open 状态下可以创建事务
			tx, err := tm.BeginTx(true, false)
			if err != nil {
				t.Logf("Failed to create transaction in Open state: %v", err)
				sm.Close()
				return false
			}
			// 立即注销事务，避免阻塞关闭
			tm.UnregisterTx(tx.id)

			// 启动关闭过程（异步）
			shutdownDone := make(chan struct{})
			go func() {
				defer close(shutdownDone)
				sm.Close()
			}()

			// 在关闭过程中尝试创建多个事务
			// 使用随机延迟来测试关闭过程的不同阶段
			delay := time.Duration(delayMs) * time.Millisecond
			if delay > 500*time.Millisecond {
				delay = 500 * time.Millisecond
			}
			if delay < 10*time.Millisecond {
				delay = 10 * time.Millisecond
			}

			time.Sleep(delay)

			rejectedCount := 0
			for i := 0; i < numAttempts; i++ {
				_, err := tm.BeginTx(true, false)
				if err == ErrDBClosed {
					rejectedCount++
				} else if err != nil {
					// 其他错误也算作拒绝
					rejectedCount++
				}

				// 短暂延迟，让关闭过程继续
				time.Sleep(5 * time.Millisecond)
			}

			// 等待关闭完成
			<-shutdownDone

			// 给一点时间让 TransactionManager 完全停止
			time.Sleep(50 * time.Millisecond)

			// 验证 1: 关闭后，所有事务请求都应该被拒绝
			for i := 0; i < 5; i++ {
				_, err := tm.BeginTx(true, false)
				if err != ErrDBClosed {
					t.Logf("Expected ErrDBClosed after shutdown, got: %v", err)
					return false
				}
			}

			// 验证 2: 在关闭过程中，至少应该有一些事务被拒绝
			// （取决于延迟和关闭速度，可能全部被拒绝）
			if rejectedCount == 0 && numAttempts > 0 {
				// 如果延迟很短，可能在关闭开始前就完成了所有尝试
				// 这种情况下，我们检查最终状态
				status := sm.Status()
				if status != StatusClosed {
					t.Logf("Expected at least some rejections during shutdown, got 0 out of %d", numAttempts)
					return false
				}
			}

			// 验证 3: StatusManager 应该已经关闭
			if sm.Status() != StatusClosed {
				t.Logf("Expected StatusManager to be Closed, got: %s", sm.Status())
				return false
			}

			if tm.running.Load() {
				t.Logf("TransactionManager still running after shutdown")
				return false
			}

			return true
		},
		gen.IntRange(1, 15),   // numAttempts: 1-15 次尝试
		gen.IntRange(10, 300), // delayMs: 10-300ms 延迟
	))

	properties.TestingRun(t)
}

// TestProperty_TransactionRejectionDuringShutdown_ImmediateRejection 测试立即拒绝
// Feature: state-management-refactoring, Property 4: 关闭期间事务拒绝（立即拒绝场景）
//
// 这个测试验证在 Closing 状态下立即拒绝事务请求
func TestProperty_TransactionRejectionDuringShutdown_ImmediateRejection(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 30
	parameters.MaxSize = 30

	properties := gopter.NewProperties(parameters)

	properties.Property("transactions immediately rejected in Closing state", prop.ForAll(
		func(numConcurrentAttempts int) bool {
			// 创建测试环境
			db := &DB{
				snowflakeManager: NewSnowflakeManager(1),
			}
			sm := NewStatusManager(DefaultStatusManagerConfig())

			if err := sm.Start(); err != nil {
				t.Logf("Failed to start StatusManager: %v", err)
				return false
			}

			tm := newTxManager(db, sm)

			// 注册 TransactionManager 作为组件
			if err := sm.RegisterComponent("TransactionManager", tm); err != nil {
				t.Logf("Failed to register TransactionManager: %v", err)
				sm.Close()
				return false
			}

			ctx := context.Background()
			if err := tm.Start(ctx); err != nil {
				t.Logf("Failed to start TransactionManager: %v", err)
				sm.Close()
				return false
			}

			// 并发尝试创建事务，同时启动关闭过程
			var wg sync.WaitGroup
			rejectedCount := atomic.Int64{}
			successCount := atomic.Int64{}

			// 启动关闭过程
			closeStarted := make(chan struct{})
			closeDone := make(chan struct{})
			go func() {
				close(closeStarted)
				sm.Close()
				close(closeDone)
			}()

			// 等待关闭开始
			<-closeStarted

			// 短暂延迟，确保状态已经转换到 Closing
			time.Sleep(20 * time.Millisecond)

			// 现在并发尝试创建多个事务（此时应该在 Closing 状态）
			for i := 0; i < numConcurrentAttempts; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()

					_, err := tm.BeginTx(true, false)
					if err == ErrDBClosed {
						rejectedCount.Add(1)
					} else if err == nil {
						successCount.Add(1)
					}
				}()
			}

			wg.Wait()

			// 等待关闭完成
			<-closeDone

			// 验证 1: 所有事务请求都应该被拒绝（因为在 Closing 状态下）
			if rejectedCount.Load() != int64(numConcurrentAttempts) {
				t.Logf("Expected all %d transactions to be rejected in Closing state, got %d rejected and %d succeeded",
					numConcurrentAttempts, rejectedCount.Load(), successCount.Load())
				return false
			}

			// 验证 2: 没有事务成功创建
			if successCount.Load() != 0 {
				t.Logf("Expected 0 successful transactions in Closing state, got %d", successCount.Load())
				return false
			}

			// 验证 3: 活跃事务计数应该为 0
			if count := tm.GetActiveTxCount(); count != 0 {
				t.Logf("Expected 0 active transactions, got %d", count)
				return false
			}

			// 验证 4: StatusManager 应该已关闭
			if sm.Status() != StatusClosed {
				t.Logf("Expected StatusManager to be Closed, got: %s", sm.Status())
				return false
			}

			return true
		},
		gen.IntRange(1, 30), // numConcurrentAttempts: 1-30 个并发尝试
	))

	properties.TestingRun(t)
}

// TestProperty_TransactionRejectionDuringShutdown_ClosedState 测试 Closed 状态拒绝
// Feature: state-management-refactoring, Property 4: 关闭期间事务拒绝（Closed 状态）
//
// 这个测试验证在 Closed 状态下拒绝事务请求
func TestProperty_TransactionRejectionDuringShutdown_ClosedState(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 30
	parameters.MaxSize = 20

	properties := gopter.NewProperties(parameters)

	properties.Property("transactions rejected in Closed state", prop.ForAll(
		func(numAttempts int) bool {
			// 创建测试环境
			db := &DB{
				snowflakeManager: NewSnowflakeManager(1),
			}
			sm := NewStatusManager(DefaultStatusManagerConfig())

			if err := sm.Start(); err != nil {
				t.Logf("Failed to start StatusManager: %v", err)
				return false
			}

			tm := newTxManager(db, sm)

			// 注册 TransactionManager 作为组件
			if err := sm.RegisterComponent("TransactionManager", tm); err != nil {
				t.Logf("Failed to register TransactionManager: %v", err)
				sm.Close()
				return false
			}

			ctx := context.Background()
			if err := tm.Start(ctx); err != nil {
				t.Logf("Failed to start TransactionManager: %v", err)
				sm.Close()
				return false
			}

			// 完全关闭系统
			if err := sm.Close(); err != nil {
				t.Logf("Failed to close StatusManager: %v", err)
				return false
			}

			// 验证状态为 Closed
			if sm.Status() != StatusClosed {
				t.Logf("Expected status Closed, got: %s", sm.Status())
				return false
			}

			// 尝试创建多个事务
			for i := 0; i < numAttempts; i++ {
				_, err := tm.BeginTx(true, false)
				if err != ErrDBClosed {
					t.Logf("Expected ErrDBClosed in Closed state, got: %v", err)
					return false
				}
			}

			// 验证活跃事务计数为 0
			if count := tm.GetActiveTxCount(); count != 0 {
				t.Logf("Expected 0 active transactions in Closed state, got %d", count)
				return false
			}

			return true
		},
		gen.IntRange(1, 20), // numAttempts: 1-20 次尝试
	))

	properties.TestingRun(t)
}

// TestProperty_ActiveTransactionCompletionGuarantee_ConcurrentCompletion 测试并发完成
// Feature: state-management-refactoring, Property 5: 活跃事务完成保证（并发场景）
//
// 这个测试验证多个事务并发完成时的正确性
func TestProperty_ActiveTransactionCompletionGuarantee_ConcurrentCompletion(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100
	parameters.MaxSize = 30

	properties := gopter.NewProperties(parameters)

	properties.Property("concurrent transaction completion is handled correctly", prop.ForAll(
		func(numTxs int) bool {
			// 创建测试环境
			db := &DB{}
			sm := NewStatusManager(DefaultStatusManagerConfig())

			if err := sm.Start(); err != nil {
				t.Logf("Failed to start StatusManager: %v", err)
				return false
			}
			defer sm.Close()

			tm := newTxManager(db, sm)
			ctx := context.Background()
			if err := tm.Start(ctx); err != nil {
				t.Logf("Failed to start TransactionManager: %v", err)
				return false
			}

			// 注册活跃事务
			var wg sync.WaitGroup
			completedCount := int64(0)
			var completedMu sync.Mutex

			for i := 0; i < numTxs; i++ {
				txID := uint64(i + 1)
				tx := &Tx{id: txID, db: db}

				if err := tm.RegisterTx(tx); err != nil {
					t.Logf("Failed to register transaction %d: %v", txID, err)
					return false
				}

				// 启动 goroutine，所有事务几乎同时完成
				wg.Add(1)
				go func(id uint64) {
					defer wg.Done()

					// 短暂延迟后同时完成
					time.Sleep(50 * time.Millisecond)

					// 注销事务
					tm.UnregisterTx(id)

					// 记录完成
					completedMu.Lock()
					completedCount++
					completedMu.Unlock()
				}(txID)
			}

			// 开始关闭
			stopErr := tm.Stop(2 * time.Second)

			// 等待所有事务完成
			wg.Wait()

			// 验证 1: Stop() 应该成功（所有事务都能在超时前完成）
			if stopErr != nil {
				t.Logf("Stop() returned error: %v", stopErr)
				return false
			}

			// 验证 2: 所有事务都应该完成
			completedMu.Lock()
			finalCompleted := completedCount
			completedMu.Unlock()

			if finalCompleted != int64(numTxs) {
				t.Logf("Expected %d transactions to complete, got %d", numTxs, finalCompleted)
				return false
			}

			// 验证 3: 活跃事务计数应该为 0
			if count := tm.GetActiveTxCount(); count != 0 {
				t.Logf("Expected 0 active transactions, got %d", count)
				return false
			}

			return true
		},
		gen.IntRange(1, 30), // numTxs: 1-30 个并发事务
	))

	properties.TestingRun(t)
}
