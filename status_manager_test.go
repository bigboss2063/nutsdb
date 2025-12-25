package nutsdb

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// MockComponent 是一个用于测试的模拟组件
type MockComponent struct {
	name        string
	startErr    error
	stopErr     error
	startDelay  time.Duration
	stopDelay   time.Duration
	startCalled atomic.Int32
	stopCalled  atomic.Int32
}

// NewMockComponent 创建一个新的模拟组件
func NewMockComponent(name string) *MockComponent {
	return &MockComponent{
		name: name,
	}
}

// Name 返回组件名称
func (mc *MockComponent) Name() string {
	return mc.name
}

// Start 启动组件
func (mc *MockComponent) Start(ctx context.Context) error {
	mc.startCalled.Add(1)
	if mc.startDelay > 0 {
		time.Sleep(mc.startDelay)
	}
	return mc.startErr
}

// Stop 停止组件
func (mc *MockComponent) Stop(timeout time.Duration) error {
	mc.stopCalled.Add(1)
	if mc.stopDelay > 0 {
		time.Sleep(mc.stopDelay)
	}
	return mc.stopErr
}

// WithStartError 设置启动错误
func (mc *MockComponent) WithStartError(err error) *MockComponent {
	mc.startErr = err
	return mc
}

// WithStopError 设置停止错误
func (mc *MockComponent) WithStopError(err error) *MockComponent {
	mc.stopErr = err
	return mc
}

// WithStartDelay 设置启动延迟
func (mc *MockComponent) WithStartDelay(delay time.Duration) *MockComponent {
	mc.startDelay = delay
	return mc
}

// WithStopDelay 设置停止延迟
func (mc *MockComponent) WithStopDelay(delay time.Duration) *MockComponent {
	mc.stopDelay = delay
	return mc
}

// GetStartCallCount 获取启动调用次数
func (mc *MockComponent) GetStartCallCount() int32 {
	return mc.startCalled.Load()
}

// GetStopCallCount 获取停止调用次数
func (mc *MockComponent) GetStopCallCount() int32 {
	return mc.stopCalled.Load()
}

// TestStatusManager_StateInitialization 测试状态初始化
func TestStatusManager_StateInitialization(t *testing.T) {
	tests := []struct {
		name           string
		config         StatusManagerConfig
		expectedStatus Status
	}{
		{
			name:           "default config initialization",
			config:         DefaultStatusManagerConfig(),
			expectedStatus: StatusInitializing,
		},
		{
			name: "custom config initialization",
			config: StatusManagerConfig{
				ShutdownTimeout: 10 * time.Second,
			},
			expectedStatus: StatusInitializing,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sm := NewStatusManager(tt.config, nil)
			defer sm.Close()

			// 验证初始状态
			if status := sm.Status(); status != tt.expectedStatus {
				t.Errorf("Expected initial status %v, got %v", tt.expectedStatus, status)
			}

			// 验证 IsOperational 返回 false（因为还未启动）
			if sm.IsOperational() {
				t.Error("Expected IsOperational to return false for Initializing state")
			}

			// 验证 Context 不为 nil
			if sm.Context() == nil {
				t.Error("Expected Context to be non-nil")
			}

			// 验证 Context 未被取消
			select {
			case <-sm.Context().Done():
				t.Error("Expected Context to not be cancelled initially")
			default:
				// Context 未被取消，符合预期
			}
		})
	}
}

// TestStatusManager_ConcurrentStateQueries 测试并发状态查询
func TestStatusManager_ConcurrentStateQueries(t *testing.T) {
	config := StatusManagerConfig{
		ShutdownTimeout: 5 * time.Second,
	}

	sm := NewStatusManager(config, nil)
	defer sm.Close()

	// 注册一些模拟组件
	comp1 := NewMockComponent("component1")
	comp2 := NewMockComponent("component2")
	comp3 := NewMockComponent("component3")

	if err := sm.RegisterComponent("component1", comp1); err != nil {
		t.Fatalf("Failed to register component1: %v", err)
	}
	if err := sm.RegisterComponent("component2", comp2); err != nil {
		t.Fatalf("Failed to register component2: %v", err)
	}
	if err := sm.RegisterComponent("component3", comp3); err != nil {
		t.Fatalf("Failed to register component3: %v", err)
	}

	// 启动 StatusManager
	if err := sm.Start(); err != nil {
		t.Fatalf("Failed to start StatusManager: %v", err)
	}

	// 验证状态已转换到 Open
	if status := sm.Status(); status != StatusOpen {
		t.Errorf("Expected status Open after Start(), got %v", status)
	}

	// 并发查询状态
	const numGoroutines = 100
	const queriesPerGoroutine = 100

	var wg sync.WaitGroup
	statusResults := make([]Status, numGoroutines*queriesPerGoroutine)
	operationalResults := make([]bool, numGoroutines*queriesPerGoroutine)

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < queriesPerGoroutine; j++ {
				idx := goroutineID*queriesPerGoroutine + j
				statusResults[idx] = sm.Status()
				operationalResults[idx] = sm.IsOperational()
			}
		}(i)
	}

	wg.Wait()

	// 验证所有查询结果一致
	expectedStatus := StatusOpen
	expectedOperational := true

	for i, status := range statusResults {
		if status != expectedStatus {
			t.Errorf("Query %d: expected status %v, got %v", i, expectedStatus, status)
		}
	}

	for i, operational := range operationalResults {
		if operational != expectedOperational {
			t.Errorf("Query %d: expected operational %v, got %v", i, expectedOperational, operational)
		}
	}
}

// TestStatusManager_ConcurrentStateQueriesDuringTransition 测试状态转换期间的并发查询
func TestStatusManager_ConcurrentStateQueriesDuringTransition(t *testing.T) {
	config := StatusManagerConfig{
		ShutdownTimeout: 5 * time.Second,
	}

	sm := NewStatusManager(config, nil)

	// 注册一个带延迟的组件，使启动过程更慢
	slowComp := NewMockComponent("slow_component").WithStartDelay(200 * time.Millisecond)
	if err := sm.RegisterComponent("slow_component", slowComp); err != nil {
		t.Fatalf("Failed to register component: %v", err)
	}

	// 在后台启动 StatusManager
	startDone := make(chan error, 1)
	go func() {
		startDone <- sm.Start()
	}()

	// 在启动过程中并发查询状态
	const numGoroutines = 50
	var wg sync.WaitGroup
	statusCounts := make(map[Status]*atomic.Int32)
	statusCounts[StatusInitializing] = &atomic.Int32{}
	statusCounts[StatusOpen] = &atomic.Int32{}

	stopQuerying := make(chan struct{})

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			// 持续查询直到收到停止信号
			for {
				select {
				case <-stopQuerying:
					return
				default:
					status := sm.Status()
					if counter, ok := statusCounts[status]; ok {
						counter.Add(1)
					}
					time.Sleep(1 * time.Millisecond)
				}
			}
		}()
	}

	// 等待启动完成
	if err := <-startDone; err != nil {
		t.Fatalf("Failed to start StatusManager: %v", err)
	}

	// 启动完成后继续查询一小段时间以确保观察到 Open 状态
	time.Sleep(50 * time.Millisecond)

	// 停止查询
	close(stopQuerying)
	wg.Wait()

	// 验证最终状态
	if status := sm.Status(); status != StatusOpen {
		t.Errorf("Expected final status Open, got %v", status)
	}

	// 验证我们观察到了状态转换
	initCount := statusCounts[StatusInitializing].Load()
	openCount := statusCounts[StatusOpen].Load()

	if initCount == 0 {
		t.Error("Expected to observe Initializing state during startup")
	}
	if openCount == 0 {
		t.Error("Expected to observe Open state after startup")
	}

	t.Logf("Observed states: Initializing=%d, Open=%d", initCount, openCount)

	// 清理
	sm.Close()
}

// TestStatusManager_StateQueryConsistency 测试状态查询的一致性
func TestStatusManager_StateQueryConsistency(t *testing.T) {
	config := StatusManagerConfig{
		ShutdownTimeout: 5 * time.Second,
	}

	sm := NewStatusManager(config, nil)
	defer sm.Close()

	// 注册组件
	comp := NewMockComponent("test_component")
	if err := sm.RegisterComponent("test_component", comp); err != nil {
		t.Fatalf("Failed to register component: %v", err)
	}

	// 启动
	if err := sm.Start(); err != nil {
		t.Fatalf("Failed to start StatusManager: %v", err)
	}

	// 在同一时刻进行多次查询，验证结果一致
	const numQueries = 1000
	results := make([]Status, numQueries)

	// 使用 barrier 确保所有 goroutine 同时开始查询
	var startBarrier sync.WaitGroup
	startBarrier.Add(1)

	var wg sync.WaitGroup
	wg.Add(numQueries)

	for i := 0; i < numQueries; i++ {
		go func(idx int) {
			defer wg.Done()
			startBarrier.Wait() // 等待所有 goroutine 就绪
			results[idx] = sm.Status()
		}(i)
	}

	// 释放所有 goroutine
	startBarrier.Done()
	wg.Wait()

	// 验证所有结果相同
	firstResult := results[0]
	for i, result := range results {
		if result != firstResult {
			t.Errorf("Query %d: expected status %v, got %v (inconsistent with first query)", i, firstResult, result)
		}
	}

	t.Logf("All %d concurrent queries returned consistent status: %v", numQueries, firstResult)
}

// TestStatusManager_IsOperationalConsistency 测试 IsOperational 的一致性
func TestStatusManager_IsOperationalConsistency(t *testing.T) {
	config := StatusManagerConfig{
		ShutdownTimeout: 5 * time.Second,
	}

	sm := NewStatusManager(config, nil)
	defer sm.Close()

	// 注册组件
	comp := NewMockComponent("test_component")
	if err := sm.RegisterComponent("test_component", comp); err != nil {
		t.Fatalf("Failed to register component: %v", err)
	}

	// 测试不同状态下的 IsOperational
	testCases := []struct {
		name                string
		setupFunc           func() error
		expectedOperational bool
		expectedStatus      Status
	}{
		{
			name:                "Initializing state",
			setupFunc:           func() error { return nil },
			expectedOperational: false,
			expectedStatus:      StatusInitializing,
		},
		{
			name: "Open state",
			setupFunc: func() error {
				return sm.Start()
			},
			expectedOperational: true,
			expectedStatus:      StatusOpen,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.setupFunc(); err != nil {
				t.Fatalf("Setup failed: %v", err)
			}

			// 并发查询 IsOperational
			const numGoroutines = 100
			results := make([]bool, numGoroutines)
			var wg sync.WaitGroup

			wg.Add(numGoroutines)
			for i := 0; i < numGoroutines; i++ {
				go func(idx int) {
					defer wg.Done()
					results[idx] = sm.IsOperational()
				}(i)
			}

			wg.Wait()

			// 验证所有结果一致
			for i, result := range results {
				if result != tc.expectedOperational {
					t.Errorf("Query %d: expected IsOperational=%v, got %v", i, tc.expectedOperational, result)
				}
			}

			// 验证状态
			if status := sm.Status(); status != tc.expectedStatus {
				t.Errorf("Expected status %v, got %v", tc.expectedStatus, status)
			}
		})
	}
}

// TestStatusManager_ContextCancellation 测试 Context 取消
func TestStatusManager_ContextCancellation(t *testing.T) {
	config := StatusManagerConfig{
		ShutdownTimeout: 5 * time.Second,
	}

	sm := NewStatusManager(config, nil)

	// 注册组件
	comp := NewMockComponent("test_component")
	if err := sm.RegisterComponent("test_component", comp); err != nil {
		t.Fatalf("Failed to register component: %v", err)
	}

	// 启动
	if err := sm.Start(); err != nil {
		t.Fatalf("Failed to start StatusManager: %v", err)
	}

	// 获取 context
	ctx := sm.Context()
	if ctx == nil {
		t.Fatal("Expected non-nil context")
	}

	// 验证 context 未被取消
	select {
	case <-ctx.Done():
		t.Error("Context should not be cancelled before Close()")
	default:
		// 符合预期
	}

	// 关闭 StatusManager
	if err := sm.Close(); err != nil {
		t.Fatalf("Failed to close StatusManager: %v", err)
	}

	// 验证 context 已被取消
	select {
	case <-ctx.Done():
		// 符合预期
	case <-time.After(1 * time.Second):
		t.Error("Context should be cancelled after Close()")
	}
}
