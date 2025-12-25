package nutsdb

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Status 表示数据库的运行状态
type Status int

const (
	StatusInitializing Status = iota
	StatusOpen
	StatusClosing
	StatusClosed
)

// StatusManager 管理数据库及其组件的生命周期和状态
type StatusManager struct {
	// 状态机
	state atomic.Value // Status

	// 组件注册表
	components   sync.Map     // map[string]*ComponentWrapper
	componentsMu sync.RWMutex // 仅用于组件注册时的顺序保证

	// 组件名称列表（用于保持注册顺序）
	componentNames []string

	// 生命周期控制
	ctx    context.Context
	cancel context.CancelFunc

	// 并发控制
	wg sync.WaitGroup

	// 状态转换日志
	transitionLog *StateTransitionLog

	// 配置
	config StatusManagerConfig

	// 错误处理
	errorHandler ComponentErrorHandler

	// 关闭进度
	shutdownProgress *ShutdownProgress
}

// StatusManagerConfig 配置选项
type StatusManagerConfig struct {
	ShutdownTimeout time.Duration // 关闭超时时间
}

// DefaultStatusManagerConfig 返回默认配置
func DefaultStatusManagerConfig() StatusManagerConfig {
	return StatusManagerConfig{
		ShutdownTimeout: 30 * time.Second,
	}
}

// NewStatusManager 创建新的 StatusManager
func NewStatusManager(config StatusManagerConfig) *StatusManager {
	ctx, cancel := context.WithCancel(context.Background())

	sm := &StatusManager{
		componentNames:   make([]string, 0),
		ctx:              ctx,
		cancel:           cancel,
		transitionLog:    NewStateTransitionLog(100),
		config:           config,
		errorHandler:     NewDefaultErrorHandler(nil),
		shutdownProgress: nil,
	}

	// 初始化状态为 Initializing
	sm.state.Store(StatusInitializing)

	return sm
}

// Status 返回当前数据库状态
func (sm *StatusManager) Status() Status {
	if v := sm.state.Load(); v != nil {
		return v.(Status)
	}
	return StatusClosed
}

// IsOperational 检查数据库是否可操作
// 只有在 Open 状态时才返回 true
func (sm *StatusManager) IsOperational() bool {
	return sm.Status() == StatusOpen
}

// Context 返回 StatusManager 的 context
// 组件应该使用此 context 监听关闭信号
func (sm *StatusManager) Context() context.Context {
	return sm.ctx
}

// GetComponentStatus 获取指定组件的状态
func (sm *StatusManager) GetComponentStatus(name string) (ComponentStatus, error) {
	value, exists := sm.components.Load(name)
	if !exists {
		return ComponentStatusStopped, fmt.Errorf("component %s not found", name)
	}

	wrapper := value.(*ComponentWrapper)
	return wrapper.GetStatus(), nil
}

// String 返回状态的字符串表示
func (s Status) String() string {
	switch s {
	case StatusInitializing:
		return "Initializing"
	case StatusOpen:
		return "Open"
	case StatusClosing:
		return "Closing"
	case StatusClosed:
		return "Closed"
	default:
		return "Unknown"
	}
}

// RegisterComponent 注册组件到 StatusManager
// name: 组件名称（必须唯一）
// component: 实现 Component 接口的组件
func (sm *StatusManager) RegisterComponent(name string, component Component) error {
	sm.componentsMu.Lock()
	defer sm.componentsMu.Unlock()

	// 检查组件是否已注册
	if _, exists := sm.components.Load(name); exists {
		return fmt.Errorf("component %s already registered", name)
	}

	// 创建组件包装器并注册
	wrapper := NewComponentWrapper(component)
	sm.components.Store(name, wrapper)
	sm.componentNames = append(sm.componentNames, name)

	return nil
}

// getComponentWrapper 获取组件包装器（内部使用）
// 优化：使用 sync.Map 避免锁竞争
func (sm *StatusManager) getComponentWrapper(name string) (*ComponentWrapper, error) {
	value, exists := sm.components.Load(name)
	if !exists {
		return nil, fmt.Errorf("component %s not found", name)
	}

	return value.(*ComponentWrapper), nil
}

// getAllComponents 获取所有组件名称（内部使用）
// 优化：直接返回预先存储的组件名称列表
func (sm *StatusManager) getAllComponents() []string {
	sm.componentsMu.RLock()
	defer sm.componentsMu.RUnlock()

	// 返回副本以避免并发修改
	names := make([]string, len(sm.componentNames))
	copy(names, sm.componentNames)
	return names
}

// transitionTo 执行状态转换
// 验证状态转换的有效性，记录转换日志
func (sm *StatusManager) transitionTo(toState Status, reason string) error {
	fromState := sm.Status()

	// 验证状态转换的有效性
	if !sm.isValidTransition(fromState, toState) {
		err := fmt.Errorf("invalid state transition from %s to %s", fromState, toState)
		sm.transitionLog.Record(StateTransition{
			FromState: fromState,
			ToState:   toState,
			Timestamp: time.Now(),
			Reason:    reason,
			Success:   false,
			Error:     err,
		})
		return err
	}

	// 执行状态转换
	sm.state.Store(toState)

	// 记录成功的状态转换
	sm.transitionLog.Record(StateTransition{
		FromState: fromState,
		ToState:   toState,
		Timestamp: time.Now(),
		Reason:    reason,
		Success:   true,
		Error:     nil,
	})

	return nil
}

// isValidTransition 检查状态转换是否有效
// 状态转换规则：
// - Initializing -> Open (所有组件启动成功)
// - Open -> Closing (调用 Close)
// - Closing -> Closed (所有组件停止)
// - Closed -> Closed (幂等，允许重复关闭)
func (sm *StatusManager) isValidTransition(from, to Status) bool {
	switch from {
	case StatusInitializing:
		return to == StatusOpen
	case StatusOpen:
		return to == StatusClosing
	case StatusClosing:
		return to == StatusClosed
	case StatusClosed:
		return to == StatusClosed // 幂等
	default:
		return false
	}
}

// GetStateTransitionLog 获取状态转换日志
func (sm *StatusManager) GetStateTransitionLog() *StateTransitionLog {
	return sm.transitionLog
}

// GetRecentTransitions 获取最近的 n 条状态转换记录
func (sm *StatusManager) GetRecentTransitions(n int) []StateTransition {
	return sm.transitionLog.GetRecent(n)
}

// Start 启动所有注册的组件
// 按照注册顺序启动组件，如果任何组件启动失败，回滚已启动的组件
func (sm *StatusManager) Start() error {
	currentStatus := sm.Status()
	if currentStatus != StatusInitializing {
		return fmt.Errorf("cannot start: current status is %s, expected Initializing", currentStatus)
	}

	// 获取所有组件名称（按注册顺序）
	componentNames := sm.getAllComponents()

	// 记录已启动的组件，用于失败时回滚
	startedComponents := make([]string, 0, len(componentNames))

	// 按顺序启动组件
	for _, name := range componentNames {
		wrapper, err := sm.getComponentWrapper(name)
		if err != nil {
			sm.rollbackStartup(startedComponents)
			return err
		}

		component := wrapper.GetComponent()

		wrapper.SetStatus(ComponentStatusStarting)

		// 启动组件
		if err := component.Start(sm.ctx); err != nil {
			wrapper.SetStatus(ComponentStatusFailed)
			wrapper.SetLastError(err)

			// 处理启动错误
			wrappedErr := sm.errorHandler.HandleStartupError(name, err)

			// 回滚已启动的组件
			sm.rollbackStartup(startedComponents)

			return wrappedErr
		}

		// 标记组件为运行状态
		wrapper.SetStatus(ComponentStatusRunning)
		wrapper.SetStartTime(time.Now())
		startedComponents = append(startedComponents, name)
	}

	// 所有组件启动成功，转换到 Open 状态
	if err := sm.transitionTo(StatusOpen, "all components started successfully"); err != nil {
		sm.rollbackStartup(startedComponents)
		return err
	}

	return nil
}

// rollbackStartup 回滚已启动的组件
// 按照启动顺序的逆序停止组件
func (sm *StatusManager) rollbackStartup(startedComponents []string) {
	// 逆序停止组件
	for i := len(startedComponents) - 1; i >= 0; i-- {
		name := startedComponents[i]
		wrapper, err := sm.getComponentWrapper(name)
		if err != nil {
			continue
		}

		component := wrapper.GetComponent()
		wrapper.SetStatus(ComponentStatusStopping)

		// 使用较短的超时时间进行回滚
		if err := component.Stop(5 * time.Second); err != nil {
			wrapper.SetStatus(ComponentStatusFailed)
			wrapper.SetLastError(err)
		} else {
			wrapper.SetStatus(ComponentStatusStopped)
			wrapper.SetStopTime(time.Now())
		}
	}
}

// Close 关闭所有组件并清理资源
// 按照注册顺序的逆序关闭组件，实现优雅关闭
func (sm *StatusManager) Close() error {
	// Use mutex to prevent concurrent Close() calls from racing
	sm.componentsMu.Lock()
	currentStatus := sm.Status()

	// Idempotent: if already Closed, return immediately
	if currentStatus == StatusClosed {
		sm.componentsMu.Unlock()
		return nil
	}

	// If already closing, wait for completion
	if currentStatus == StatusClosing {
		sm.componentsMu.Unlock()
		return sm.waitForClosed()
	}

	// Transition to Closing state while holding the lock
	if err := sm.transitionTo(StatusClosing, "Close() called"); err != nil {
		sm.componentsMu.Unlock()
		return err
	}
	sm.componentsMu.Unlock()

	// 取消 context，通知所有组件开始关闭
	sm.cancel()

	// 获取所有组件名称，然后反转顺序（关闭顺序是注册顺序的逆序）
	componentNames := sm.getAllComponents()

	// 反转顺序
	shutdownOrder := make([]string, len(componentNames))
	for i, name := range componentNames {
		shutdownOrder[len(componentNames)-1-i] = name
	}

	// 初始化关闭进度跟踪
	sm.shutdownProgress = NewShutdownProgress(len(shutdownOrder))
	sm.shutdownProgress.SetCurrentPhase("Stopping components")

	// 创建超时 context
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), sm.config.ShutdownTimeout)
	defer shutdownCancel()

	// 使用 channel 跟踪关闭完成
	doneCh := make(chan struct{})
	go func() {
		sm.shutdownComponents(shutdownOrder)
		close(doneCh)
	}()

	// 等待关闭完成或超时
	select {
	case <-doneCh:
	case <-shutdownCtx.Done():
		sm.shutdownProgress.SetCurrentPhase("Timeout - forcing closure")
	}

	// 等待所有 goroutine 完成
	sm.wg.Wait()

	// 转换到 Closed 状态
	sm.transitionTo(StatusClosed, "all components stopped")

	return nil
}

// shutdownComponents 按顺序关闭组件
func (sm *StatusManager) shutdownComponents(order []string) {
	// Handle empty component list
	if len(order) == 0 {
		sm.shutdownProgress.SetCurrentPhase("Complete")
		return
	}

	for _, name := range order {
		wrapper, err := sm.getComponentWrapper(name)
		if err != nil {
			sm.shutdownProgress.AddFailedComponent(name)
			sm.shutdownProgress.IncrementStopped()
			continue
		}

		component := wrapper.GetComponent()
		wrapper.SetStatus(ComponentStatusStopping)

		// Calculate remaining timeout per component
		remainingTimeout := sm.config.ShutdownTimeout / time.Duration(len(order))
		if remainingTimeout < time.Second {
			remainingTimeout = time.Second
		}

		// Always try to stop the component - Stop() should be idempotent
		// This handles cases where components are registered after Start() and started manually
		if err := component.Stop(remainingTimeout); err != nil {
			wrapper.SetStatus(ComponentStatusFailed)
			wrapper.SetLastError(err)
			sm.errorHandler.HandleShutdownError(name, err)
			sm.shutdownProgress.AddFailedComponent(name)
		} else {
			wrapper.SetStatus(ComponentStatusStopped)
			wrapper.SetStopTime(time.Now())
		}

		sm.shutdownProgress.IncrementStopped()
	}

	sm.shutdownProgress.SetCurrentPhase("Complete")
}

// waitForClosed 等待关闭完成
func (sm *StatusManager) waitForClosed() error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	timeout := time.After(sm.config.ShutdownTimeout)

	for {
		select {
		case <-ticker.C:
			if sm.Status() == StatusClosed {
				return nil
			}
		case <-timeout:
			return fmt.Errorf("timeout waiting for StatusManager to close")
		}
	}
}

// GetShutdownProgress 获取关闭进度
// Returns nil if shutdown has not started yet
func (sm *StatusManager) GetShutdownProgress() *ShutdownProgress {
	return sm.shutdownProgress
}

// GetSystemHealth 获取系统整体健康状态
// 优化：避免持有锁时执行健康检查
func (sm *StatusManager) GetSystemHealth() *SystemHealth {
	health := NewSystemHealth(sm.Status())

	// 收集所有组件信息
	sm.components.Range(func(key, value interface{}) bool {
		name := key.(string)
		wrapper := value.(*ComponentWrapper)
		status := wrapper.GetStatus()

		componentHealth := HealthStatus{
			ComponentName: name,
			Status:        status,
			Healthy:       status == ComponentStatusRunning,
			LastCheck:     time.Now(),
			LastError:     nil,
			Uptime:        wrapper.GetUptime(),
			Metadata:      make(map[string]interface{}),
		}

		// 添加额外的元数据
		componentHealth.Metadata["start_time"] = wrapper.GetStartTime()
		if !wrapper.GetStopTime().IsZero() {
			componentHealth.Metadata["stop_time"] = wrapper.GetStopTime()
		}
		if lastErr := wrapper.GetLastError(); lastErr != nil {
			componentHealth.Metadata["last_error"] = lastErr.Error()
		}

		health.AddComponentHealth(componentHealth)
		return true
	})

	return health
}

// Add 添加 delta 到 WaitGroup 计数器
// 组件应该在启动 goroutine 时调用此方法
func (sm *StatusManager) Add(delta int) {
	sm.wg.Add(delta)
}

// Done 减少 WaitGroup 计数器
// 组件应该在 goroutine 完成时调用此方法
func (sm *StatusManager) Done() {
	sm.wg.Done()
}
