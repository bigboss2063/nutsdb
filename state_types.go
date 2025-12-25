package nutsdb

import (
	"sync"
	"time"
)

// Note: Status type and constants are already defined in status_manager.go
// We reuse those definitions for the new state management system

// StateTransition 记录状态转换事件
type StateTransition struct {
	FromState Status
	ToState   Status
	Timestamp time.Time
	Reason    string
	Success   bool
	Error     error
}

// StateTransitionLog 状态转换日志
type StateTransitionLog struct {
	transitions []StateTransition
	mu          sync.RWMutex
	maxSize     int
}

// NewStateTransitionLog 创建新的状态转换日志
func NewStateTransitionLog(maxSize int) *StateTransitionLog {
	if maxSize <= 0 {
		maxSize = 100 // 默认保留最近 100 条记录
	}
	return &StateTransitionLog{
		transitions: make([]StateTransition, 0, maxSize),
		maxSize:     maxSize,
	}
}

// Record 记录状态转换
func (stl *StateTransitionLog) Record(transition StateTransition) {
	stl.mu.Lock()
	defer stl.mu.Unlock()

	stl.transitions = append(stl.transitions, transition)

	// 如果超过最大大小，删除最旧的记录
	if len(stl.transitions) > stl.maxSize {
		stl.transitions = stl.transitions[len(stl.transitions)-stl.maxSize:]
	}
}

// GetRecent 获取最近的 n 条状态转换记录
func (stl *StateTransitionLog) GetRecent(n int) []StateTransition {
	stl.mu.RLock()
	defer stl.mu.RUnlock()

	if n <= 0 || n > len(stl.transitions) {
		n = len(stl.transitions)
	}

	result := make([]StateTransition, n)
	copy(result, stl.transitions[len(stl.transitions)-n:])
	return result
}

// GetAll 获取所有状态转换记录
func (stl *StateTransitionLog) GetAll() []StateTransition {
	stl.mu.RLock()
	defer stl.mu.RUnlock()

	result := make([]StateTransition, len(stl.transitions))
	copy(result, stl.transitions)
	return result
}

// Clear 清空所有状态转换记录
func (stl *StateTransitionLog) Clear() {
	stl.mu.Lock()
	defer stl.mu.Unlock()
	stl.transitions = make([]StateTransition, 0, stl.maxSize)
}

// HealthStatus 组件健康状态
type HealthStatus struct {
	ComponentName string
	Status        ComponentStatus
	Healthy       bool
	LastCheck     time.Time
	LastError     error
	Uptime        time.Duration
	Metadata      map[string]interface{}
}

// SystemHealth 系统整体健康状态
type SystemHealth struct {
	DBStatus   Status
	Healthy    bool
	Components map[string]HealthStatus
	Timestamp  time.Time
}

// NewSystemHealth 创建新的系统健康状态
func NewSystemHealth(dbStatus Status) *SystemHealth {
	return &SystemHealth{
		DBStatus:   dbStatus,
		Healthy:    true,
		Components: make(map[string]HealthStatus),
		Timestamp:  time.Now(),
	}
}

// AddComponentHealth 添加组件健康状态
func (sh *SystemHealth) AddComponentHealth(health HealthStatus) {
	sh.Components[health.ComponentName] = health
	// 如果任何组件不健康，系统整体标记为不健康
	if !health.Healthy {
		sh.Healthy = false
	}
}

// IsHealthy 检查系统是否健康
func (sh *SystemHealth) IsHealthy() bool {
	return sh.Healthy && sh.DBStatus == StatusOpen
}

// ShutdownProgress 关闭进度跟踪
type ShutdownProgress struct {
	TotalComponents     int
	StoppedComponents   int
	FailedComponents    []string
	StartTime           time.Time
	EstimatedCompletion time.Time
	CurrentPhase        string
	mu                  sync.RWMutex
}

// NewShutdownProgress 创建新的关闭进度跟踪器
func NewShutdownProgress(totalComponents int) *ShutdownProgress {
	return &ShutdownProgress{
		TotalComponents:   totalComponents,
		StoppedComponents: 0,
		FailedComponents:  make([]string, 0),
		StartTime:         time.Now(),
		CurrentPhase:      "Initializing",
	}
}

// IncrementStopped 增加已停止组件计数
func (sp *ShutdownProgress) IncrementStopped() {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.StoppedComponents++
}

// AddFailedComponent 添加失败的组件
func (sp *ShutdownProgress) AddFailedComponent(componentName string) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.FailedComponents = append(sp.FailedComponents, componentName)
}

// SetCurrentPhase 设置当前阶段
func (sp *ShutdownProgress) SetCurrentPhase(phase string) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.CurrentPhase = phase
}

// SetEstimatedCompletion 设置预计完成时间
func (sp *ShutdownProgress) SetEstimatedCompletion(t time.Time) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.EstimatedCompletion = t
}

// GetProgress 获取进度百分比 (0-100)
func (sp *ShutdownProgress) GetProgress() float64 {
	sp.mu.RLock()
	defer sp.mu.RUnlock()

	if sp.TotalComponents == 0 {
		return 100.0
	}
	return float64(sp.StoppedComponents) / float64(sp.TotalComponents) * 100.0
}

// GetElapsedTime 获取已用时间
func (sp *ShutdownProgress) GetElapsedTime() time.Duration {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	return time.Since(sp.StartTime)
}

// IsComplete 检查是否完成
func (sp *ShutdownProgress) IsComplete() bool {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	return sp.StoppedComponents >= sp.TotalComponents
}

// GetFailedComponents 获取失败的组件列表
func (sp *ShutdownProgress) GetFailedComponents() []string {
	sp.mu.RLock()
	defer sp.mu.RUnlock()

	result := make([]string, len(sp.FailedComponents))
	copy(result, sp.FailedComponents)
	return result
}

// GetCurrentPhase 获取当前阶段
func (sp *ShutdownProgress) GetCurrentPhase() string {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	return sp.CurrentPhase
}
