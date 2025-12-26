package nutsdb

import (
	"sync"
	"time"
)

// Note: Status type and constants are already defined in status_manager.go
// We reuse those definitions for the new state management system

// ComponentStatus 组件状态
type ComponentStatus int

const (
	ComponentStatusStopped ComponentStatus = iota
	ComponentStatusStarting
	ComponentStatusRunning
	ComponentStatusStopping
	ComponentStatusFailed
)

// String 返回组件状态的字符串表示
func (cs ComponentStatus) String() string {
	switch cs {
	case ComponentStatusStopped:
		return "Stopped"
	case ComponentStatusStarting:
		return "Starting"
	case ComponentStatusRunning:
		return "Running"
	case ComponentStatusStopping:
		return "Stopping"
	case ComponentStatusFailed:
		return "Failed"
	default:
		return "Unknown"
	}
}

// ShutdownProgress 关闭进度跟踪（简化版）
type ShutdownProgress struct {
	TotalComponents   int
	StoppedComponents int
	FailedComponents  []string
	StartTime         time.Time
	CurrentPhase      string
	mu                sync.RWMutex
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
