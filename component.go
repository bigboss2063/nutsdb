package nutsdb

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Component 定义组件的标准生命周期接口
type Component interface {
	// Name 返回组件名称
	Name() string

	// Start 启动组件
	// ctx: 用于监听关闭信号的 context
	// 返回错误表示启动失败
	Start(ctx context.Context) error

	// Stop 停止组件
	// timeout: 停止超时时间
	// 返回错误表示停止失败（但不应阻止其他组件关闭）
	Stop(timeout time.Duration) error
}

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

// ComponentWrapper 组件包装器，用于管理组件的状态
type ComponentWrapper struct {
	component Component
	status    atomic.Value // ComponentStatus
	startTime time.Time
	stopTime  time.Time
	lastError error
	mu        sync.RWMutex
}

// NewComponentWrapper 创建新的组件包装器
func NewComponentWrapper(component Component) *ComponentWrapper {
	cw := &ComponentWrapper{
		component: component,
	}
	cw.status.Store(ComponentStatusStopped)
	return cw
}

// GetComponent 返回被包装的组件
func (cw *ComponentWrapper) GetComponent() Component {
	return cw.component
}

// GetStatus 返回当前组件状态
func (cw *ComponentWrapper) GetStatus() ComponentStatus {
	return cw.status.Load().(ComponentStatus)
}

// SetStatus 设置组件状态
func (cw *ComponentWrapper) SetStatus(status ComponentStatus) {
	cw.status.Store(status)
}

// SetStartTime 设置启动时间
func (cw *ComponentWrapper) SetStartTime(t time.Time) {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	cw.startTime = t
}

// GetStartTime 获取启动时间
func (cw *ComponentWrapper) GetStartTime() time.Time {
	cw.mu.RLock()
	defer cw.mu.RUnlock()
	return cw.startTime
}

// SetStopTime 设置停止时间
func (cw *ComponentWrapper) SetStopTime(t time.Time) {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	cw.stopTime = t
}

// GetStopTime 获取停止时间
func (cw *ComponentWrapper) GetStopTime() time.Time {
	cw.mu.RLock()
	defer cw.mu.RUnlock()
	return cw.stopTime
}

// SetLastError 设置最后的错误
func (cw *ComponentWrapper) SetLastError(err error) {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	cw.lastError = err
}

// GetLastError 获取最后的错误
func (cw *ComponentWrapper) GetLastError() error {
	cw.mu.RLock()
	defer cw.mu.RUnlock()
	return cw.lastError
}

// GetUptime 获取组件运行时间
func (cw *ComponentWrapper) GetUptime() time.Duration {
	cw.mu.RLock()
	defer cw.mu.RUnlock()

	if cw.startTime.IsZero() {
		return 0
	}

	if !cw.stopTime.IsZero() {
		return cw.stopTime.Sub(cw.startTime)
	}

	return time.Since(cw.startTime)
}
