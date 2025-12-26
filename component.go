package nutsdb

import (
	"context"
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

// ComponentWrapper 组件包装器，用于管理组件的状态
type ComponentWrapper struct {
	component Component
	status    atomic.Value // ComponentStatus
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
