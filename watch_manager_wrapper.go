package nutsdb

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

// WatchManagerWrapper 包装 Watch Manager 使其符合 Component 接口
// 负责管理 Watch Manager 的生命周期，实现优雅停止和订阅者通知
type WatchManagerWrapper struct {
	manager       *watchManager
	statusManager *StatusManager

	// 运行控制
	running atomic.Bool
	started chan struct{} // Signal when distributor is ready
}

// NewWatchManagerWrapper 创建新的 WatchManagerWrapper
func NewWatchManagerWrapper(manager *watchManager, sm *StatusManager) *WatchManagerWrapper {
	return &WatchManagerWrapper{
		manager:       manager,
		statusManager: sm,
		started:       make(chan struct{}),
	}
}

// Name 返回组件名称
func (ww *WatchManagerWrapper) Name() string {
	return "WatchManager"
}

// Start 启动 Watch Manager
// 实现 Component 接口
func (ww *WatchManagerWrapper) Start(ctx context.Context) error {
	if ww.running.Load() {
		return fmt.Errorf("WatchManager already running")
	}

	// Watch Manager 在创建时已经初始化
	// 这里只需要启动 distributor goroutine
	ww.statusManager.Add(1)
	go func() {
		defer ww.statusManager.Done()
		// Signal that distributor goroutine has started
		close(ww.started)
		ww.manager.startDistributor()
	}()

	// Wait for distributor goroutine to start before returning
	// This ensures the component is ready when Start() returns
	select {
	case <-ww.started:
		// Distributor goroutine has started
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timeout waiting for WatchManager distributor to start")
	}

	// 标记为运行状态
	ww.running.Store(true)

	return nil
}

// Stop 停止 Watch Manager
// 通知所有订阅者数据库即将关闭，并关闭所有订阅通道
// 实现 Component 接口
func (ww *WatchManagerWrapper) Stop(timeout time.Duration) error {
	if !ww.running.Load() {
		return nil
	}

	// 关闭 Watch Manager
	// 这会取消 context，通知所有 goroutine 停止
	ww.manager.close()

	// 等待 Watch Manager 完成清理
	// Watch Manager 的 startDistributor 会在 context 取消后退出
	// 并调用 cleanUpSubscribers 清理所有订阅者
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		if ww.manager.isClosed() {
			break
		}

		if time.Now().After(deadline) {
			break
		}

		<-ticker.C
	}

	// 标记为停止状态
	ww.running.Store(false)

	return nil
}

// GetManager 返回被包装的 Watch Manager
// 用于访问 Watch Manager 的其他方法
func (ww *WatchManagerWrapper) GetManager() *watchManager {
	return ww.manager
}
