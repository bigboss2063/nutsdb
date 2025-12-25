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

	// 日志
	logger ComponentLogger
}

// NewWatchManagerWrapper 创建新的 WatchManagerWrapper
func NewWatchManagerWrapper(manager *watchManager, sm *StatusManager, logger ComponentLogger) *WatchManagerWrapper {
	if logger == nil {
		logger = &DefaultComponentLogger{}
	}

	return &WatchManagerWrapper{
		manager:       manager,
		statusManager: sm,
		logger:        logger,
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

	ww.logger.Infof("WatchManager starting")

	// Watch Manager 在创建时已经初始化
	// 这里只需要启动 distributor goroutine
	ww.statusManager.Add(1)
	go func() {
		defer ww.statusManager.Done()
		ww.manager.startDistributor()
	}()

	// 标记为运行状态
	ww.running.Store(true)

	ww.logger.Infof("WatchManager started successfully")

	return nil
}

// Stop 停止 Watch Manager
// 通知所有订阅者数据库即将关闭，并关闭所有订阅通道
// 实现 Component 接口
func (ww *WatchManagerWrapper) Stop(timeout time.Duration) error {
	if !ww.running.Load() {
		ww.logger.Infof("WatchManager already stopped")
		return nil
	}

	ww.logger.Infof("WatchManager stopping, notifying all subscribers")

	// 关闭 Watch Manager
	// 这会取消 context，通知所有 goroutine 停止
	if err := ww.manager.close(); err != nil {
		ww.logger.Warnf("WatchManager close error: %v", err)
	}

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
			ww.logger.Warnf("WatchManager stop timeout after %v", timeout)
			break
		}

		<-ticker.C
	}

	// 标记为停止状态
	ww.running.Store(false)

	ww.logger.Infof("WatchManager stopped successfully")

	return nil
}

// GetManager 返回被包装的 Watch Manager
// 用于访问 Watch Manager 的其他方法
func (ww *WatchManagerWrapper) GetManager() *watchManager {
	return ww.manager
}
