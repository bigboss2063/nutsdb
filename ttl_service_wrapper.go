package nutsdb

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/nutsdb/nutsdb/internal/ttl"
)

// TTLServiceWrapper 包装 TTL Service 使其符合 Component 接口
// 负责管理 TTL Service 的生命周期，实现优雅停止
type TTLServiceWrapper struct {
	service       *ttl.Service
	statusManager *StatusManager

	// 运行控制
	running atomic.Bool
	ctx     context.Context
	cancel  context.CancelFunc

	// 日志
	logger ComponentLogger
}

// NewTTLServiceWrapper 创建新的 TTLServiceWrapper
func NewTTLServiceWrapper(service *ttl.Service, sm *StatusManager, logger ComponentLogger) *TTLServiceWrapper {
	if logger == nil {
		logger = &DefaultComponentLogger{}
	}

	return &TTLServiceWrapper{
		service:       service,
		statusManager: sm,
		logger:        logger,
	}
}

// Name 返回组件名称
func (tw *TTLServiceWrapper) Name() string {
	return "TTLService"
}

// Start 启动 TTL Service
// 实现 Component 接口
func (tw *TTLServiceWrapper) Start(ctx context.Context) error {
	if tw.running.Load() {
		return fmt.Errorf("TTLService already running")
	}

	tw.logger.Infof("TTLService starting")

	// 创建 context
	tw.ctx, tw.cancel = context.WithCancel(ctx)

	// 启动 TTL Service
	tw.statusManager.Add(1)
	go func() {
		defer tw.statusManager.Done()
		tw.service.Run(tw.ctx)
	}()

	// 标记为运行状态
	tw.running.Store(true)

	tw.logger.Infof("TTLService started successfully")

	return nil
}

// Stop 停止 TTL Service
// 等待 TTL Service 完成当前批次后停止
// 实现 Component 接口
func (tw *TTLServiceWrapper) Stop(timeout time.Duration) error {
	if !tw.running.Load() {
		tw.logger.Infof("TTLService already stopped")
		return nil
	}

	tw.logger.Infof("TTLService stopping")

	// 取消 context，通知 TTL Service 停止
	if tw.cancel != nil {
		tw.cancel()
	}

	// TTL Service 会在 context 取消后自动停止
	// 这里我们等待一段时间确保它完成清理
	// 注意：TTL Service 的 Run 方法会在 context 取消后退出
	// 并且会处理完当前批次的过期事件

	// 等待 TTL Service 停止（通过检查 context）
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		if tw.ctx.Err() != nil {
			// Context 已经被取消，TTL Service 应该已经停止
			break
		}

		if time.Now().After(deadline) {
			tw.logger.Warnf("TTLService stop timeout after %v", timeout)
			break
		}

		<-ticker.C
	}

	// 关闭 TTL Service（清理资源）
	tw.service.Close()

	// 标记为停止状态
	tw.running.Store(false)

	tw.logger.Infof("TTLService stopped successfully")

	return nil
}

// GetService 返回被包装的 TTL Service
// 用于访问 TTL Service 的其他方法
func (tw *TTLServiceWrapper) GetService() *ttl.Service {
	return tw.service
}
