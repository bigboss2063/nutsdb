package nutsdb

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"
	"time"
)

// MergeWorker 管理数据文件合并操作
// 实现 Component 接口，负责定期或手动触发的合并操作
type MergeWorker struct {
	db            *DB
	statusManager *StatusManager

	// 合并控制
	mergeStartCh chan struct{}
	mergeEndCh   chan error
	isMerging    atomic.Bool

	// 定时器
	ticker *time.Ticker

	// 配置
	config MergeConfig

	// 运行控制
	ctx    context.Context
	cancel context.CancelFunc
	doneCh chan struct{}
}

// MergeConfig 合并配置
type MergeConfig struct {
	MergeInterval   time.Duration // 自动合并间隔，0 表示禁用自动合并
	EnableAutoMerge bool          // 是否启用自动合并
}

// DefaultMergeConfig 返回默认合并配置
func DefaultMergeConfig() MergeConfig {
	return MergeConfig{
		MergeInterval:   0,     // 默认禁用自动合并
		EnableAutoMerge: false, // 默认禁用自动合并
	}
}

// NewMergeWorker 创建新的 MergeWorker
func NewMergeWorker(db *DB, sm *StatusManager, config MergeConfig) *MergeWorker {
	return &MergeWorker{
		db:            db,
		statusManager: sm,
		mergeStartCh:  make(chan struct{}),
		mergeEndCh:    make(chan error, 1),
		config:        config,
		doneCh:        make(chan struct{}),
	}
}

// Name 返回组件名称
func (mw *MergeWorker) Name() string {
	return "MergeWorker"
}

// Start 启动 MergeWorker
// 实现 Component 接口
func (mw *MergeWorker) Start(ctx context.Context) error {
	// 创建 context
	mw.ctx, mw.cancel = context.WithCancel(ctx)

	// 初始化定时器
	if mw.config.EnableAutoMerge && mw.config.MergeInterval > 0 {
		mw.ticker = time.NewTicker(mw.config.MergeInterval)
	} else {
		// 创建一个永不触发的 ticker
		mw.ticker = time.NewTicker(math.MaxInt64)
		mw.ticker.Stop()
	}

	// 启动工作 goroutine
	mw.statusManager.Add(1)
	go mw.run()

	return nil
}

// Stop 停止 MergeWorker
// 等待当前合并操作完成或超时
// 实现 Component 接口
func (mw *MergeWorker) Stop(timeout time.Duration) error {
	// 取消 context，通知工作 goroutine 停止
	if mw.cancel != nil {
		mw.cancel()
	}

	// 停止定时器
	if mw.ticker != nil {
		mw.ticker.Stop()
	}

	// 等待工作 goroutine 完成或超时
	select {
	case <-mw.doneCh:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("MergeWorker stop timeout")
	}
}

// TriggerMerge 手动触发合并操作
// 如果数据库正在关闭或已关闭，则拒绝合并请求
func (mw *MergeWorker) TriggerMerge() error {
	// 检查数据库状态
	status := mw.statusManager.Status()
	if status == StatusClosing || status == StatusClosed {
		return ErrDBClosed
	}

	// 检查是否已经在合并中
	if mw.isMerging.Load() {
		return ErrIsMerging
	}

	// 发送合并请求
	select {
	case mw.mergeStartCh <- struct{}{}:
		// 等待合并完成
		return <-mw.mergeEndCh
	case <-mw.ctx.Done():
		return ErrDBClosed
	}
}

// IsMerging 返回是否正在合并
func (mw *MergeWorker) IsMerging() bool {
	return mw.isMerging.Load()
}

// run 是 MergeWorker 的主循环
// 监听合并请求和定时器事件
func (mw *MergeWorker) run() {
	defer mw.statusManager.Done()
	defer close(mw.doneCh)

	for {
		select {
		case <-mw.ctx.Done():
			// 收到关闭信号，退出主循环
			return

		case <-mw.mergeStartCh:
			// 收到手动合并请求

			// 在执行合并前再次检查关闭信号
			select {
			case <-mw.ctx.Done():
				return
			default:
			}

			// 执行合并
			err := mw.performMerge()

			// 发送合并结果（非阻塞）
			select {
			case mw.mergeEndCh <- err:
			default:
			}

			// 如果启用了自动合并，重置定时器
			if mw.config.EnableAutoMerge && mw.config.MergeInterval > 0 {
				mw.ticker.Reset(mw.config.MergeInterval)
			}

		case <-mw.ticker.C:
			// 定时器触发自动合并

			// 在执行合并前检查关闭信号
			select {
			case <-mw.ctx.Done():
				return
			default:
			}

			// 执行合并（忽略错误，因为是自动合并）
			_ = mw.performMerge()
		}
	}
}

// performMerge 执行实际的合并操作
func (mw *MergeWorker) performMerge() error {
	// 检查数据库状态
	status := mw.statusManager.Status()
	if status == StatusClosing || status == StatusClosed {
		return ErrDBClosed
	}

	// 设置合并标志
	if !mw.isMerging.CompareAndSwap(false, true) {
		return ErrIsMerging
	}
	defer mw.isMerging.Store(false)

	// 执行合并
	err := mw.db.merge()

	return err
}

// SetMergeInterval 设置合并间隔
// 如果 interval > 0，则启用自动合并；否则禁用
func (mw *MergeWorker) SetMergeInterval(interval time.Duration) {
	mw.config.MergeInterval = interval

	if interval > 0 {
		mw.config.EnableAutoMerge = true
		if mw.ticker != nil {
			mw.ticker.Reset(interval)
		}
	} else {
		mw.config.EnableAutoMerge = false
		if mw.ticker != nil {
			mw.ticker.Stop()
		}
	}
}

// GetMergeInterval 获取合并间隔
func (mw *MergeWorker) GetMergeInterval() time.Duration {
	return mw.config.MergeInterval
}
