package nutsdb

import (
	"fmt"
	"log"
)

// ComponentErrorHandler 组件错误处理接口
// Note: 使用不同的名称以避免与 options.go 中的 ErrorHandler 冲突
type ComponentErrorHandler interface {
	// HandleStartupError 处理启动错误
	// component: 组件名称
	// err: 错误信息
	// 返回处理后的错误（可能包装或修改原错误）
	HandleStartupError(component string, err error) error

	// HandleRuntimeError 处理运行时错误
	// component: 组件名称
	// err: 错误信息
	HandleRuntimeError(component string, err error)

	// HandleShutdownError 处理关闭错误
	// component: 组件名称
	// err: 错误信息
	HandleShutdownError(component string, err error)
}

// RecoveryStrategy 错误恢复策略
type RecoveryStrategy int

const (
	RecoveryIgnore   RecoveryStrategy = iota // 忽略错误
	RecoveryRetry                            // 重试操作
	RecoveryRestart                          // 重启组件
	RecoveryShutdown                         // 触发优雅关闭
)

// String 返回恢复策略的字符串表示
func (rs RecoveryStrategy) String() string {
	switch rs {
	case RecoveryIgnore:
		return "Ignore"
	case RecoveryRetry:
		return "Retry"
	case RecoveryRestart:
		return "Restart"
	case RecoveryShutdown:
		return "Shutdown"
	default:
		return "Unknown"
	}
}

// ComponentLogger 组件日志接口
type ComponentLogger interface {
	Errorf(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Debugf(format string, args ...interface{})
}

// DefaultComponentLogger 默认日志实现
type DefaultComponentLogger struct{}

// Errorf 记录错误日志
func (dl *DefaultComponentLogger) Errorf(format string, args ...interface{}) {
	log.Printf("[ERROR] "+format, args...)
}

// Warnf 记录警告日志
func (dl *DefaultComponentLogger) Warnf(format string, args ...interface{}) {
	log.Printf("[WARN] "+format, args...)
}

// Infof 记录信息日志
func (dl *DefaultComponentLogger) Infof(format string, args ...interface{}) {
	// log.Printf("[INFO] "+format, args...)
}

// Debugf 记录调试日志
func (dl *DefaultComponentLogger) Debugf(format string, args ...interface{}) {
	// log.Printf("[DEBUG] "+format, args...)
}

// DefaultErrorHandler 默认错误处理器
type DefaultErrorHandler struct {
	logger       ComponentLogger
	onFatalError func(error) // 致命错误回调
}

// NewDefaultErrorHandler 创建默认错误处理器
func NewDefaultErrorHandler(logger ComponentLogger, onFatalError func(error)) *DefaultErrorHandler {
	if logger == nil {
		logger = &DefaultComponentLogger{}
	}
	return &DefaultErrorHandler{
		logger:       logger,
		onFatalError: onFatalError,
	}
}

// HandleStartupError 处理启动错误
func (deh *DefaultErrorHandler) HandleStartupError(component string, err error) error {
	if err == nil {
		return nil
	}

	wrappedErr := fmt.Errorf("component %s failed to start: %w", component, err)
	deh.logger.Errorf("Startup error: %v", wrappedErr)

	// 启动错误通常是致命的，触发回调
	if deh.onFatalError != nil {
		deh.onFatalError(wrappedErr)
	}

	return wrappedErr
}

// HandleRuntimeError 处理运行时错误
func (deh *DefaultErrorHandler) HandleRuntimeError(component string, err error) {
	if err == nil {
		return
	}

	deh.logger.Errorf("Runtime error in component %s: %v", component, err)

	// 运行时错误可能需要恢复策略，这里只记录
	// 具体的恢复策略由调用者决定
}

// HandleShutdownError 处理关闭错误
func (deh *DefaultErrorHandler) HandleShutdownError(component string, err error) {
	if err == nil {
		return
	}

	// 关闭错误不应阻止其他组件关闭，只记录警告
	deh.logger.Warnf("Shutdown error in component %s: %v", component, err)
}

// ErrorWithRecovery 带恢复策略的错误
type ErrorWithRecovery struct {
	Err      error
	Strategy RecoveryStrategy
	Context  map[string]interface{}
}

// Error 实现 error 接口
func (ewr *ErrorWithRecovery) Error() string {
	return fmt.Sprintf("%v (recovery: %s)", ewr.Err, ewr.Strategy)
}

// Unwrap 支持错误解包
func (ewr *ErrorWithRecovery) Unwrap() error {
	return ewr.Err
}

// NewErrorWithRecovery 创建带恢复策略的错误
func NewErrorWithRecovery(err error, strategy RecoveryStrategy) *ErrorWithRecovery {
	return &ErrorWithRecovery{
		Err:      err,
		Strategy: strategy,
		Context:  make(map[string]interface{}),
	}
}

// WithContext 添加上下文信息
func (ewr *ErrorWithRecovery) WithContext(key string, value interface{}) *ErrorWithRecovery {
	ewr.Context[key] = value
	return ewr
}

// GetContext 获取上下文信息
func (ewr *ErrorWithRecovery) GetContext(key string) (interface{}, bool) {
	val, ok := ewr.Context[key]
	return val, ok
}
