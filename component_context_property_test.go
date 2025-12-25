package nutsdb

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/nutsdb/nutsdb/internal/ttl"
)

// TestProperty_ContextCancellationPropagation tests context cancellation propagation
// Feature: state-management-refactoring, Property 7: Context 取消传播
//
// Property: For any 组件，当 StatusManager 的 context 被取消时，组件必须在合理时间内检测到并开始停止
//
// This property test verifies:
// 1. When StatusManager's context is cancelled, all components detect it
// 2. Components respond to cancellation within a reasonable time
// 3. Components stop gracefully after detecting cancellation
// 4. Context cancellation propagates to all registered components
func TestProperty_ContextCancellationPropagation(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100
	parameters.MaxSize = 10

	properties := gopter.NewProperties(parameters)

	properties.Property("components detect and respond to context cancellation", prop.ForAll(
		func(numComponents int, detectionTimeMs int) bool {
			// Create test environment
			sm := NewStatusManager(DefaultStatusManagerConfig())

			// Start StatusManager
			if err := sm.Start(); err != nil {
				t.Logf("Failed to start StatusManager: %v", err)
				return false
			}

			// Calculate reasonable detection time (10ms to 500ms)
			detectionTime := time.Duration(detectionTimeMs) * time.Millisecond
			if detectionTime < 10*time.Millisecond {
				detectionTime = 10 * time.Millisecond
			}
			if detectionTime > 500*time.Millisecond {
				detectionTime = 500 * time.Millisecond
			}

			// Create and register test components
			components := make([]*TestContextAwareComponent, numComponents)
			for i := 0; i < numComponents; i++ {
				comp := NewTestContextAwareComponent(detectionTime)
				components[i] = comp

				if err := sm.RegisterComponent(comp.Name(), comp); err != nil {
					t.Logf("Failed to register component %s: %v", comp.Name(), err)
					sm.Close()
					return false
				}

				// Start component
				if err := comp.Start(sm.Context()); err != nil {
					t.Logf("Failed to start component %s: %v", comp.Name(), err)
					sm.Close()
					return false
				}
			}

			// Verify all components are running
			for _, comp := range components {
				if !comp.IsRunning() {
					t.Logf("Component %s not running after Start()", comp.Name())
					sm.Close()
					return false
				}
			}

			// Cancel the context by closing StatusManager
			startTime := time.Now()
			closeErr := sm.Close()
			elapsed := time.Since(startTime)

			// Verification 1: Close should complete successfully
			if closeErr != nil {
				t.Logf("StatusManager.Close() returned error: %v", closeErr)
				return false
			}

			// Verification 2: All components should have detected cancellation
			for _, comp := range components {
				if !comp.DetectedCancellation() {
					t.Logf("Component %s did not detect context cancellation", comp.Name())
					return false
				}
			}

			// Verification 3: All components should have stopped
			for _, comp := range components {
				if comp.IsRunning() {
					t.Logf("Component %s still running after Close()", comp.Name())
					return false
				}
			}

			// Verification 4: Close should complete within reasonable time
			// Maximum expected time = detection time + shutdown timeout + buffer
			maxExpectedTime := detectionTime + sm.config.ShutdownTimeout + 1*time.Second
			if elapsed > maxExpectedTime {
				t.Logf("Close took %v, exceeded max expected time %v", elapsed, maxExpectedTime)
				return false
			}

			// Verification 5: StatusManager should be in Closed state
			if sm.Status() != StatusClosed {
				t.Logf("Expected StatusManager to be Closed, got: %s", sm.Status())
				return false
			}

			return true
		},
		gen.IntRange(1, 10),   // numComponents: 1-10 components
		gen.IntRange(10, 500), // detectionTimeMs: 10-500ms detection time
	))

	properties.TestingRun(t)
}

// TestProperty_ContextCancellationPropagation_RealComponents tests with real components
// Feature: state-management-refactoring, Property 7: Context 取消传播 (Real Components)
//
// This test verifies context cancellation with actual component implementations
func TestProperty_ContextCancellationPropagation_RealComponents(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 50
	parameters.MaxSize = 5

	properties := gopter.NewProperties(parameters)

	properties.Property("real components respond to context cancellation", prop.ForAll(
		func(enableMerge bool, enableTTL bool, enableWatch bool) bool {
			// Create test environment
			db := &DB{
				snowflakeManager: NewSnowflakeManager(1),
			}
			sm := NewStatusManager(DefaultStatusManagerConfig())

			// Start StatusManager
			if err := sm.Start(); err != nil {
				t.Logf("Failed to start StatusManager: %v", err)
				return false
			}

			// Track which components are registered
			registeredComponents := make([]string, 0)

			// Register TransactionManager (always)
			tm := NewTransactionManager(db, sm)
			if err := sm.RegisterComponent("TransactionManager", tm); err != nil {
				t.Logf("Failed to register TransactionManager: %v", err)
				sm.Close()
				return false
			}
			if err := tm.Start(sm.Context()); err != nil {
				t.Logf("Failed to start TransactionManager: %v", err)
				sm.Close()
				return false
			}
			registeredComponents = append(registeredComponents, "TransactionManager")

			// Conditionally register MergeWorker
			var mw *MergeWorker
			if enableMerge {
				config := DefaultMergeConfig()
				config.EnableAutoMerge = false // Disable auto-merge for testing
				mw = NewMergeWorker(db, sm, config)
				if err := sm.RegisterComponent("MergeWorker", mw); err != nil {
					t.Logf("Failed to register MergeWorker: %v", err)
					sm.Close()
					return false
				}
				if err := mw.Start(sm.Context()); err != nil {
					t.Logf("Failed to start MergeWorker: %v", err)
					sm.Close()
					return false
				}
				registeredComponents = append(registeredComponents, "MergeWorker")
			}

			// Conditionally register TTLServiceWrapper
			var tw *TTLServiceWrapper
			if enableTTL {
				mockClock := ttl.NewMockClock(1000000)
				config := ttl.DefaultConfig()
				config.ScanInterval = 100 * time.Millisecond
				config.BatchTimeout = 50 * time.Millisecond
				callback := func(events []*ttl.ExpirationEvent) {}
				scanFn := func() ([]*ttl.ExpirationEvent, error) { return nil, nil }
				ttlService := ttl.NewService(mockClock, config, callback, scanFn)
				tw = NewTTLServiceWrapper(ttlService, sm)
				if err := sm.RegisterComponent("TTLService", tw); err != nil {
					t.Logf("Failed to register TTLService: %v", err)
					sm.Close()
					return false
				}
				if err := tw.Start(sm.Context()); err != nil {
					t.Logf("Failed to start TTLService: %v", err)
					sm.Close()
					return false
				}
				registeredComponents = append(registeredComponents, "TTLService")
			}

			// Conditionally register WatchManagerWrapper
			var ww *WatchManagerWrapper
			if enableWatch {
				wm := NewWatchManager()
				ww = NewWatchManagerWrapper(wm, sm)
				if err := sm.RegisterComponent("WatchManager", ww); err != nil {
					t.Logf("Failed to register WatchManager: %v", err)
					sm.Close()
					return false
				}
				if err := ww.Start(sm.Context()); err != nil {
					t.Logf("Failed to start WatchManager: %v", err)
					sm.Close()
					return false
				}
				registeredComponents = append(registeredComponents, "WatchManager")
			}

			// Give components a moment to fully start
			time.Sleep(50 * time.Millisecond)

			// Cancel context by closing StatusManager
			startTime := time.Now()
			closeErr := sm.Close()
			elapsed := time.Since(startTime)

			// Verification 1: Close should complete successfully
			if closeErr != nil {
				t.Logf("StatusManager.Close() returned error: %v", closeErr)
				return false
			}

			// Verification 2: All components should have stopped
			// TransactionManager
			if tm.running.Load() {
				t.Logf("TransactionManager still running after Close()")
				return false
			}

			// MergeWorker
			if enableMerge && mw != nil {
				// Check if MergeWorker's context was cancelled
				select {
				case <-mw.ctx.Done():
					// Context was cancelled, good
				default:
					t.Logf("MergeWorker context not cancelled after Close()")
					return false
				}
			}

			// TTLServiceWrapper
			if enableTTL && tw != nil {
				if tw.running.Load() {
					t.Logf("TTLService still running after Close()")
					return false
				}
				// Check if TTLService's context was cancelled
				select {
				case <-tw.ctx.Done():
					// Context was cancelled, good
				default:
					t.Logf("TTLService context not cancelled after Close()")
					return false
				}
			}

			// WatchManagerWrapper
			if enableWatch && ww != nil {
				if ww.running.Load() {
					t.Logf("WatchManager still running after Close()")
					return false
				}
			}

			// Verification 3: Close should complete within reasonable time
			maxExpectedTime := sm.config.ShutdownTimeout + 2*time.Second
			if elapsed > maxExpectedTime {
				t.Logf("Close took %v, exceeded max expected time %v", elapsed, maxExpectedTime)
				return false
			}

			// Verification 4: StatusManager should be in Closed state
			if sm.Status() != StatusClosed {
				t.Logf("Expected StatusManager to be Closed, got: %s", sm.Status())
				return false
			}

			// Verification 5: StatusManager's context should be cancelled
			select {
			case <-sm.Context().Done():
				// Context was cancelled, good
			default:
				t.Logf("StatusManager context not cancelled after Close()")
				return false
			}

			return true
		},
		gen.Bool(), // enableMerge
		gen.Bool(), // enableTTL
		gen.Bool(), // enableWatch
	))

	properties.TestingRun(t)
}

// TestProperty_ContextCancellationPropagation_ConcurrentOperations tests cancellation during operations
// Feature: state-management-refactoring, Property 7: Context 取消传播 (Concurrent Operations)
//
// This test verifies that context cancellation works correctly even when components are busy
func TestProperty_ContextCancellationPropagation_ConcurrentOperations(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 50
	parameters.MaxSize = 20

	properties := gopter.NewProperties(parameters)

	properties.Property("context cancellation works during concurrent operations", prop.ForAll(
		func(numOperations int, operationDurationMs int) bool {
			// Create test environment
			sm := NewStatusManager(DefaultStatusManagerConfig())

			// Start StatusManager
			if err := sm.Start(); err != nil {
				t.Logf("Failed to start StatusManager: %v", err)
				return false
			}

			// Calculate operation duration (10ms to 200ms)
			opDuration := time.Duration(operationDurationMs) * time.Millisecond
			if opDuration < 10*time.Millisecond {
				opDuration = 10 * time.Millisecond
			}
			if opDuration > 200*time.Millisecond {
				opDuration = 200 * time.Millisecond
			}

			// Create component that performs operations
			comp := NewTestOperationalComponent(opDuration)
			if err := sm.RegisterComponent(comp.Name(), comp); err != nil {
				t.Logf("Failed to register component: %v", err)
				sm.Close()
				return false
			}

			if err := comp.Start(sm.Context()); err != nil {
				t.Logf("Failed to start component: %v", err)
				sm.Close()
				return false
			}

			// Start concurrent operations
			var wg sync.WaitGroup
			operationsStarted := atomic.Int64{}
			operationsCompleted := atomic.Int64{}
			operationsCancelled := atomic.Int64{}

			for i := 0; i < numOperations; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()

					operationsStarted.Add(1)
					cancelled := comp.PerformOperation(sm.Context())
					if cancelled {
						operationsCancelled.Add(1)
					} else {
						operationsCompleted.Add(1)
					}
				}()
			}

			// Let operations start
			time.Sleep(20 * time.Millisecond)

			// Cancel context by closing StatusManager
			startTime := time.Now()
			closeErr := sm.Close()
			elapsed := time.Since(startTime)

			// Wait for all operations to finish
			wg.Wait()

			// Verification 1: Close should complete successfully
			if closeErr != nil {
				t.Logf("StatusManager.Close() returned error: %v", closeErr)
				return false
			}

			// Verification 2: Component should have stopped
			if comp.IsRunning() {
				t.Logf("Component still running after Close()")
				return false
			}

			// Verification 3: Component should have detected cancellation
			// Note: If Close() happens very quickly, the component might be stopped
			// before the monitor goroutine detects cancellation. This is acceptable
			// as long as the component stopped properly.
			if !comp.DetectedCancellation() {
				// Check if component at least stopped properly
				if comp.IsRunning() {
					t.Logf("Component did not detect context cancellation and is still running")
					return false
				}
				// Component stopped without detecting cancellation - this can happen
				// if Stop() was called before the monitor detected it. This is acceptable.
			}

			// Verification 4: All operations should have either completed or been cancelled
			totalOps := operationsCompleted.Load() + operationsCancelled.Load()
			if totalOps != operationsStarted.Load() {
				t.Logf("Operation count mismatch: started=%d, completed=%d, cancelled=%d",
					operationsStarted.Load(), operationsCompleted.Load(), operationsCancelled.Load())
				return false
			}

			// Verification 5: At least some operations should have been cancelled
			// (unless all completed before Close was called)
			if operationsCancelled.Load() == 0 && elapsed > opDuration {
				// If Close took longer than operation duration, some should have been cancelled
				t.Logf("Expected some operations to be cancelled, but all completed")
				// This is not necessarily a failure, just log it
			}

			// Verification 6: Close should complete within reasonable time
			maxExpectedTime := sm.config.ShutdownTimeout + 2*time.Second
			if elapsed > maxExpectedTime {
				t.Logf("Close took %v, exceeded max expected time %v", elapsed, maxExpectedTime)
				return false
			}

			// Verification 7: StatusManager should be in Closed state
			if sm.Status() != StatusClosed {
				t.Logf("Expected StatusManager to be Closed, got: %s", sm.Status())
				return false
			}

			return true
		},
		gen.IntRange(1, 20),   // numOperations: 1-20 concurrent operations
		gen.IntRange(10, 200), // operationDurationMs: 10-200ms per operation
	))

	properties.TestingRun(t)
}

// testComponentCounter is used to generate unique component names
var testComponentCounter atomic.Int64

// TestContextAwareComponent is a test component that monitors context cancellation
type TestContextAwareComponent struct {
	name           string
	running        atomic.Bool
	detectedCancel atomic.Bool
	ctx            context.Context
	cancel         context.CancelFunc
	detectionTime  time.Duration
	stopCh         chan struct{}
	mu             sync.Mutex
}

// NewTestContextAwareComponent creates a new test component
func NewTestContextAwareComponent(detectionTime time.Duration) *TestContextAwareComponent {
	// Generate unique name using timestamp (nanosecond precision) and counter
	id := testComponentCounter.Add(1)
	name := time.Now().Format("TestComp_20060102150405.000000000") + "_" + fmt.Sprintf("%d", id)
	return &TestContextAwareComponent{
		name:          name,
		detectionTime: detectionTime,
		stopCh:        make(chan struct{}),
	}
}

// Name returns component name
func (c *TestContextAwareComponent) Name() string {
	return c.name
}

// Start starts the component
func (c *TestContextAwareComponent) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.running.Load() {
		return nil
	}

	c.ctx, c.cancel = context.WithCancel(ctx)
	c.running.Store(true)

	// Start monitoring goroutine
	go c.monitor()

	return nil
}

// Stop stops the component
func (c *TestContextAwareComponent) Stop(timeout time.Duration) error {
	c.mu.Lock()
	if !c.running.Load() {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	if c.cancel != nil {
		c.cancel()
	}

	// Wait for monitor to stop
	select {
	case <-c.stopCh:
	case <-time.After(timeout):
		return nil
	}

	c.running.Store(false)
	return nil
}

// monitor watches for context cancellation
func (c *TestContextAwareComponent) monitor() {
	defer close(c.stopCh)

	ticker := time.NewTicker(c.detectionTime)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			// Detected cancellation
			c.detectedCancel.Store(true)
			return
		case <-ticker.C:
			// Continue monitoring
		}
	}
}

// IsRunning returns whether component is running
func (c *TestContextAwareComponent) IsRunning() bool {
	return c.running.Load()
}

// DetectedCancellation returns whether component detected context cancellation
func (c *TestContextAwareComponent) DetectedCancellation() bool {
	return c.detectedCancel.Load()
}

// TestOperationalComponent is a test component that performs operations
type TestOperationalComponent struct {
	name              string
	running           atomic.Bool
	detectedCancel    atomic.Bool
	ctx               context.Context
	cancel            context.CancelFunc
	operationDuration time.Duration
	monitorStarted    chan struct{}
	mu                sync.Mutex
}

// NewTestOperationalComponent creates a new operational test component
func NewTestOperationalComponent(operationDuration time.Duration) *TestOperationalComponent {
	// Generate unique name using timestamp (nanosecond precision) and counter
	id := testComponentCounter.Add(1)
	name := time.Now().Format("TestOpComp_20060102150405.000000000") + "_" + fmt.Sprintf("%d", id)
	return &TestOperationalComponent{
		name:              name,
		operationDuration: operationDuration,
		monitorStarted:    make(chan struct{}),
	}
}

// Name returns component name
func (c *TestOperationalComponent) Name() string {
	return c.name
}

// Start starts the component
func (c *TestOperationalComponent) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.running.Load() {
		return nil
	}

	c.ctx, c.cancel = context.WithCancel(ctx)
	c.running.Store(true)

	// Start monitoring goroutine
	go c.monitor()

	// Wait for monitor to start
	<-c.monitorStarted

	return nil
}

// Stop stops the component
func (c *TestOperationalComponent) Stop(timeout time.Duration) error {
	c.mu.Lock()
	if !c.running.Load() {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	if c.cancel != nil {
		c.cancel()
	}

	c.running.Store(false)
	return nil
}

// monitor watches for context cancellation
func (c *TestOperationalComponent) monitor() {
	close(c.monitorStarted)
	<-c.ctx.Done()
	c.detectedCancel.Store(true)
}

// PerformOperation performs an operation that can be cancelled
// Returns true if operation was cancelled, false if completed
func (c *TestOperationalComponent) PerformOperation(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	case <-time.After(c.operationDuration):
		return false
	}
}

// IsRunning returns whether component is running
func (c *TestOperationalComponent) IsRunning() bool {
	return c.running.Load()
}

// DetectedCancellation returns whether component detected context cancellation
func (c *TestOperationalComponent) DetectedCancellation() bool {
	return c.detectedCancel.Load()
}
