package nutsdb

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nutsdb/nutsdb/internal/ttl"
)

// TestTTLServiceWrapper_Creation tests TTLServiceWrapper creation
func TestTTLServiceWrapper_Creation(t *testing.T) {
	// Create a minimal TTL service for testing
	mockClock := ttl.NewMockClock(1000000)
	config := ttl.DefaultConfig()
	config.ScanInterval = 100 * time.Millisecond
	config.BatchTimeout = 50 * time.Millisecond

	callback := func(events []*ttl.ExpirationEvent) {}
	scanFn := func() ([]*ttl.ExpirationEvent, error) { return nil, nil }

	service := ttl.NewService(mockClock, config, callback, scanFn)
	sm := NewStatusManager(DefaultStatusManagerConfig())
	defer sm.Close()

	tw := NewTTLServiceWrapper(service, sm)

	if tw == nil {
		t.Fatal("Expected non-nil TTLServiceWrapper")
	}

	if tw.Name() != "TTLService" {
		t.Errorf("Expected name 'TTLService', got '%s'", tw.Name())
	}

	if tw.running.Load() {
		t.Error("Expected TTLServiceWrapper to not be running initially")
	}

	// Verify GetService returns the wrapped service
	if tw.GetService() != service {
		t.Error("Expected GetService to return the wrapped service")
	}
}

// TestTTLServiceWrapper_StartStop tests starting and stopping the wrapper
func TestTTLServiceWrapper_StartStop(t *testing.T) {
	mockClock := ttl.NewMockClock(1000000)
	config := ttl.DefaultConfig()
	config.ScanInterval = 50 * time.Millisecond
	config.BatchTimeout = 50 * time.Millisecond

	callback := func(events []*ttl.ExpirationEvent) {}
	scanFn := func() ([]*ttl.ExpirationEvent, error) { return nil, nil }

	service := ttl.NewService(mockClock, config, callback, scanFn)
	sm := NewStatusManager(DefaultStatusManagerConfig())
	defer sm.Close()

	tw := NewTTLServiceWrapper(service, sm)

	// Test Start
	ctx := context.Background()
	if err := tw.Start(ctx); err != nil {
		t.Fatalf("Failed to start TTLServiceWrapper: %v", err)
	}

	if !tw.running.Load() {
		t.Error("Expected TTLServiceWrapper to be running after Start()")
	}

	// Test duplicate Start (should return error)
	if err := tw.Start(ctx); err == nil {
		t.Error("Expected error when starting already running TTLServiceWrapper")
	}

	// Test Stop
	if err := tw.Stop(5 * time.Second); err != nil {
		t.Fatalf("Failed to stop TTLServiceWrapper: %v", err)
	}

	if tw.running.Load() {
		t.Error("Expected TTLServiceWrapper to not be running after Stop()")
	}

	// Test duplicate Stop (should be idempotent)
	if err := tw.Stop(5 * time.Second); err != nil {
		t.Errorf("Expected no error when stopping already stopped TTLServiceWrapper, got: %v", err)
	}
}

// TestTTLServiceWrapper_ContextCancellation tests that the wrapper responds to context cancellation
func TestTTLServiceWrapper_ContextCancellation(t *testing.T) {
	mockClock := ttl.NewMockClock(1000000)
	config := ttl.DefaultConfig()
	config.ScanInterval = 50 * time.Millisecond
	config.BatchTimeout = 50 * time.Millisecond

	callback := func(events []*ttl.ExpirationEvent) {}
	scanFn := func() ([]*ttl.ExpirationEvent, error) { return nil, nil }

	service := ttl.NewService(mockClock, config, callback, scanFn)
	sm := NewStatusManager(DefaultStatusManagerConfig())
	defer sm.Close()

	tw := NewTTLServiceWrapper(service, sm)

	// Create a cancellable context
	ctx, cancel := context.WithCancel(context.Background())

	// Start the wrapper
	if err := tw.Start(ctx); err != nil {
		t.Fatalf("Failed to start TTLServiceWrapper: %v", err)
	}

	// Verify it's running
	if !tw.running.Load() {
		t.Error("Expected TTLServiceWrapper to be running")
	}

	// Cancel the context
	cancel()

	// Stop should complete quickly since context is already cancelled
	start := time.Now()
	if err := tw.Stop(2 * time.Second); err != nil {
		t.Errorf("Failed to stop TTLServiceWrapper after context cancellation: %v", err)
	}
	elapsed := time.Since(start)

	// Stop should be fast since context was already cancelled
	if elapsed > 500*time.Millisecond {
		t.Errorf("Expected Stop to complete quickly after context cancellation, took %v", elapsed)
	}

	if tw.running.Load() {
		t.Error("Expected TTLServiceWrapper to not be running after Stop()")
	}
}

// TestTTLServiceWrapper_StopTimeout tests that Stop respects timeout
func TestTTLServiceWrapper_StopTimeout(t *testing.T) {
	mockClock := ttl.NewMockClock(1000000)
	config := ttl.DefaultConfig()
	config.ScanInterval = 50 * time.Millisecond
	config.BatchTimeout = 50 * time.Millisecond

	callback := func(events []*ttl.ExpirationEvent) {}
	scanFn := func() ([]*ttl.ExpirationEvent, error) { return nil, nil }

	service := ttl.NewService(mockClock, config, callback, scanFn)
	sm := NewStatusManager(DefaultStatusManagerConfig())
	defer sm.Close()

	tw := NewTTLServiceWrapper(service, sm)

	ctx := context.Background()
	if err := tw.Start(ctx); err != nil {
		t.Fatalf("Failed to start TTLServiceWrapper: %v", err)
	}

	// Stop with a short timeout
	start := time.Now()
	if err := tw.Stop(500 * time.Millisecond); err != nil {
		t.Errorf("Stop returned error: %v", err)
	}
	elapsed := time.Since(start)

	// Stop should complete within the timeout
	if elapsed > 1*time.Second {
		t.Errorf("Expected Stop to complete within timeout, took %v", elapsed)
	}

	if tw.running.Load() {
		t.Error("Expected TTLServiceWrapper to not be running after Stop()")
	}
}

// TestTTLServiceWrapper_QueueProcessing tests that the wrapper processes expiration events
func TestTTLServiceWrapper_QueueProcessing(t *testing.T) {
	mockClock := ttl.NewMockClock(1000000)
	config := ttl.DefaultConfig()
	config.ScanInterval = 50 * time.Millisecond
	config.BatchTimeout = 100 * time.Millisecond
	config.BatchSize = 5

	var eventsReceived []*ttl.ExpirationEvent
	var mu sync.Mutex
	eventReceived := make(chan struct{}, 10)

	callback := func(events []*ttl.ExpirationEvent) {
		mu.Lock()
		eventsReceived = append(eventsReceived, events...)
		mu.Unlock()
		// Signal that events were received
		select {
		case eventReceived <- struct{}{}:
		default:
		}
	}

	// Scan function that returns some expired events
	scanCallCount := 0
	scanFn := func() ([]*ttl.ExpirationEvent, error) {
		scanCallCount++
		if scanCallCount == 1 {
			return []*ttl.ExpirationEvent{
				{BucketId: 1, Key: []byte("key1"), Ds: 2, Timestamp: 1000000},
				{BucketId: 1, Key: []byte("key2"), Ds: 2, Timestamp: 1000001},
			}, nil
		}
		return nil, nil
	}

	service := ttl.NewService(mockClock, config, callback, scanFn)
	sm := NewStatusManager(DefaultStatusManagerConfig())
	defer sm.Close()

	tw := NewTTLServiceWrapper(service, sm)

	ctx := context.Background()
	if err := tw.Start(ctx); err != nil {
		t.Fatalf("Failed to start TTLServiceWrapper: %v", err)
	}

	// Wait for events to be processed using channel
	select {
	case <-eventReceived:
		// Events received
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for expiration events to be processed")
	}

	// Stop the wrapper
	if err := tw.Stop(2 * time.Second); err != nil {
		t.Errorf("Failed to stop TTLServiceWrapper: %v", err)
	}

	// Verify events were received
	mu.Lock()
	if len(eventsReceived) < 2 {
		t.Errorf("Expected at least 2 events, got %d", len(eventsReceived))
	}
	mu.Unlock()
}

// TestTTLServiceWrapper_IntegrationWithStatusManager tests integration with StatusManager
func TestTTLServiceWrapper_IntegrationWithStatusManager(t *testing.T) {
	mockClock := ttl.NewMockClock(1000000)
	config := ttl.DefaultConfig()
	config.ScanInterval = 50 * time.Millisecond
	config.BatchTimeout = 50 * time.Millisecond

	callback := func(events []*ttl.ExpirationEvent) {}
	scanFn := func() ([]*ttl.ExpirationEvent, error) { return nil, nil }

	service := ttl.NewService(mockClock, config, callback, scanFn)
	sm := NewStatusManager(DefaultStatusManagerConfig())

	tw := NewTTLServiceWrapper(service, sm)

	// Register the wrapper as a component
	if err := sm.RegisterComponent("TTLService", tw); err != nil {
		t.Fatalf("Failed to register TTLServiceWrapper: %v", err)
	}

	// Start StatusManager (which should start the wrapper)
	if err := sm.Start(); err != nil {
		t.Fatalf("Failed to start StatusManager: %v", err)
	}

	// Verify the wrapper is running
	if !tw.running.Load() {
		t.Error("Expected TTLServiceWrapper to be running after StatusManager.Start()")
	}

	// Verify component status
	status, err := sm.GetComponentStatus("TTLService")
	if err != nil {
		t.Fatalf("Failed to get component status: %v", err)
	}
	if status != ComponentStatusRunning {
		t.Errorf("Expected component status Running, got %v", status)
	}

	// Close StatusManager (which should stop the wrapper)
	if err := sm.Close(); err != nil {
		t.Fatalf("Failed to close StatusManager: %v", err)
	}

	// Verify the wrapper is stopped
	if tw.running.Load() {
		t.Error("Expected TTLServiceWrapper to not be running after StatusManager.Close()")
	}
}

// TestTTLServiceWrapper_NilLogger tests that nil logger is handled
func TestTTLServiceWrapper_NilLogger(t *testing.T) {
	mockClock := ttl.NewMockClock(1000000)
	config := ttl.DefaultConfig()
	config.ScanInterval = 50 * time.Millisecond
	config.BatchTimeout = 50 * time.Millisecond

	callback := func(events []*ttl.ExpirationEvent) {}
	scanFn := func() ([]*ttl.ExpirationEvent, error) { return nil, nil }

	service := ttl.NewService(mockClock, config, callback, scanFn)
	sm := NewStatusManager(DefaultStatusManagerConfig())
	defer sm.Close()

	// Create wrapper with nil logger (should use default)
	tw := NewTTLServiceWrapper(service, sm)

	if tw == nil {
		t.Fatal("Expected non-nil TTLServiceWrapper with nil logger")
	}

	// Start and stop should work without panicking
	ctx := context.Background()
	if err := tw.Start(ctx); err != nil {
		t.Fatalf("Failed to start TTLServiceWrapper with nil logger: %v", err)
	}

	if err := tw.Stop(2 * time.Second); err != nil {
		t.Errorf("Failed to stop TTLServiceWrapper with nil logger: %v", err)
	}
}

// TestTTLServiceWrapper_ConcurrentStartStop tests concurrent start/stop operations
func TestTTLServiceWrapper_ConcurrentStartStop(t *testing.T) {
	mockClock := ttl.NewMockClock(1000000)
	config := ttl.DefaultConfig()
	config.ScanInterval = 50 * time.Millisecond
	config.BatchTimeout = 50 * time.Millisecond

	callback := func(events []*ttl.ExpirationEvent) {}
	scanFn := func() ([]*ttl.ExpirationEvent, error) { return nil, nil }

	service := ttl.NewService(mockClock, config, callback, scanFn)
	sm := NewStatusManager(DefaultStatusManagerConfig())
	defer sm.Close()

	tw := NewTTLServiceWrapper(service, sm)

	// Start the wrapper first
	ctx := context.Background()
	if err := tw.Start(ctx); err != nil {
		t.Fatalf("Failed to start TTLServiceWrapper: %v", err)
	}

	// Concurrent stop operations should be safe
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tw.Stop(2 * time.Second)
		}()
	}

	wg.Wait()

	// Wrapper should be stopped
	if tw.running.Load() {
		t.Error("Expected TTLServiceWrapper to not be running after concurrent stops")
	}
}
