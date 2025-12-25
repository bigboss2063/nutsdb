package nutsdb

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTestWatchManagerWrapper creates a WatchManagerWrapper for testing
func createTestWatchManagerWrapper() (*WatchManagerWrapper, *StatusManager) {
	sm := NewStatusManager(DefaultStatusManagerConfig())
	wm := NewWatchManager()
	wrapper := NewWatchManagerWrapper(wm, sm)
	return wrapper, sm
}

func TestWatchManagerWrapper_Name(t *testing.T) {
	wrapper, _ := createTestWatchManagerWrapper()
	assert.Equal(t, "WatchManager", wrapper.Name())
}

func TestWatchManagerWrapper_StartAndStop(t *testing.T) {
	t.Run("start and stop successfully", func(t *testing.T) {
		wrapper, sm := createTestWatchManagerWrapper()

		// Start the wrapper
		err := wrapper.Start(sm.Context())
		require.NoError(t, err)
		assert.True(t, wrapper.running.Load(), "wrapper should be running after Start")

		// Stop the wrapper
		err = wrapper.Stop(5 * time.Second)
		require.NoError(t, err)
		assert.False(t, wrapper.running.Load(), "wrapper should not be running after Stop")
	})

	t.Run("start already running returns error", func(t *testing.T) {
		wrapper, sm := createTestWatchManagerWrapper()

		// Start the wrapper
		err := wrapper.Start(sm.Context())
		require.NoError(t, err)

		// Try to start again
		err = wrapper.Start(sm.Context())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already running")

		// Cleanup
		wrapper.Stop(5 * time.Second)
	})

	t.Run("stop already stopped is idempotent", func(t *testing.T) {
		wrapper, sm := createTestWatchManagerWrapper()

		// Start and stop
		err := wrapper.Start(sm.Context())
		require.NoError(t, err)

		err = wrapper.Stop(5 * time.Second)
		require.NoError(t, err)

		// Stop again should not error
		err = wrapper.Stop(5 * time.Second)
		assert.NoError(t, err)
	})

	t.Run("stop without start is safe", func(t *testing.T) {
		wrapper, _ := createTestWatchManagerWrapper()

		// Stop without starting should not error
		err := wrapper.Stop(5 * time.Second)
		assert.NoError(t, err)
	})
}

func TestWatchManagerWrapper_SubscriberNotification(t *testing.T) {
	t.Run("subscribers receive messages after start", func(t *testing.T) {
		wrapper, sm := createTestWatchManagerWrapper()

		// Start the wrapper
		err := wrapper.Start(sm.Context())
		require.NoError(t, err)
		defer wrapper.Stop(5 * time.Second)

		// Give distributor time to start
		time.Sleep(100 * time.Millisecond)

		// Subscribe to a bucket/key
		bucket := "test_bucket"
		key := "test_key"
		subscriber, err := wrapper.GetManager().subscribe(bucket, key)
		require.NoError(t, err)

		// Send a message
		value := []byte("test_value")
		message := NewMessage(bucket, key, value, DataSetFlag, uint64(time.Now().Unix()))
		err = wrapper.GetManager().sendMessage(message)
		require.NoError(t, err)

		// Verify subscriber receives the message
		select {
		case msg, ok := <-subscriber.receiveChan:
			require.True(t, ok, "channel should be open")
			assert.Equal(t, bucket, msg.BucketName)
			assert.Equal(t, key, msg.Key)
			assert.Equal(t, value, msg.Value)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for message")
		}
	})

	t.Run("multiple subscribers receive messages", func(t *testing.T) {
		wrapper, sm := createTestWatchManagerWrapper()

		// Start the wrapper
		err := wrapper.Start(sm.Context())
		require.NoError(t, err)
		defer wrapper.Stop(5 * time.Second)

		// Give distributor time to start
		time.Sleep(100 * time.Millisecond)

		bucket := "test_bucket"
		key := "test_key"
		numSubscribers := 3

		// Create multiple subscribers
		subscribers := make([]*subscriber, numSubscribers)
		for i := 0; i < numSubscribers; i++ {
			sub, err := wrapper.GetManager().subscribe(bucket, key)
			require.NoError(t, err)
			subscribers[i] = sub
		}

		// Send a message
		value := []byte("broadcast_value")
		message := NewMessage(bucket, key, value, DataSetFlag, uint64(time.Now().Unix()))
		err = wrapper.GetManager().sendMessage(message)
		require.NoError(t, err)

		// Verify all subscribers receive the message
		var wg sync.WaitGroup
		for i, sub := range subscribers {
			wg.Add(1)
			go func(idx int, s *subscriber) {
				defer wg.Done()
				select {
				case msg, ok := <-s.receiveChan:
					assert.True(t, ok, "channel %d should be open", idx)
					assert.Equal(t, bucket, msg.BucketName)
					assert.Equal(t, key, msg.Key)
					assert.Equal(t, value, msg.Value)
				case <-time.After(2 * time.Second):
					t.Errorf("timeout waiting for message on subscriber %d", idx)
				}
			}(i, sub)
		}
		wg.Wait()
	})
}

func TestWatchManagerWrapper_ChannelClosing(t *testing.T) {
	t.Run("subscriber channels closed on stop", func(t *testing.T) {
		wrapper, sm := createTestWatchManagerWrapper()

		// Start the wrapper
		err := wrapper.Start(sm.Context())
		require.NoError(t, err)

		// Give distributor time to start
		time.Sleep(100 * time.Millisecond)

		// Subscribe to a bucket/key
		bucket := "test_bucket"
		key := "test_key"
		subscriber, err := wrapper.GetManager().subscribe(bucket, key)
		require.NoError(t, err)

		// Stop the wrapper
		err = wrapper.Stop(5 * time.Second)
		require.NoError(t, err)

		// Wait for cleanup
		time.Sleep(200 * time.Millisecond)

		// Verify channel is closed
		select {
		case _, ok := <-subscriber.receiveChan:
			assert.False(t, ok, "channel should be closed after stop")
		case <-time.After(1 * time.Second):
			t.Fatal("timeout waiting for channel to close")
		}
	})

	t.Run("multiple subscriber channels closed on stop", func(t *testing.T) {
		wrapper, sm := createTestWatchManagerWrapper()

		// Start the wrapper
		err := wrapper.Start(sm.Context())
		require.NoError(t, err)

		// Give distributor time to start
		time.Sleep(100 * time.Millisecond)

		// Create multiple subscribers on different buckets/keys
		type subInfo struct {
			bucket string
			key    string
			sub    *subscriber
		}
		subs := []subInfo{
			{bucket: "bucket1", key: "key1"},
			{bucket: "bucket1", key: "key2"},
			{bucket: "bucket2", key: "key1"},
		}

		for i := range subs {
			sub, err := wrapper.GetManager().subscribe(subs[i].bucket, subs[i].key)
			require.NoError(t, err)
			subs[i].sub = sub
		}

		// Stop the wrapper
		err = wrapper.Stop(5 * time.Second)
		require.NoError(t, err)

		// Wait for cleanup
		time.Sleep(200 * time.Millisecond)

		// Verify all channels are closed
		for _, s := range subs {
			select {
			case _, ok := <-s.sub.receiveChan:
				assert.False(t, ok, "channel for %s/%s should be closed", s.bucket, s.key)
			case <-time.After(1 * time.Second):
				t.Fatalf("timeout waiting for channel %s/%s to close", s.bucket, s.key)
			}
		}
	})

	t.Run("send message after stop returns error", func(t *testing.T) {
		wrapper, sm := createTestWatchManagerWrapper()

		// Start the wrapper
		err := wrapper.Start(sm.Context())
		require.NoError(t, err)

		// Give distributor time to start
		time.Sleep(100 * time.Millisecond)

		// Stop the wrapper
		err = wrapper.Stop(5 * time.Second)
		require.NoError(t, err)

		// Try to send a message after stop
		message := NewMessage("bucket", "key", []byte("value"), DataSetFlag, uint64(time.Now().Unix()))
		err = wrapper.GetManager().sendMessage(message)
		assert.Error(t, err)
		assert.Equal(t, ErrWatchManagerClosed, err)
	})
}

func TestWatchManagerWrapper_GetManager(t *testing.T) {
	wrapper, _ := createTestWatchManagerWrapper()
	manager := wrapper.GetManager()
	assert.NotNil(t, manager, "GetManager should return the underlying watch manager")
}

func TestWatchManagerWrapper_ConcurrentOperations(t *testing.T) {
	t.Run("concurrent subscribe and stop", func(t *testing.T) {
		wrapper, sm := createTestWatchManagerWrapper()

		// Start the wrapper
		err := wrapper.Start(sm.Context())
		require.NoError(t, err)

		// Give distributor time to start
		time.Sleep(100 * time.Millisecond)

		var wg sync.WaitGroup
		stopCh := make(chan struct{})

		// Start goroutines that continuously subscribe
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				for {
					select {
					case <-stopCh:
						return
					default:
						_, _ = wrapper.GetManager().subscribe("bucket", "key")
						time.Sleep(10 * time.Millisecond)
					}
				}
			}(i)
		}

		// Let subscriptions happen for a bit
		time.Sleep(100 * time.Millisecond)

		// Stop the wrapper while subscriptions are happening
		close(stopCh)
		err = wrapper.Stop(5 * time.Second)
		require.NoError(t, err)

		wg.Wait()
	})
}
