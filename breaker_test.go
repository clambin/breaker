package breaker

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCircuitBreaker_Do(t *testing.T) {
	cfg := Configuration{
		ErrorThreshold:   5,
		OpenDuration:     500 * time.Millisecond,
		SuccessThreshold: 10,
	}
	newCB := func(state State) *CircuitBreaker {
		cb := New(cfg)
		switch state {
		case StateClosed:
			cb.close()
		case StateOpen:
			cb.open()
		case StateHalfOpen:
			cb.halfOpen()
		}
		return cb
	}

	t.Run("closed cb remains closed on success", func(t *testing.T) {
		t.Parallel()
		cb := newCB(StateClosed)
		assert.NoError(t, cb.Do(func() error { return nil }))
		assert.Equal(t, StateClosed, cb.GetState())
	})

	t.Run("closed cb closes after ErrorThreshold errors", func(t *testing.T) {
		t.Parallel()
		cb := newCB(StateClosed)
		var calls int
		for cb.GetState() == StateClosed {
			assert.Error(t, cb.Do(func() error { return errors.New("error") }))
			calls++
		}
		assert.Equal(t, cfg.ErrorThreshold, calls)
	})

	t.Run("closed cb only closes after ErrorThreshold consecutive errors", func(t *testing.T) {
		t.Parallel()
		cb := newCB(StateClosed)
		for range cfg.ErrorThreshold {
			assert.Error(t, cb.Do(func() error { return errors.New("error") }))
			assert.NoError(t, cb.Do(func() error { return nil }))
		}
		assert.Equal(t, StateClosed, cb.GetState())
	})

	t.Run("open cb doesn't perform the call", func(t *testing.T) {
		t.Parallel()
		cb := newCB(StateOpen)
		cb.configuration.OpenDuration = time.Hour
		var calls int
		for range 10 {
			err := cb.Do(func() error {
				calls++
				return nil
			})
			assert.ErrorIs(t, err, ErrCircuitOpen)
		}
		assert.Zero(t, calls)
	})

	t.Run("open cb goes half-open after OpenDuration", func(t *testing.T) {
		t.Parallel()
		cb := newCB(StateOpen)
		start := time.Now()
		for cb.GetState() == StateOpen {
			time.Sleep(100 * time.Millisecond)
		}
		assert.GreaterOrEqual(t, time.Since(start), cfg.OpenDuration)
	})

	t.Run("half-open cb opens on error", func(t *testing.T) {
		t.Parallel()
		cb := newCB(StateHalfOpen)
		assert.Error(t, cb.Do(func() error { return errors.New("error") }))
		assert.Equal(t, StateOpen.String(), cb.GetState().String())
	})

	t.Run("half-open cb closes after SuccessThreshold successes", func(t *testing.T) {
		t.Parallel()
		cb := newCB(StateHalfOpen)
		var calls int
		for cb.GetState() == StateHalfOpen {
			assert.NoError(t, cb.Do(func() error { return nil }))
			calls++
		}
		assert.Equal(t, StateClosed, cb.GetState())
		assert.Equal(t, cfg.SuccessThreshold, calls)
	})
}

func TestCircuitBreaker_Do_Throttled(t *testing.T) {
	cfg := Configuration{
		ErrorThreshold:   1,
		OpenDuration:     500 * time.Millisecond,
		SuccessThreshold: 10,
		HalfOpenThrottle: 2,
	}
	cb := New(cfg)
	_ = cb.Do(func() error { return errors.New("error") })
	assert.Eventually(t, func() bool { return cb.GetState() == StateHalfOpen }, time.Second, 100*time.Millisecond)

	var wg sync.WaitGroup
	var maxCalls atomic.Int32
	var inflightCalls atomic.Int32

	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = cb.Do(func() error {
				inflight := inflightCalls.Add(1)
				if highest := maxCalls.Load(); inflight > highest {
					maxCalls.Store(inflight)
				}
				time.Sleep(100 * time.Millisecond)
				inflightCalls.Add(-1)
				return nil
			})
		}()
	}

	wg.Wait()
	assert.Equal(t, int32(cfg.HalfOpenThrottle), maxCalls.Load())
}

func TestCircuitBreaker_Do_Custom(t *testing.T) {
	cfg := Configuration{
		ErrorThreshold:   5,
		OpenDuration:     500 * time.Millisecond,
		SuccessThreshold: 10,
		ShouldOpen:       func(_ Counters) bool { return true },
		ShouldClose:      func(_ Counters) bool { return true },
	}
	cb := New(cfg)
	assert.Equal(t, StateClosed.String(), cb.GetState().String())
	_ = cb.Do(func() error { return errors.New("error") })
	// custom ShouldOpen ignores ErrorThreshold: circuit opens on first error
	assert.Equal(t, StateOpen.String(), cb.GetState().String())
	assert.Eventually(t, func() bool { return cb.GetState() == StateHalfOpen }, time.Second, 50*time.Millisecond)
	_ = cb.Do(func() error { return nil })
	// custom ShouldClose ignores SuccessThreshold: circuit closes on first success
	assert.Equal(t, StateClosed.String(), cb.GetState().String())
}

func TestCircuitBreaker_GetCounters(t *testing.T) {
	cb := New(Configuration{
		ErrorThreshold:   1,
		OpenDuration:     500 * time.Millisecond,
		SuccessThreshold: 1,
		Logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	_ = cb.Do(func() error { return nil })
	assert.Equal(t, Counters{Calls: 1, Successes: 1, ConsecutiveSuccesses: 1}, cb.GetCounters())

	_ = cb.Do(func() error { return errors.New("error") })
	assert.Equal(t, Counters{}, cb.GetCounters())

	assert.Eventually(t, func() bool { return cb.GetState() == StateHalfOpen }, time.Second, 100*time.Millisecond)
	assert.Equal(t, Counters{}, cb.GetCounters())

	_ = cb.Do(func() error { return nil })
	_ = cb.Do(func() error { return nil })
	assert.Equal(t, Counters{Calls: 1, Successes: 1, ConsecutiveSuccesses: 1}, cb.GetCounters())
}

/*
	func TestCircuitBreaker_Logger(t *testing.T) {
		var output bytes.Buffer
		l := slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{
			Level: slog.LevelDebug,
			ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
				if a.Key == slog.TimeKey {
					return slog.Attr{}
				}
				return a
			},
		}))
		cb := New(Configuration{ErrorThreshold: 1, Logger: l})
		_ = cb.Do(func() error {
			return errors.New("error")
		})
		assert.Equal(t, `level=DEBUG msg="state change detected" state=open

`, output.String())
}
*/
func BenchmarkCircuitBreaker_Do(b *testing.B) {
	cb := New(Configuration{
		ErrorThreshold:   5,
		OpenDuration:     time.Millisecond,
		SuccessThreshold: 10,
	})

	b.Run("success", func(b *testing.B) {
		for range b.N {
			_ = cb.Do(func() error {
				return nil
			})
		}
	})
	b.Run("error", func(b *testing.B) {
		for range b.N {
			_ = cb.Do(func() error {
				return errors.New("error")
			})
		}
	})
}

func ExampleCircuitBreaker_Do() {
	cfg := Configuration{ErrorThreshold: 2}
	cb := New(cfg)
	for i := range cfg.ErrorThreshold + 1 {
		err := cb.Do(func() error {
			return errors.New("error")
		})
		fmt.Printf("%d: err: %v\n", i, err)
	}
	//Output:
	//0: err: error
	//1: err: error
	//2: err: circuit is open
}

func TestState_String(t *testing.T) {
	tests := []struct {
		name string
		s    State
		want string
	}{
		{
			name: "closed",
			s:    StateClosed,
			want: "closed",
		},
		{
			name: "open",
			s:    StateOpen,
			want: "open",
		},
		{
			name: "half-open",
			s:    StateHalfOpen,
			want: "half-open",
		},
		{
			name: "invalid",
			s:    -1,
			want: "unknown",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.s.String())
		})
	}
}
