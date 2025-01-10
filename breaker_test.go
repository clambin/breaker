package breaker

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
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
		_ = cb.Do(func() error { return nil })
		if got := cb.GetState(); got != StateClosed {
			t.Errorf("state should be closed; got %v", got)
		}
	})

	t.Run("closed cb closes after ErrorThreshold errors", func(t *testing.T) {
		t.Parallel()
		cb := newCB(StateClosed)
		var calls int
		for cb.GetState() == StateClosed {
			_ = cb.Do(func() error { return errors.New("error") })
			calls++
		}
		if calls != cfg.ErrorThreshold {
			t.Errorf("calls should be %v; got %v", cfg.ErrorThreshold, calls)
		}
	})

	t.Run("closed cb only closes after ErrorThreshold consecutive errors", func(t *testing.T) {
		t.Parallel()
		cb := newCB(StateClosed)
		for range cfg.ErrorThreshold {
			_ = cb.Do(func() error { return errors.New("error") })
			_ = cb.Do(func() error { return nil })
		}
		if got := cb.GetState(); got != StateClosed {
			t.Errorf("state should be closed; got %v", got)
		}
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
			if !errors.Is(err, ErrCircuitOpen) {
				t.Errorf("error should be ErrCircuitOpen; got %v", err)
			}
		}
		if calls != 0 {
			t.Errorf("calls should be 0; got %v", calls)
		}
	})

	t.Run("open cb goes half-open after OpenDuration", func(t *testing.T) {
		t.Parallel()
		cb := newCB(StateOpen)
		start := time.Now()
		for cb.GetState() == StateOpen {
			time.Sleep(100 * time.Millisecond)
		}
		if got := time.Since(start); got < cfg.OpenDuration {
			t.Errorf("circuit breaker closed too soon; want: %v, got %v", cfg.OpenDuration, got)
		}
	})

	t.Run("half-open cb opens on error", func(t *testing.T) {
		t.Parallel()
		cb := newCB(StateHalfOpen)
		_ = cb.Do(func() error { return errors.New("error") })
		if got := cb.GetState(); got != StateOpen {
			t.Errorf("state should be open; got %v", got)
		}
	})

	t.Run("half-open cb closes after SuccessThreshold successes", func(t *testing.T) {
		t.Parallel()
		cb := newCB(StateHalfOpen)
		var calls int
		for cb.GetState() == StateHalfOpen {
			_ = cb.Do(func() error { return nil })
			calls++
		}
		if got := cb.GetState(); got != StateClosed {
			t.Errorf("state should be closed; got %v", got)
		}
		if calls != cfg.SuccessThreshold {
			t.Errorf("calls should be %v; got %v", cfg.SuccessThreshold, calls)
		}
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
	if err := waitFor(func() bool { return cb.GetState() == StateHalfOpen }, time.Second, 100*time.Millisecond); err != nil {
		t.Fatalf("wait for half-open state failed: %v", err)

	}

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
	if calls := int(maxCalls.Load()); calls != cfg.HalfOpenThrottle {
		t.Errorf("maxCalls should be %v; got %v", cfg.HalfOpenThrottle, calls)
	}
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
	if got := cb.GetState(); got != StateClosed {
		t.Errorf("state should be closed; got %v", got)
	}

	// custom ShouldOpen ignores ErrorThreshold: circuit opens on first error
	_ = cb.Do(func() error { return errors.New("error") })
	if got := cb.GetState(); got != StateOpen {
		t.Errorf("state should be open; got %v", got)
	}

	// circuit should half-open after OpenDuration interval
	if err := waitFor(func() bool { return cb.GetState() == StateHalfOpen }, time.Second, 100*time.Millisecond); err != nil {
		t.Fatalf("wait for half-open state failed: %v", err)
	}

	// custom ShouldClose ignores SuccessThreshold: circuit closes on first success
	_ = cb.Do(func() error { return nil })
	if got := cb.GetState(); got != StateClosed {
		t.Errorf("state should be closed; got %v", got)
	}
}

func TestCircuitBreaker_GetCounters(t *testing.T) {
	cb := New(Configuration{
		ErrorThreshold:   1,
		OpenDuration:     500 * time.Millisecond,
		SuccessThreshold: 1,
		Logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	_ = cb.Do(func() error { return nil })
	want := Counters{Calls: 1, Successes: 1, ConsecutiveSuccesses: 1}
	if got := cb.GetCounters(); !reflect.DeepEqual(got, want) {
		t.Errorf("GetCounters(): got %v; want %v", got, want)
	}

	_ = cb.Do(func() error { return errors.New("error") })
	want = Counters{}
	if got := cb.GetCounters(); !reflect.DeepEqual(got, want) {
		t.Errorf("GetCounters(): got %v; want %v", got, want)
	}

	if err := waitFor(func() bool { return cb.GetState() == StateHalfOpen }, time.Second, 100*time.Millisecond); err != nil {
		t.Fatalf("wait for half-open state failed: %v", err)
	}
	want = Counters{}
	if got := cb.GetCounters(); !reflect.DeepEqual(got, want) {
		t.Errorf("GetCounters(): got %v; want %v", got, want)
	}

	_ = cb.Do(func() error { return nil })
	_ = cb.Do(func() error { return nil })
	want = Counters{Calls: 1, Successes: 1, ConsecutiveSuccesses: 1}
	if got := cb.GetCounters(); !reflect.DeepEqual(got, want) {
		t.Errorf("GetCounters(): got %v; want %v", got, want)
	}
}

func BenchmarkCircuitBreaker_Do(b *testing.B) {
	cb := New(Configuration{
		ErrorThreshold:   5,
		OpenDuration:     time.Millisecond,
		SuccessThreshold: 10,
	})

	b.Run("success", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_ = cb.Do(func() error {
				return nil
			})
		}
	})
	b.Run("error", func(b *testing.B) {
		b.ReportAllocs()
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
			if got := tt.s.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func waitFor(condition func() bool, timeout, interval time.Duration) error {
	ch := make(chan bool, 1)

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for tickCh := ticker.C; ; {
		select {
		case <-timer.C:
			return errors.New("timeout")
		case <-tickCh:
			// don't run multiple tests in parallel
			tickCh = nil
			go func() { ch <- condition() }()
		case v := <-ch:
			if v {
				return nil
			}
			// start testing again
			tickCh = ticker.C
		}
	}
}
