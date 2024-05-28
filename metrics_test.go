package breaker

import (
	"errors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

func TestCircuitBreaker_Metrics(t *testing.T) {
	metrics := NewMetrics("foo", "", "cb", nil)
	cfg := Configuration{
		ErrorThreshold:   5,
		OpenDuration:     500 * time.Millisecond,
		SuccessThreshold: 5,
		Metrics:          metrics,
	}
	cb := New(cfg)

	for range cfg.ErrorThreshold - 1 {
		_ = cb.Do(func() error { return errors.New("error") })
	}
	assert.NoError(t, testutil.CollectAndCompare(metrics, strings.NewReader(`
# HELP foo_circuit_breaker_consecutive_errors consecutive errors
# TYPE foo_circuit_breaker_consecutive_errors gauge
foo_circuit_breaker_consecutive_errors{circuit_breaker="cb"} 4

# HELP foo_circuit_breaker_consecutive_successes consecutive successes
# TYPE foo_circuit_breaker_consecutive_successes gauge
foo_circuit_breaker_consecutive_successes{circuit_breaker="cb"} 0

# HELP foo_circuit_breaker_state state of the circuit breaker (0: closed, 1:open, 2:half-open)
# TYPE foo_circuit_breaker_state gauge
foo_circuit_breaker_state{circuit_breaker="cb"} 0
`)))

	// open the circuit breaker
	_ = cb.Do(func() error { return errors.New("error") })
	assert.NoError(t, testutil.CollectAndCompare(metrics, strings.NewReader(`
# HELP foo_circuit_breaker_consecutive_errors consecutive errors
# TYPE foo_circuit_breaker_consecutive_errors gauge
foo_circuit_breaker_consecutive_errors{circuit_breaker="cb"} 0

# HELP foo_circuit_breaker_consecutive_successes consecutive successes
# TYPE foo_circuit_breaker_consecutive_successes gauge
foo_circuit_breaker_consecutive_successes{circuit_breaker="cb"} 0

# HELP foo_circuit_breaker_state state of the circuit breaker (0: closed, 1:open, 2:half-open)
# TYPE foo_circuit_breaker_state gauge
foo_circuit_breaker_state{circuit_breaker="cb"} 1
`)))

	// wait for the circuit breaker to half-open
	assert.Eventually(t, func() bool { return cb.GetState() == StateHalfOpen }, time.Second, 100*time.Millisecond)
	for range cfg.SuccessThreshold - 1 {
		_ = cb.Do(func() error { return nil })
	}
	assert.NoError(t, testutil.CollectAndCompare(metrics, strings.NewReader(`
# HELP foo_circuit_breaker_consecutive_errors consecutive errors
# TYPE foo_circuit_breaker_consecutive_errors gauge
foo_circuit_breaker_consecutive_errors{circuit_breaker="cb"} 0

# HELP foo_circuit_breaker_consecutive_successes consecutive successes
# TYPE foo_circuit_breaker_consecutive_successes gauge
foo_circuit_breaker_consecutive_successes{circuit_breaker="cb"} 4

# HELP foo_circuit_breaker_state state of the circuit breaker (0: closed, 1:open, 2:half-open)
# TYPE foo_circuit_breaker_state gauge
foo_circuit_breaker_state{circuit_breaker="cb"} 2
`)))

	// close the circuit breaker
	_ = cb.Do(func() error { return nil })
	assert.NoError(t, testutil.CollectAndCompare(metrics, strings.NewReader(`
# HELP foo_circuit_breaker_consecutive_errors consecutive errors
# TYPE foo_circuit_breaker_consecutive_errors gauge
foo_circuit_breaker_consecutive_errors{circuit_breaker="cb"} 0

# HELP foo_circuit_breaker_consecutive_successes consecutive successes
# TYPE foo_circuit_breaker_consecutive_successes gauge
foo_circuit_breaker_consecutive_successes{circuit_breaker="cb"} 0

# HELP foo_circuit_breaker_state state of the circuit breaker (0: closed, 1:open, 2:half-open)
# TYPE foo_circuit_breaker_state gauge
foo_circuit_breaker_state{circuit_breaker="cb"} 0
`)))
}
