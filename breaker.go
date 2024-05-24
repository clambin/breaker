/*
Package breaker implements the circuit breaker design pattern.

A circuit breaker stops requests if a service is not working. It has three states:

  - Closed: all requests go through. After a configurable number of errors, the circuit breaker opens.
  - Open: the circuit breaker stops all requests from reaching the service.
  - Half-Open: after a configurable duration, the circuit moves to 'half-open' state. It allows requests to go through, but any error open the circuit again. After a configurable number of successful calls, the circuit breaker closes fully.
*/
package breaker

import (
	"errors"
	"github.com/prometheus/client_golang/prometheus"
	"sync"
	"time"
)

var _ prometheus.Collector = &CircuitBreaker{}

// CircuitBreaker implements the circuit breaker design pattern.
type CircuitBreaker struct {
	configuration      Configuration
	throttle           chan struct{}
	lock               sync.Mutex
	counters           Counters
	state              State
	openExpiration     time.Time
	halfOpenExpiration time.Time
	metrics
}

// Configuration for the circuit breaker.
type Configuration struct {
	// ErrorThreshold is the number of errors before the circuit breaker opens.
	ErrorThreshold int
	// OpenDuration is how long the circuit breaker stays open before moving to 'half-open' state. Default is 10 seconds.
	OpenDuration time.Duration
	// SuccessThreshold is the number of successful calls that will close the half-open circuit breaker.
	SuccessThreshold int
	// HalfOpenDuration is currently not used.
	HalfOpenDuration time.Duration
	// HalfOpenThrottle limits the number of parallel requests while the circuit is half-open. Default is zero (no throttling).
	HalfOpenThrottle int
	// ShouldOpen overrides when a circuit breaker opens. If nil, the circuit breaker opens after ErrorThreshold consecutive errors.
	ShouldOpen func(Counters) bool
	// ShouldClose overrides when a circuit breaker opens. If nil, the circuit breaker closes after SuccessThreshold consecutive successful calls.
	ShouldClose func(Counters) bool
	// Namespace is used to generate the prometheus metric name
	Namespace string
	// Subsystem is used to generate the prometheus metric name
	Subsystem string
	// Name of the circuit breaker. Added as a label to the prometheus metric
	Name string
}

// ErrCircuitOpen is the error returned by CircuitBreaker.Do when the circuit is open.
var ErrCircuitOpen = errors.New("circuit is open")

// New returns a new CircuitBreaker.
func New(configuration Configuration) *CircuitBreaker {
	if configuration.ShouldOpen == nil {
		configuration.ShouldOpen = defaultShouldOpen(configuration)
	}
	if configuration.ShouldClose == nil {
		configuration.ShouldClose = defaultShouldClose(configuration)
	}
	if configuration.OpenDuration == 0 {
		configuration.OpenDuration = 10 * time.Second
	}
	return &CircuitBreaker{
		configuration: configuration,
		throttle:      make(chan struct{}, configuration.HalfOpenThrottle),
		metrics:       newMetrics(configuration.Namespace, configuration.Subsystem, configuration.Name, nil),
	}
}

func defaultShouldOpen(configuration Configuration) func(counters Counters) bool {
	return func(counters Counters) bool {
		return counters.ConsecutiveErrors >= configuration.ErrorThreshold
	}
}

func defaultShouldClose(configuration Configuration) func(counters Counters) bool {
	return func(counters Counters) bool {
		return counters.ConsecutiveSuccesses >= configuration.SuccessThreshold
	}
}

// GetState returns the State of the circuit breaker.
func (c *CircuitBreaker) GetState() State {
	return c.getState()
}

// GetCounters returns the Counters of the circuit breaker.
func (c *CircuitBreaker) GetCounters() Counters {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.counters
}

// Do executes f() in line with the circuit breaker's state. If the circuit breaker is open, Do does not call f() and returns ErrCircuitOpen.
func (c *CircuitBreaker) Do(f func() error) error {
	state := c.getState()
	if state == StateOpen {
		return ErrCircuitOpen
	}

	if state == StateHalfOpen && c.configuration.HalfOpenThrottle > 0 {
		// simple semaphore implementation to throttle requests while half-open
		c.throttle <- struct{}{}
		defer func() { <-c.throttle }()
	}

	err := f()
	if err == nil {
		c.onSuccess()
	} else {
		c.onError()
	}
	return err
}

func (c *CircuitBreaker) getState() State {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.state == StateOpen && time.Until(c.openExpiration) < 0 {
		c.halfOpen()
	}
	return c.state
}

func (c *CircuitBreaker) onSuccess() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.counters.pass()
	if c.state == StateHalfOpen && c.configuration.ShouldClose(c.counters) {
		c.close()
	}
}

func (c *CircuitBreaker) onError() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.counters.fail()
	// any error during half-open state immediately opens the circuit again. Too harsh?
	if c.state == StateHalfOpen || c.configuration.ShouldOpen(c.counters) {
		c.open()
	}
}

func (c *CircuitBreaker) open() {
	c.setState(StateOpen)
	c.openExpiration = time.Now().Add(c.configuration.OpenDuration)
}
func (c *CircuitBreaker) halfOpen() {
	c.setState(StateHalfOpen)
	c.halfOpenExpiration = time.Now().Add(c.configuration.HalfOpenDuration)
}
func (c *CircuitBreaker) close() {
	c.setState(StateClosed)
}

func (c *CircuitBreaker) setState(state State) {
	c.state = state
	c.counters.reset()
}

func (c *CircuitBreaker) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range c.metrics {
		ch <- desc
	}
}

func (c *CircuitBreaker) Collect(ch chan<- prometheus.Metric) {
	counters := c.GetCounters()
	ch <- prometheus.MustNewConstMetric(c.metrics[stateMetric], prometheus.GaugeValue, float64(c.getState()))
	ch <- prometheus.MustNewConstMetric(c.metrics[consecutiveSuccessesMetric], prometheus.GaugeValue, float64(counters.ConsecutiveSuccesses))
	ch <- prometheus.MustNewConstMetric(c.metrics[consecutiveErrorsMetric], prometheus.GaugeValue, float64(counters.ConsecutiveErrors))
}

// State of the circuit breaker.
type State int

const (
	// StateClosed is a closed circuit breaker: all requests are processed until the circuit breaker opens.
	StateClosed State = iota
	// StateOpen is an open circuit breaker: no requests are processed until the circuit breaker moves to half-open state
	StateOpen
	// StateHalfOpen is a half-open circuit breaker: errors will open the circuit breaker immediately. Sufficient successful calls close the circuit breaker.
	StateHalfOpen
)

var stateStrings = map[State]string{
	StateClosed:   "closed",
	StateHalfOpen: "half-open",
	StateOpen:     "open",
}

// String returns the string representation of a State.
func (s State) String() string {
	if value, ok := stateStrings[s]; ok {
		return value
	}
	return "unknown"
}

// Counters holds the statistics of the performed calls.
// It is passed to Configuration.ShouldOpen and Configuration.ShouldClose and can be used to implement custom behaviour to open and close a circuit breaker.
//
// CircuitBreaker resets the counters after each state change, so the
type Counters struct {
	// Calls is the number of calls performed (successfully or unsuccessfully).
	Calls int
	// Successes is the total number of successful calls.
	Successes int
	// ConsecutiveSuccesses is the number of consecutive successful calls.
	ConsecutiveSuccesses int
	// Errors is the total number of unsuccessful calls.
	Errors int
	// ConsecutiveErrors is the number of consecutive unsuccessful calls.
	ConsecutiveErrors int
}

func (c *Counters) pass() {
	c.Calls++
	c.Successes++
	c.ConsecutiveSuccesses++
	c.ConsecutiveErrors = 0
}

func (c *Counters) fail() {
	c.Calls++
	c.Errors++
	c.ConsecutiveErrors++
	c.ConsecutiveSuccesses = 0
}

func (c *Counters) reset() {
	c.Calls = 0
	c.Successes = 0
	c.ConsecutiveSuccesses = 0
	c.Errors = 0
	c.ConsecutiveErrors = 0
}

const (
	stateMetric                = "state"
	consecutiveSuccessesMetric = "consecutive_successes"
	consecutiveErrorsMetric    = "consecutive_errors"
)

type metrics map[string]*prometheus.Desc

func newMetrics(namespace, subsystem, cbName string, constLabels map[string]string) metrics {
	if cbName != "" {
		if constLabels == nil {
			constLabels = make(map[string]string)
		}
		constLabels["circuit_breaker"] = cbName
	}
	return metrics{
		stateMetric: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "circuit_breaker_state"),
			"state of the circuit breaker (0: closed, 1:open, 2:half-open)",
			nil,
			constLabels,
		),
		consecutiveSuccessesMetric: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "circuit_breaker_consecutive_successes"),
			"consecutive successes",
			nil,
			constLabels,
		),
		consecutiveErrorsMetric: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "circuit_breaker_consecutive_errors"),
			"consecutive errors",
			nil,
			constLabels,
		),
	}
}
