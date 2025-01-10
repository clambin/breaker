package breaker

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	stateMetric                prometheus.Gauge
	consecutiveSuccessesMetric prometheus.Gauge
	consecutiveErrorsMetric    prometheus.Gauge
}

// NewMetrics returns Prometheus metrics to be used with a CircuitBreaker.
func NewMetrics(namespace, subsystem, cbName string, constLabels prometheus.Labels) *Metrics {
	if cbName != "" {
		if constLabels == nil {
			constLabels = make(map[string]string, 1)
		}
		constLabels["circuit_breaker"] = cbName
	}
	return &Metrics{
		stateMetric: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "circuit_breaker_state",
			Help:        "state of the circuit breaker (0: closed, 1:open, 2:half-open)",
			ConstLabels: constLabels,
		}),
		consecutiveSuccessesMetric: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "circuit_breaker_consecutive_successes",
			Help:        "consecutive successes",
			ConstLabels: constLabels,
		}),
		consecutiveErrorsMetric: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "circuit_breaker_consecutive_errors",
			Help:        "consecutive errors",
			ConstLabels: constLabels,
		}),
	}
}

// Describe implements the prometheus.Collector interface.
func (m *Metrics) Describe(ch chan<- *prometheus.Desc) {
	m.stateMetric.Describe(ch)
	m.consecutiveSuccessesMetric.Describe(ch)
	m.consecutiveErrorsMetric.Describe(ch)
}

// Collect implements the prometheus.Collector interface.
func (m *Metrics) Collect(ch chan<- prometheus.Metric) {
	m.stateMetric.Collect(ch)
	m.consecutiveSuccessesMetric.Collect(ch)
	m.consecutiveErrorsMetric.Collect(ch)
}

func (m *Metrics) onStateChange(state State) {
	m.stateMetric.Set(float64(state))
}

func (m *Metrics) onCounterChange(counters Counters) {
	m.consecutiveSuccessesMetric.Set(float64(counters.ConsecutiveSuccesses))
	m.consecutiveErrorsMetric.Set(float64(counters.ConsecutiveErrors))
}
