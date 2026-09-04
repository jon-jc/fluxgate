package observability

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds the process's instruments and its registry.
//
// The registry is owned rather than global. A package-level default registry
// makes two things impossible that matter here: running two services in one
// test binary without their metrics colliding, and knowing at a glance which
// instruments a given service actually exposes.
type Metrics struct {
	registry *prometheus.Registry

	// HTTP instruments.
	httpRequests *prometheus.CounterVec
	httpDuration *prometheus.HistogramVec
	httpInFlight prometheus.Gauge

	// Ingestion instruments.
	pointsAccepted *prometheus.CounterVec
	pointsRejected *prometheus.CounterVec
	batchSize      prometheus.Histogram

	// Pipeline instruments.
	publishTotal    *prometheus.CounterVec
	publishDuration prometheus.Histogram
	breakerState    *prometheus.GaugeVec

	// Aggregation instruments.
	messagesTotal   *prometheus.CounterVec
	windowsFlushed  prometheus.Counter
	flushDuration   prometheus.Histogram
	rollupsWritten  prometheus.Counter
	openWindows     prometheus.Gauge
	trackedSeries   prometheus.Gauge
	watermarkLagSec prometheus.Gauge
}

// latencyBuckets span a microsecond to ten seconds.
//
// The boundaries are exponential rather than linear because latency is: the
// difference between 1ms and 2ms matters and the difference between 8s and 9s
// does not, so linear buckets would waste resolution exactly where nothing
// interesting happens.
var latencyBuckets = prometheus.ExponentialBuckets(0.0005, 2, 16)

// NewMetrics builds the instruments for a service.
func NewMetrics(service string) *Metrics {
	registry := prometheus.NewRegistry()

	// The Go runtime and process collectors answer the questions that come
	// first in an incident -- is it memory, is it goroutines, is it GC --
	// before anyone looks at an application metric.
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	labels := prometheus.Labels{"service": service}

	m := &Metrics{
		registry: registry,

		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   "fluxgate",
			Subsystem:   "http",
			Name:        "requests_total",
			Help:        "HTTP requests by route, method and status class.",
			ConstLabels: labels,
			// The route pattern, not the path: labelling by raw path would
			// create a new series per distinct URL, which is the cardinality
			// explosion this system exists to help people find.
		}, []string{"route", "method", "status"}),

		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace:   "fluxgate",
			Subsystem:   "http",
			Name:        "request_duration_seconds",
			Help:        "HTTP request latency by route.",
			ConstLabels: labels,
			Buckets:     latencyBuckets,
		}, []string{"route", "method"}),

		httpInFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   "fluxgate",
			Subsystem:   "http",
			Name:        "requests_in_flight",
			Help:        "HTTP requests currently being served.",
			ConstLabels: labels,
		}),

		pointsAccepted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   "fluxgate",
			Subsystem:   "ingest",
			Name:        "points_accepted_total",
			Help:        "Metric points admitted to the pipeline.",
			ConstLabels: labels,
		}, []string{"tenant"}),

		pointsRejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   "fluxgate",
			Subsystem:   "ingest",
			Name:        "points_rejected_total",
			Help:        "Metric points rejected, by reason.",
			ConstLabels: labels,
		}, []string{"tenant", "reason"}),

		batchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace:   "fluxgate",
			Subsystem:   "ingest",
			Name:        "batch_points",
			Help:        "Points per submitted batch.",
			ConstLabels: labels,
			Buckets:     prometheus.ExponentialBuckets(1, 2, 12),
		}),

		publishTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   "fluxgate",
			Subsystem:   "publish",
			Name:        "batches_total",
			Help:        "Batches published, by outcome.",
			ConstLabels: labels,
		}, []string{"outcome"}),

		publishDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace:   "fluxgate",
			Subsystem:   "publish",
			Name:        "duration_seconds",
			Help:        "Time to publish a batch and receive the broker's acknowledgement.",
			ConstLabels: labels,
			Buckets:     latencyBuckets,
		}),

		breakerState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace:   "fluxgate",
			Subsystem:   "resilience",
			Name:        "breaker_state",
			Help:        "Circuit breaker state: 0 closed, 1 half-open, 2 open.",
			ConstLabels: labels,
		}, []string{"breaker"}),

		messagesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   "fluxgate",
			Subsystem:   "consume",
			Name:        "messages_total",
			Help:        "Messages consumed, by outcome.",
			ConstLabels: labels,
		}, []string{"outcome"}),

		windowsFlushed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "fluxgate",
			Subsystem:   "aggregate",
			Name:        "windows_flushed_total",
			Help:        "Aggregation windows committed to storage.",
			ConstLabels: labels,
		}),

		flushDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace:   "fluxgate",
			Subsystem:   "aggregate",
			Name:        "flush_duration_seconds",
			Help:        "Time to commit a flush transaction.",
			ConstLabels: labels,
			Buckets:     latencyBuckets,
		}),

		rollupsWritten: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "fluxgate",
			Subsystem:   "aggregate",
			Name:        "rollups_written_total",
			Help:        "Series-window rollups written.",
			ConstLabels: labels,
		}),

		openWindows: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   "fluxgate",
			Subsystem:   "aggregate",
			Name:        "open_windows",
			Help:        "Windows currently held in memory.",
			ConstLabels: labels,
		}),

		trackedSeries: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   "fluxgate",
			Subsystem:   "aggregate",
			Name:        "tracked_series",
			Help:        "Distinct series held across open windows. The cardinality bound applies to this.",
			ConstLabels: labels,
		}),

		watermarkLagSec: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "fluxgate",
			Subsystem: "aggregate",
			Name:      "watermark_lag_seconds",
			Help: "How far the event-time watermark trails wall-clock time. " +
				"A steadily rising value means the pipeline is falling behind.",
			ConstLabels: labels,
		}),
	}

	registry.MustRegister(
		m.httpRequests, m.httpDuration, m.httpInFlight,
		m.pointsAccepted, m.pointsRejected, m.batchSize,
		m.publishTotal, m.publishDuration, m.breakerState,
		m.messagesTotal, m.windowsFlushed, m.flushDuration,
		m.rollupsWritten, m.openWindows, m.trackedSeries, m.watermarkLagSec,
	)

	return m
}

// Registry exposes the registry, for tests and for registering collectors a
// service owns.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// Handler serves the scrape endpoint.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{
		// A failing collector must not fail the scrape: a partial answer keeps
		// the rest of the dashboard alive, and the error is reported as a
		// metric of its own.
		ErrorHandling: promhttp.ContinueOnError,
		Registry:      m.registry,
	})
}

// ObserveRequest records one HTTP request.
//
// The route is the registered pattern, never the raw path: labelling by path
// mints a new series per distinct URL, which is precisely the cardinality
// explosion this system exists to help people find.
func (m *Metrics) ObserveRequest(route, method string, status int, d time.Duration) {
	if m == nil {
		return
	}
	m.httpRequests.WithLabelValues(route, method, statusClass(status)).Inc()
	m.httpDuration.WithLabelValues(route, method).Observe(d.Seconds())
}

// TrackInFlight increments the in-flight gauge and returns its decrement.
func (m *Metrics) TrackInFlight() func() {
	if m == nil {
		return func() {}
	}
	m.httpInFlight.Inc()
	return m.httpInFlight.Dec
}

// ObserveIngest records the outcome of one submitted batch.
func (m *Metrics) ObserveIngest(tenant string, accepted, rejected, total int) {
	if m == nil {
		return
	}
	if accepted > 0 {
		m.pointsAccepted.WithLabelValues(tenant).Add(float64(accepted))
	}
	if rejected > 0 {
		m.pointsRejected.WithLabelValues(tenant, "validation").Add(float64(rejected))
	}
	m.batchSize.Observe(float64(total))
}

// ObservePublish records a publish attempt.
func (m *Metrics) ObservePublish(outcome string, d time.Duration) {
	if m == nil {
		return
	}
	m.publishTotal.WithLabelValues(outcome).Inc()
	m.publishDuration.Observe(d.Seconds())
}

// SetBreakerState records a circuit breaker's state.
func (m *Metrics) SetBreakerState(name string, state int) {
	if m == nil {
		return
	}
	m.breakerState.WithLabelValues(name).Set(float64(state))
}

// ObserveMessage records the outcome of one consumed message.
func (m *Metrics) ObserveMessage(outcome string) {
	if m == nil {
		return
	}
	m.messagesTotal.WithLabelValues(outcome).Inc()
}

// ObserveFlush records a completed flush.
func (m *Metrics) ObserveFlush(windows, rollups int, d time.Duration) {
	if m == nil {
		return
	}
	m.windowsFlushed.Add(float64(windows))
	m.rollupsWritten.Add(float64(rollups))
	m.flushDuration.Observe(d.Seconds())
}

// SetAggregationState records the aggregator's in-memory footprint and how far
// the watermark trails wall-clock time.
func (m *Metrics) SetAggregationState(openWindows, series int, watermarkLag time.Duration) {
	if m == nil {
		return
	}
	m.openWindows.Set(float64(openWindows))
	m.trackedSeries.Set(float64(series))
	m.watermarkLagSec.Set(watermarkLag.Seconds())
}

// statusClass buckets a status code as 2xx, 4xx and so on.
//
// The exact code would multiply the series count for no analytical gain: alerts
// and dashboards are written against classes, and the individual code is
// already in the access log where it can be read in context.
func statusClass(status int) string {
	switch {
	case status >= 500:
		return "5xx"
	case status >= 400:
		return "4xx"
	case status >= 300:
		return "3xx"
	case status >= 200:
		return "2xx"
	default:
		return strconv.Itoa(status)
	}
}
