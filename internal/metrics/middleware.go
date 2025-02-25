package metrics

import (
	"net/http"
	"strconv"
	"time"
)

// responseWriter is a wrapper for http.ResponseWriter that captures the status code
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

// WriteHeader captures the status code before writing it
func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// MetricsMiddleware wraps an HTTP handler with Prometheus metrics
func MetricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Get metrics instance
		metrics := GetMetrics()

		// Track request in flight
		metrics.IncRequestsInFlight()
		defer metrics.DecRequestsInFlight()

		// Capture response status
		rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		// Record start time
		start := time.Now()

		// Call the next handler
		next.ServeHTTP(rw, r)

		// Calculate duration
		duration := time.Since(start).Seconds()

		// Record metrics
		metrics.RecordRequest(r.Method, r.URL.Path, strconv.Itoa(rw.statusCode))
		metrics.ObserveRequestDuration(r.Method, r.URL.Path, duration)
	})
}

// WithMetrics adds metrics middleware to a handler
func WithMetrics(handler http.Handler) http.Handler {
	return MetricsMiddleware(handler)
}

// WrapHandlerFunc wraps an http.HandlerFunc with metrics middleware
func WrapHandlerFunc(handlerFunc http.HandlerFunc) http.Handler {
	return MetricsMiddleware(handlerFunc)
}
