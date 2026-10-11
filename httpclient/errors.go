package httpclient

import (
	"fmt"
	"net/http"
	"time"
)

// APIError is an answer with a non-2xx status, read from the common error envelope.
// Message and Code come from the callee's envelope; a body that is not an envelope is never copied.
type APIError struct {
	Status     int
	Code       string
	Message    string
	Details    any
	TraceID    string
	RetryAfter time.Duration
}

func (e *APIError) Error() string {
	return fmt.Sprintf("http %d %s: %s (traceId %s)", e.Status, e.Code, e.Message, e.TraceID)
}

// Retryable reports whether Annex D rule 7 allows another attempt: 429 and 5xx only.
func (e *APIError) Retryable() bool {
	return e.Status == http.StatusTooManyRequests || e.Status >= http.StatusInternalServerError
}

// TransportError is a failure before a full answer arrived (network, timeout, truncated body).
type TransportError struct{ Err error }

func (e *TransportError) Error() string { return "transport: " + e.Err.Error() }

func (e *TransportError) Unwrap() error { return e.Err }
