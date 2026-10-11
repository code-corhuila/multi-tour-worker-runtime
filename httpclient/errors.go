package httpclient

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
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

// readAPIError reads the common envelope {error, message, details?, traceId}; a body that is not
// one yields only the status, so foreign text never reaches an error.
func readAPIError(resp *http.Response, body []byte) *APIError {
	e := &APIError{Status: resp.StatusCode, Message: http.StatusText(resp.StatusCode)}
	var env struct {
		Error   string `json:"error"`
		Message string `json:"message"`
		Details any    `json:"details"`
		TraceID string `json:"traceId"`
	}
	if json.Unmarshal(body, &env) == nil && env.Error != "" {
		e.Code, e.Message, e.Details, e.TraceID = env.Error, env.Message, env.Details, env.TraceID
	}
	// shortcut: Retry-After in seconds only, HTTP-date form is ignored; upgrade if a callee sends dates.
	if s, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && s > 0 {
		e.RetryAfter = time.Duration(s) * time.Second
	}
	return e
}

// TransportError is a failure before a full answer arrived (network, timeout, truncated body).
type TransportError struct{ Err error }

func (e *TransportError) Error() string { return "transport: " + e.Err.Error() }

func (e *TransportError) Unwrap() error { return e.Err }
