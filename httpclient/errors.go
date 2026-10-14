package httpclient

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// APIError is an answer with a non-2xx status, read from the common error envelope.
// Message and Code come from the callee's envelope; a body that is not an envelope is never copied.
type APIError struct {
	Status     int
	Code       string
	Message    string
	Details    string
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
// Every copied field is stripped of the token and cut to 500 runes.
func (c *Client) readAPIError(resp *http.Response, body []byte) *APIError {
	e := &APIError{Status: resp.StatusCode, Message: http.StatusText(resp.StatusCode)}
	var env struct {
		Error   string          `json:"error"`
		Message string          `json:"message"`
		Details json.RawMessage `json:"details"`
		TraceID string          `json:"traceId"`
	}
	if json.Unmarshal(body, &env) == nil && env.Error != "" {
		e.Code, e.Message = c.clean(env.Error), c.clean(env.Message)
		e.Details, e.TraceID = c.clean(string(env.Details)), c.clean(env.TraceID)
	}
	// shortcut: Retry-After in seconds only, HTTP-date form is ignored; upgrade if a callee sends dates.
	if s, err := strconv.ParseInt(resp.Header.Get("Retry-After"), 10, 64); err == nil && s > 0 {
		e.RetryAfter = maxWait
		if s < int64(maxWait/time.Second) {
			e.RetryAfter = time.Duration(s) * time.Second
		}
	}
	return e
}

func (c *Client) clean(s string) string {
	if t := c.token.Reveal(); t != "" {
		s = strings.ReplaceAll(s, t, "***")
	}
	if r := []rune(s); len(r) > 500 {
		s = string(r[:500])
	}
	return s
}

// TransportError is a failure before a full answer arrived (network, timeout, truncated body).
type TransportError struct{ Err error }

func (e *TransportError) Error() string { return "transport: " + e.Err.Error() }

func (e *TransportError) Unwrap() error { return e.Err }

// DecodeError is a 2xx answer that could not be used: invalid JSON or a body over the size limit.
// It is never retried.
type DecodeError struct{ Err error }

func (e *DecodeError) Error() string { return "decode response: " + e.Err.Error() }

func (e *DecodeError) Unwrap() error { return e.Err }
