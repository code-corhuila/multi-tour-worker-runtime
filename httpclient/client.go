package httpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strings"
	"time"

	"github.com/code-corhuila/multi-tour-worker-runtime/config"
	"github.com/code-corhuila/multi-tour-worker-runtime/correlation"
)

const (
	maxBody         = 1 << 20          // bytes read from one answer
	maxWait         = 30 * time.Second // ceiling of any wait between attempts
	defaultTimeout  = 5 * time.Second
	defaultAttempts = 3
	defaultBaseWait = 200 * time.Millisecond
)

// Sleeper waits d or until ctx ends; tests replace it so nothing really sleeps.
type Sleeper func(ctx context.Context, d time.Duration) error

// Request is one call to another domain. Path is relative to the client's base URL.
type Request struct {
	Method         string
	Path           string
	Body           any
	IdempotencyKey string
}

// Client calls other domains' APIs as the worker (output adapter).
type Client struct {
	baseURL  string
	token    config.Secret
	http     *http.Client
	timeout  time.Duration
	attempts int
	baseWait time.Duration
	sleep    Sleeper
	jitter   func(max time.Duration) time.Duration
}

// Option changes a default of New.
type Option func(*Client)

// WithTimeout sets the time limit of each attempt (HTTP_TIMEOUT).
func WithTimeout(d time.Duration) Option { return func(c *Client) { c.timeout = d } }

// WithAttempts sets the total number of attempts per call (HTTP_ATTEMPTS).
func WithAttempts(n int) Option { return func(c *Client) { c.attempts = n } }

// WithBaseWait sets the base of the backoff.
func WithBaseWait(d time.Duration) Option { return func(c *Client) { c.baseWait = d } }

// WithSleeper replaces the real wait.
func WithSleeper(s Sleeper) Option { return func(c *Client) { c.sleep = s } }

// WithJitter replaces the random choice in [0, max].
func WithJitter(f func(max time.Duration) time.Duration) Option {
	return func(c *Client) { c.jitter = f }
}

// New returns a client with the defaults of plan D-03: 5 s per attempt, 3 attempts, 200 ms base.
// Options below their minimum fall back to those defaults. Redirects are never followed, so the
// bearer token is only ever sent to baseURL.
func New(baseURL string, token config.Secret, opts ...Option) *Client {
	c := &Client{baseURL: strings.TrimRight(baseURL, "/"), token: token,
		http: &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}},
		timeout: defaultTimeout, attempts: defaultAttempts, baseWait: defaultBaseWait}
	for _, o := range opts {
		o(c)
	}
	if c.timeout <= 0 {
		c.timeout = defaultTimeout
	}
	if c.attempts < 1 {
		c.attempts = defaultAttempts
	}
	if c.baseWait <= 0 {
		c.baseWait = defaultBaseWait
	}
	return c
}

// Get calls GET path and decodes the answer into out (nil to ignore it).
func (c *Client) Get(ctx context.Context, path string, out any) error {
	return c.Do(ctx, Request{Method: http.MethodGet, Path: path}, out)
}

// Do sends req with the token and the context's correlation id, retrying per Annex D rule 7.
func (c *Client) Do(ctx context.Context, req Request, out any) error {
	var payload []byte
	if req.Body != nil {
		var err error
		if payload, err = json.Marshal(req.Body); err != nil {
			return fmt.Errorf("encode request body: %w", err)
		}
	}
	for attempt := 1; ; attempt++ {
		err := c.once(ctx, req, payload, out)
		if err == nil {
			return nil
		}
		if attempt >= c.attempts || ctx.Err() != nil || !retryable(err) || !repeatable(req) {
			return err
		}
		d := c.delay(attempt, err)
		if dl, ok := ctx.Deadline(); ok && time.Until(dl) <= d {
			return err // the wait would outlive the context
		}
		if perr := c.pause(ctx, d); perr != nil {
			return perr
		}
	}
}

// repeatable: a call that creates something is repeated only when it carries an Idempotency-Key.
func repeatable(req Request) bool {
	return req.IdempotencyKey != "" || req.Method == http.MethodGet || req.Method == http.MethodHead
}

func retryable(err error) bool {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.Retryable()
	}
	var tErr *TransportError
	return errors.As(err, &tErr)
}

// delay is the server's Retry-After when it gave one, else a random time in [0, min(base x 2^attempt, maxWait)].
func (c *Client) delay(attempt int, last error) time.Duration {
	var apiErr *APIError
	if errors.As(last, &apiErr) && apiErr.RetryAfter > 0 {
		return apiErr.RetryAfter
	}
	ceiling := c.baseWait
	for i := 0; i < attempt && ceiling < maxWait; i++ {
		ceiling *= 2
	}
	ceiling = min(ceiling, maxWait)
	if c.jitter != nil {
		return c.jitter(ceiling)
	}
	return rand.N(ceiling + 1)
}

func (c *Client) pause(ctx context.Context, d time.Duration) error {
	if c.sleep != nil {
		return c.sleep(ctx, d)
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) once(ctx context.Context, req Request, payload []byte, out any) error {
	actx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	hr, err := http.NewRequestWithContext(actx, req.Method, c.baseURL+"/"+strings.TrimLeft(req.Path, "/"), bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	hr.Header.Set("Authorization", "Bearer "+c.token.Reveal())
	hr.Header.Set("Accept", "application/json")
	if id := correlation.From(ctx); id != "" {
		hr.Header.Set(correlation.Header, id)
	}
	if payload != nil {
		hr.Header.Set("Content-Type", "application/json")
	}
	if req.IdempotencyKey != "" {
		hr.Header.Set("Idempotency-Key", req.IdempotencyKey)
	}
	resp, err := c.http.Do(hr)
	if err != nil {
		return &TransportError{Err: err}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil {
		return &TransportError{Err: err}
	}
	oversized := len(body) > maxBody
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if oversized {
			return &DecodeError{Err: errors.New("body exceeds 1 MiB")}
		}
		if out == nil || len(body) == 0 {
			return nil
		}
		if err := json.Unmarshal(body, out); err != nil {
			return &DecodeError{Err: err}
		}
		return nil
	}
	return c.readAPIError(resp, body)
}
