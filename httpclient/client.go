package httpclient

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/code-corhuila/multi-tour-worker-runtime/config"
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
func New(baseURL string, token config.Secret, opts ...Option) *Client {
	c := &Client{baseURL: baseURL, token: token, http: &http.Client{},
		timeout: 5 * time.Second, attempts: 3, baseWait: 200 * time.Millisecond}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Get calls GET path and decodes the answer into out (nil to ignore it).
func (c *Client) Get(ctx context.Context, path string, out any) error {
	return c.Do(ctx, Request{Method: http.MethodGet, Path: path}, out)
}

// Do sends req with the token and the context's correlation id, retrying per Annex D rule 7.
func (c *Client) Do(ctx context.Context, req Request, out any) error {
	return errors.New("not implemented")
}
