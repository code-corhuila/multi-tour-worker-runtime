package config

import (
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"
)

// Config holds the variables shared by all workers.
type Config struct {
	BatchSize    int
	RunTimeout   time.Duration
	HTTPTimeout  time.Duration
	HTTPAttempts int
	ServiceToken Secret
}

// Secret is a credential that never prints itself; Reveal gives the real value.
type Secret string

// Reveal returns the real value.
func (s Secret) Reveal() string { return string(s) }

// String, GoString and LogValue cover fmt (%v, %s, %#v) and slog, so the token cannot leak by accident.
func (Secret) String() string { return "***" }

func (Secret) GoString() string { return "***" }

func (Secret) LogValue() slog.Value { return slog.StringValue("***") }

// Load reads the shared variables through getenv and applies the declared defaults.
// A missing SERVICE_TOKEN is an error: the worker must not start without it.
func Load(getenv func(string) string) (Config, error) {
	c := Config{ServiceToken: Secret(getenv("SERVICE_TOKEN"))}
	if c.ServiceToken == "" {
		return Config{}, errors.New("SERVICE_TOKEN is required")
	}
	var err error
	if c.BatchSize, err = positiveInt(getenv, "BATCH_SIZE", 100); err != nil {
		return Config{}, err
	}
	if c.HTTPAttempts, err = positiveInt(getenv, "HTTP_ATTEMPTS", 3); err != nil {
		return Config{}, err
	}
	if c.RunTimeout, err = positiveDuration(getenv, "RUN_TIMEOUT", 30*time.Second); err != nil {
		return Config{}, err
	}
	if c.HTTPTimeout, err = positiveDuration(getenv, "HTTP_TIMEOUT", 5*time.Second); err != nil {
		return Config{}, err
	}
	return c, nil
}

func positiveInt(getenv func(string) string, name string, def int) (int, error) {
	s := getenv(name)
	if s == "" {
		return def, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("%s must be a positive integer, got %q", name, s)
	}
	return n, nil
}

func positiveDuration(getenv func(string) string, name string, def time.Duration) (time.Duration, error) {
	s := getenv(name)
	if s == "" {
		return def, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration, got %q", name, s)
	}
	return d, nil
}
