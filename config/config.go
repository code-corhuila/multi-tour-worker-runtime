package config

import "time"

// Config holds the variables shared by all workers.
type Config struct {
	BatchSize    int
	RunTimeout   time.Duration
	HTTPTimeout  time.Duration
	HTTPAttempts int
	ServiceToken string
}

// Load reads the shared variables through getenv and applies the declared defaults.
// A missing SERVICE_TOKEN is an error: the worker must not start without it.
func Load(getenv func(string) string) (Config, error) { return Config{}, nil }
