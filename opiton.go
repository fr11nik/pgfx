package pgfx

import (
	"time"

	"github.com/exaring/otelpgx"
)

// Option -.
type Option func(*Postgres)

// MaxPoolSize -.
func MaxPoolSize(size int32) Option {
	return func(c *Postgres) {
		c.maxPoolSize = size
	}
}

// ConnAttempts -.
func ConnAttempts(attempts int32) Option {
	return func(c *Postgres) {
		c.connAttempts = attempts
	}
}

// ConnTimeout -.
func ConnTimeout(timeout time.Duration) Option {
	return func(c *Postgres) {
		c.connTimeout = timeout
	}
}

func WithTracer() Option {
	return func(p *Postgres) {
		p.qt = otelpgx.NewTracer()
	}
}
