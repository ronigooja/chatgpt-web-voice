package accounts

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/tokenutil"
)

const (
	// MinRateLimitCooldown is the floor applied to a 429 Retry-After.
	MinRateLimitCooldown = 10 * time.Second
	// maxRetryAfter bounds a hostile or mistaken Retry-After header.
	maxRetryAfter = time.Hour
	// invalidAfterUnauthorized is the number of consecutive 401 outcomes that
	// disables an account. The first one only cools down for a re-probe.
	invalidAfterUnauthorized = 2
)

// OutcomeKind classifies one upstream attempt made with an account token.
type OutcomeKind int

const (
	OutcomeSuccess OutcomeKind = iota + 1
	// OutcomeRateLimited is an upstream 429.
	OutcomeRateLimited
	// OutcomeTransient covers 5xx, network errors, and Cloudflare challenge
	// pages. It never invalidates an account.
	OutcomeTransient
	// OutcomeUnauthorized is a real JSON 401 from the API.
	OutcomeUnauthorized
)

// Outcome is one reported upstream result.
type Outcome struct {
	Kind       OutcomeKind
	RetryAfter time.Duration
	// ObservedAt is when the request started. Zero means "now". An outcome older
	// than the stored state (a newer outcome, or an account edit) is ignored.
	ObservedAt time.Time
}

// HealthState describes the account after an outcome was considered.
type HealthState struct {
	// Applied is false when the outcome was stale, the token is unknown, or the
	// account is disabled.
	Applied           bool
	Invalidated       bool
	CooldownUntil     time.Time
	FailureCount      int
	UnauthorizedCount int
}

// HealthConfig tunes cooldown and expiry behavior. Zero values use defaults.
type HealthConfig struct {
	ExpiryGuard          time.Duration
	BackoffBase          time.Duration
	BackoffMax           time.Duration
	TransientCooldown    time.Duration
	UnauthorizedCooldown time.Duration
}

// DefaultHealthConfig returns the built-in settings.
func DefaultHealthConfig() HealthConfig {
	return HealthConfig{
		ExpiryGuard:          5 * time.Minute,
		BackoffBase:          30 * time.Second,
		BackoffMax:           30 * time.Minute,
		TransientCooldown:    15 * time.Second,
		UnauthorizedCooldown: time.Minute,
	}
}

func (c HealthConfig) withDefaults() HealthConfig {
	d := DefaultHealthConfig()
	if c.ExpiryGuard < 0 {
		c.ExpiryGuard = 0
	} else if c.ExpiryGuard == 0 {
		c.ExpiryGuard = d.ExpiryGuard
	}
	if c.BackoffBase <= 0 {
		c.BackoffBase = d.BackoffBase
	}
	if c.BackoffMax <= 0 {
		c.BackoffMax = d.BackoffMax
	}
	if c.BackoffMax < c.BackoffBase {
		c.BackoffMax = c.BackoffBase
	}
	if c.TransientCooldown <= 0 {
		c.TransientCooldown = d.TransientCooldown
	}
	if c.UnauthorizedCooldown <= 0 {
		c.UnauthorizedCooldown = d.UnauthorizedCooldown
	}
	return c
}

// WithHealth configures cooldown and expiry guard settings.
func (p *Pool) WithHealth(cfg HealthConfig) *Pool {
	if p != nil {
		p.health = cfg.withDefaults()
	}
	return p
}

// WithClock replaces the pool clock; intended for tests.
func (p *Pool) WithClock(now func() time.Time) *Pool {
	if p != nil && now != nil {
		p.clock = now
	}
	return p
}

func (p *Pool) now() time.Time {
	if p.clock != nil {
		return p.clock()
	}
	return time.Now()
}

func (p *Pool) healthConfig() HealthConfig {
	if p.health == (HealthConfig{}) {
		return DefaultHealthConfig()
	}
	return p.health
}

// usable reports whether an enabled account may be handed out right now.
func (p *Pool) usable(account Account) bool {
	now := p.now()
	if account.CooldownUntil > now.UnixMilli() {
		return false
	}
	if account.ExpiresAt > 0 &&
		!now.Add(p.healthConfig().ExpiryGuard).Before(time.Unix(account.ExpiresAt, 0)) {
		return false
	}
	return true
}

// ParseRetryAfter reads a Retry-After header value (delta seconds or HTTP
// date). It returns 0 when the value is missing or unusable.
func ParseRetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	var d time.Duration
	if secs, err := strconv.ParseInt(value, 10, 64); err == nil {
		if secs <= 0 {
			return 0
		}
		if secs > int64(maxRetryAfter/time.Second) {
			return maxRetryAfter
		}
		d = time.Duration(secs) * time.Second
	} else if t, err := http.ParseTime(value); err == nil {
		d = t.Sub(now)
	}
	if d <= 0 {
		return 0
	}
	if d > maxRetryAfter {
		d = maxRetryAfter
	}
	return d
}

func (c HealthConfig) backoff(failures int) time.Duration {
	d := c.BackoffBase
	for i := 1; i < failures && d < c.BackoffMax; i++ {
		d *= 2
	}
	if d > c.BackoffMax {
		d = c.BackoffMax
	}
	return d
}

// RecordOutcome folds one upstream result into the persisted account state.
// The token identifies the account version: an outcome for a token that has
// since been replaced finds no row and is ignored, as is one observed before
// the latest recorded outcome or account edit.
func (p *Pool) RecordOutcome(token string, outcome Outcome) (HealthState, error) {
	token = strings.TrimSpace(token)
	if token == "" || outcome.Kind < OutcomeSuccess || outcome.Kind > OutcomeUnauthorized {
		return HealthState{}, nil
	}
	hash := p.tokenHash(token)
	p.db.Lock()
	defer p.db.Unlock()

	var id int64
	var disabled int
	var cooldownMs, outcomeMs int64
	var failures, unauthorized int
	err := p.db.Conn().QueryRow(
		`SELECT id, disabled, cooldown_until, failure_count, unauthorized_count, outcome_at
		 FROM accounts WHERE token_hash = ?`, hash,
	).Scan(&id, &disabled, &cooldownMs, &failures, &unauthorized, &outcomeMs)
	if errors.Is(err, sql.ErrNoRows) {
		return HealthState{}, nil
	}
	if err != nil {
		return HealthState{}, fmt.Errorf("read account health: %w", err)
	}
	now := p.now()
	observed := outcome.ObservedAt
	if observed.IsZero() {
		observed = now
	}
	if disabled != 0 || observed.UnixMilli() < outcomeMs {
		return HealthState{}, nil
	}

	cfg := p.healthConfig()
	state := HealthState{Applied: true}
	cooldown := time.Duration(0)
	switch outcome.Kind {
	case OutcomeSuccess:
		failures, unauthorized, cooldownMs = 0, 0, 0
	case OutcomeRateLimited:
		failures++
		if outcome.RetryAfter > 0 {
			cooldown = outcome.RetryAfter
			if cooldown < MinRateLimitCooldown {
				cooldown = MinRateLimitCooldown
			}
		} else {
			cooldown = cfg.backoff(failures)
		}
	case OutcomeTransient:
		failures++
		cooldown = cfg.TransientCooldown
	case OutcomeUnauthorized:
		failures++
		unauthorized++
		cooldown = cfg.UnauthorizedCooldown
	}
	if cooldown > 0 {
		if until := now.Add(cooldown).UnixMilli(); until > cooldownMs {
			cooldownMs = until
		}
	}

	if outcome.Kind == OutcomeUnauthorized && unauthorized >= invalidAfterUnauthorized {
		_, err = p.db.Conn().Exec(`
			UPDATE accounts SET disabled = 1, status = '禁用', invalid_at = ?,
				cooldown_until = ?, failure_count = ?, unauthorized_count = ?, outcome_at = ?,
				updated_at = CURRENT_TIMESTAMP
			WHERE id = ?`,
			float64(now.Unix()), cooldownMs, failures, unauthorized, observed.UnixMilli(), id)
		state.Invalidated = true
	} else {
		_, err = p.db.Conn().Exec(`
			UPDATE accounts SET cooldown_until = ?, failure_count = ?, unauthorized_count = ?,
				outcome_at = ?, updated_at = CURRENT_TIMESTAMP
			WHERE id = ?`,
			cooldownMs, failures, unauthorized, observed.UnixMilli(), id)
	}
	if err != nil {
		return HealthState{}, fmt.Errorf("record account outcome: %w", err)
	}
	state.FailureCount = failures
	state.UnauthorizedCount = unauthorized
	if cooldownMs > 0 {
		state.CooldownUntil = time.UnixMilli(cooldownMs)
	}
	return state, nil
}

// tokenExpiry returns the JWT exp (unix seconds) or 0 when unknown.
func tokenExpiry(token string) int64 {
	exp, err := tokenutil.ParseAccessTokenExpiry(token)
	if err != nil || !exp.HasExp {
		return 0
	}
	return exp.Exp
}
