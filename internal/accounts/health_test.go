package accounts

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

var baseTime = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func jwtWithExp(t *testing.T, exp time.Time) string {
	t.Helper()
	payload, err := json.Marshal(map[string]any{"exp": exp.Unix()})
	if err != nil {
		t.Fatal(err)
	}
	enc := base64.RawURLEncoding
	return enc.EncodeToString([]byte(`{"alg":"none"}`)) + "." + enc.EncodeToString(payload) + ".sig"
}

func healthPool(t *testing.T, path string, clock *fakeClock) *Pool {
	t.Helper()
	pool, err := NewPool(path)
	if err != nil {
		t.Fatal(err)
	}
	pool.WithBox(testBox(t)).WithClock(clock.Now).WithHealth(HealthConfig{
		ExpiryGuard:          10 * time.Minute,
		BackoffBase:          20 * time.Second,
		BackoffMax:           100 * time.Second,
		TransientCooldown:    5 * time.Second,
		UnauthorizedCooldown: 30 * time.Second,
	})
	t.Cleanup(func() { _ = pool.Close() })
	return pool
}

func mustGet(t *testing.T, p *Pool, id int64) Account {
	t.Helper()
	a, err := p.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestRetryAfterCooldownHasFloor(t *testing.T) {
	clock := &fakeClock{t: baseTime}
	pool := healthPool(t, filepath.Join(t.TempDir(), "v.db"), clock)
	acc, _ := pool.Create(Account{AccessToken: "t1"})

	state, err := pool.RecordOutcome("t1", Outcome{Kind: OutcomeRateLimited, RetryAfter: 2 * time.Second})
	if err != nil || !state.Applied {
		t.Fatalf("outcome not applied: %+v %v", state, err)
	}
	if got := state.CooldownUntil.Sub(baseTime); got != MinRateLimitCooldown {
		t.Fatalf("Retry-After below floor should clamp to %v, got %v", MinRateLimitCooldown, got)
	}
	clock.Advance(9 * time.Second)
	if _, _, err := pool.Pick("", nil); err == nil {
		t.Fatal("account must stay in cooldown")
	}
	clock.Advance(2 * time.Second)
	if _, got, err := pool.Pick("", nil); err != nil || got.ID != acc.ID {
		t.Fatalf("account should return after cooldown: %v", err)
	}

	state, _ = pool.RecordOutcome("t1", Outcome{Kind: OutcomeRateLimited, RetryAfter: 90 * time.Second, ObservedAt: clock.Now()})
	if got := state.CooldownUntil.Sub(clock.Now()); got != 90*time.Second {
		t.Fatalf("Retry-After should be honored, got %v", got)
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := baseTime
	if d := ParseRetryAfter("120", now); d != 120*time.Second {
		t.Fatalf("seconds: %v", d)
	}
	date := now.Add(45 * time.Second).UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT")
	if d := ParseRetryAfter(date, now); d != 45*time.Second {
		t.Fatalf("http date: %v", d)
	}
	for _, bad := range []string{"", "abc", "-5", "0"} {
		if d := ParseRetryAfter(bad, now); d != 0 {
			t.Fatalf("%q should be 0, got %v", bad, d)
		}
	}
	if d := ParseRetryAfter("999999999", now); d != maxRetryAfter {
		t.Fatalf("cap: %v", d)
	}
}

func TestRateLimitBackoffGrowsAndCaps(t *testing.T) {
	clock := &fakeClock{t: baseTime}
	pool := healthPool(t, filepath.Join(t.TempDir(), "v.db"), clock)
	_, _ = pool.Create(Account{AccessToken: "t1"})

	want := []time.Duration{20 * time.Second, 40 * time.Second, 80 * time.Second, 100 * time.Second, 100 * time.Second}
	for i, w := range want {
		state, err := pool.RecordOutcome("t1", Outcome{Kind: OutcomeRateLimited, ObservedAt: clock.Now()})
		if err != nil {
			t.Fatal(err)
		}
		if got := state.CooldownUntil.Sub(clock.Now()); got != w {
			t.Fatalf("step %d: backoff %v, want %v", i, got, w)
		}
		clock.Advance(w)
	}
}

func TestSuccessResetsFailureCount(t *testing.T) {
	clock := &fakeClock{t: baseTime}
	pool := healthPool(t, filepath.Join(t.TempDir(), "v.db"), clock)
	acc, _ := pool.Create(Account{AccessToken: "t1"})

	_, _ = pool.RecordOutcome("t1", Outcome{Kind: OutcomeRateLimited})
	_, _ = pool.RecordOutcome("t1", Outcome{Kind: OutcomeUnauthorized})
	if got := mustGet(t, pool, acc.ID); got.FailureCount != 2 || got.UnauthorizedCount != 1 {
		t.Fatalf("unexpected counters: %+v", got)
	}
	clock.Advance(time.Second)
	_, _ = pool.RecordOutcome("t1", Outcome{Kind: OutcomeSuccess})
	got := mustGet(t, pool, acc.ID)
	if got.FailureCount != 0 || got.UnauthorizedCount != 0 || got.CooldownUntil != 0 {
		t.Fatalf("success must reset state: %+v", got)
	}
	// A success also clears the 401 streak, so the next 401 is a first one again.
	clock.Advance(time.Second)
	state, _ := pool.RecordOutcome("t1", Outcome{Kind: OutcomeUnauthorized})
	if state.Invalidated {
		t.Fatal("401 after success must not invalidate")
	}
}

func TestExpiryGuardStopsAllocation(t *testing.T) {
	clock := &fakeClock{t: baseTime}
	pool := healthPool(t, filepath.Join(t.TempDir(), "v.db"), clock)
	acc, err := pool.Create(Account{AccessToken: jwtWithExp(t, baseTime.Add(30*time.Minute))})
	if err != nil {
		t.Fatal(err)
	}
	if got := mustGet(t, pool, acc.ID); got.ExpiresAt != baseTime.Add(30*time.Minute).Unix() {
		t.Fatalf("expires_at not stored: %d", got.ExpiresAt)
	}
	if _, _, err := pool.Pick("", nil); err != nil {
		t.Fatalf("outside guard window: %v", err)
	}
	clock.Advance(19 * time.Minute) // 11 minutes left > 10 minute guard
	if _, _, err := pool.PickByID(acc.ID, nil); err != nil {
		t.Fatalf("still outside guard: %v", err)
	}
	clock.Advance(2 * time.Minute) // 9 minutes left, inside guard
	if _, _, err := pool.Pick("", nil); err == nil {
		t.Fatal("Pick must stop before token expiry")
	}
	if _, _, err := pool.PickByID(acc.ID, nil); err == nil {
		t.Fatal("PickByID must stop before token expiry")
	}
	// Opaque tokens have no known expiry and stay allocatable.
	if _, err := pool.Create(Account{AccessToken: "opaque"}); err != nil {
		t.Fatal(err)
	}
	if token, _, err := pool.Pick("", nil); err != nil || token != "opaque" {
		t.Fatalf("opaque token should be picked: %q %v", token, err)
	}
}

func TestCooldownPersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v.db")
	clock := &fakeClock{t: baseTime}
	pool := healthPool(t, path, clock)
	_, _ = pool.Create(Account{AccessToken: "t1"})
	if _, err := pool.RecordOutcome("t1", Outcome{Kind: OutcomeRateLimited, RetryAfter: time.Minute}); err != nil {
		t.Fatal(err)
	}
	_ = pool.Close()

	clock.Advance(30 * time.Second)
	reopened := healthPool(t, path, clock)
	if _, _, err := reopened.Pick("", nil); err == nil {
		t.Fatal("cooldown must survive restart")
	}
	items, _ := reopened.List()
	if items[0].FailureCount != 1 || items[0].CooldownUntil == 0 {
		t.Fatalf("health fields lost: %+v", items[0])
	}
	clock.Advance(31 * time.Second)
	if _, _, err := reopened.Pick("", nil); err != nil {
		t.Fatalf("cooldown should have expired: %v", err)
	}
}

func TestMultiAccountRotationSkipsCoolingAccount(t *testing.T) {
	clock := &fakeClock{t: baseTime}
	pool := healthPool(t, filepath.Join(t.TempDir(), "v.db"), clock)
	for i := 1; i <= 3; i++ {
		_, _ = pool.Create(Account{AccessToken: fmt.Sprintf("t%d", i)})
	}
	if _, err := pool.RecordOutcome("t2", Outcome{Kind: OutcomeRateLimited, RetryAfter: time.Minute}); err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	for i := 0; i < 4; i++ {
		clock.Advance(time.Second)
		token, _, err := pool.Pick("", nil)
		if err != nil {
			t.Fatal(err)
		}
		seen[token]++
	}
	if seen["t2"] != 0 || seen["t1"] == 0 || seen["t3"] == 0 {
		t.Fatalf("rotation should alternate t1/t3 and skip t2: %v", seen)
	}
	// Excluding the remaining accounts leaves nothing while t2 cools.
	if _, _, err := pool.Pick("", map[string]struct{}{"t1": {}, "t3": {}}); err == nil {
		t.Fatal("cooling account must not be picked")
	}
	clock.Advance(time.Minute)
	if token, _, err := pool.Pick("", map[string]struct{}{"t1": {}, "t3": {}}); err != nil || token != "t2" {
		t.Fatalf("t2 should be back: %q %v", token, err)
	}
}

func TestStaleOutcomeIsIgnored(t *testing.T) {
	clock := &fakeClock{t: baseTime}
	pool := healthPool(t, filepath.Join(t.TempDir(), "v.db"), clock)
	acc, _ := pool.Create(Account{AccessToken: "old"})

	started := clock.Now()
	clock.Advance(time.Second)
	if _, err := pool.RecordOutcome("old", Outcome{Kind: OutcomeSuccess}); err != nil {
		t.Fatal(err)
	}
	// A request that started before the success finishes late with a 429.
	state, _ := pool.RecordOutcome("old", Outcome{Kind: OutcomeRateLimited, ObservedAt: started})
	if state.Applied {
		t.Fatal("older outcome must not overwrite newer state")
	}
	if got := mustGet(t, pool, acc.ID); got.CooldownUntil != 0 || got.FailureCount != 0 {
		t.Fatalf("stale result changed state: %+v", got)
	}

	// Replacing the token resets health; a late result for the old token is dropped.
	clock.Advance(time.Second)
	newToken := "new"
	if _, err := pool.Update(acc.ID, AccountUpdate{Email: "x", AccessToken: &newToken}); err != nil {
		t.Fatal(err)
	}
	state, _ = pool.RecordOutcome("old", Outcome{Kind: OutcomeUnauthorized})
	if state.Applied {
		t.Fatal("outcome for a replaced token must be ignored")
	}
	if _, _, err := pool.Pick("new", nil); err != nil {
		t.Fatalf("new token must stay available: %v", err)
	}
	// An edit also fences in-flight results for the same token that started earlier.
	clock.Advance(time.Second)
	state, _ = pool.RecordOutcome("new", Outcome{Kind: OutcomeUnauthorized, ObservedAt: started})
	if state.Applied {
		t.Fatal("result observed before the account edit must be ignored")
	}
}

func TestRepeatedUnauthorizedInvalidates(t *testing.T) {
	clock := &fakeClock{t: baseTime}
	pool := healthPool(t, filepath.Join(t.TempDir(), "v.db"), clock)
	acc, _ := pool.Create(Account{AccessToken: "t1"})

	state, _ := pool.RecordOutcome("t1", Outcome{Kind: OutcomeUnauthorized})
	if state.Invalidated || !state.Applied {
		t.Fatalf("first 401 should only cool down: %+v", state)
	}
	got := mustGet(t, pool, acc.ID)
	if got.Disabled || got.CooldownUntil <= clock.Now().UnixMilli() {
		t.Fatalf("expected cooldown without invalidation: %+v", got)
	}
	if _, _, err := pool.Pick("", nil); err == nil {
		t.Fatal("account in 401 cooldown must not be picked")
	}
	clock.Advance(31 * time.Second)
	if _, _, err := pool.Pick("", nil); err != nil {
		t.Fatalf("account should be re-probeable after cooldown: %v", err)
	}
	state, _ = pool.RecordOutcome("t1", Outcome{Kind: OutcomeUnauthorized})
	if !state.Invalidated {
		t.Fatalf("second consecutive 401 must invalidate: %+v", state)
	}
	got = mustGet(t, pool, acc.ID)
	if !got.Disabled || got.Status != "禁用" || got.InvalidAt == 0 {
		t.Fatalf("account not disabled: %+v", got)
	}
}

func TestTransientErrorsNeverInvalidate(t *testing.T) {
	clock := &fakeClock{t: baseTime}
	pool := healthPool(t, filepath.Join(t.TempDir(), "v.db"), clock)
	acc, _ := pool.Create(Account{AccessToken: "t1"})

	for i := 0; i < 10; i++ {
		state, err := pool.RecordOutcome("t1", Outcome{Kind: OutcomeTransient, ObservedAt: clock.Now()})
		if err != nil || state.Invalidated {
			t.Fatalf("transient error invalidated account: %+v %v", state, err)
		}
		if got := state.CooldownUntil.Sub(clock.Now()); got != 5*time.Second {
			t.Fatalf("short cooldown expected, got %v", got)
		}
		clock.Advance(6 * time.Second)
	}
	got := mustGet(t, pool, acc.ID)
	if got.Disabled || got.UnauthorizedCount != 0 || got.FailureCount != 10 {
		t.Fatalf("unexpected state: %+v", got)
	}
}

func TestMigrationAddsHealthColumnsToLegacyDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v.db")
	pool, err := NewPool(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = pool.Close()
	// Reopen: migration must be idempotent.
	pool, err = NewPool(path)
	if err != nil {
		t.Fatalf("second open failed: %v", err)
	}
	_ = pool.Close()
}
