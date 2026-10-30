package voice

import (
	"net/http"
	"time"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/accounts"
)

// outcomeRecorder is implemented by *accounts.Pool. It is optional so narrow
// test doubles of AccountRepository keep working.
type outcomeRecorder interface {
	RecordOutcome(token string, outcome accounts.Outcome) (accounts.HealthState, error)
}

// classifyUpstream maps an upstream attempt to a pool outcome. ok is false when
// the result says nothing about account health (for example a 400 or a JSON
// 403). Cloudflare challenge pages are never treated as 401, whatever their
// status code.
func classifyUpstream(status int, contentType, body, retryAfter string, err error, now time.Time) (accounts.Outcome, bool) {
	if err != nil {
		return accounts.Outcome{Kind: accounts.OutcomeTransient}, true
	}
	html := classifyProbeBody(contentType, body) == "html"
	switch {
	case status == http.StatusTooManyRequests:
		return accounts.Outcome{
			Kind:       accounts.OutcomeRateLimited,
			RetryAfter: accounts.ParseRetryAfter(retryAfter, now),
		}, true
	case status >= 500, html:
		return accounts.Outcome{Kind: accounts.OutcomeTransient}, true
	case status == http.StatusUnauthorized:
		return accounts.Outcome{Kind: accounts.OutcomeUnauthorized}, true
	case status >= 200 && status < 300:
		return accounts.Outcome{Kind: accounts.OutcomeSuccess}, true
	}
	return accounts.Outcome{}, false
}

// reportOutcome classifies an upstream result and records it on the pool.
// started is when the request began, so late results cannot overwrite newer
// state. It returns the resulting health state (zero when nothing applied).
func (s *Service) reportOutcome(token string, started time.Time, status int, contentType, body, retryAfter string, err error) accounts.HealthState {
	recorder, ok := s.pool.(outcomeRecorder)
	if !ok {
		return accounts.HealthState{}
	}
	outcome, relevant := classifyUpstream(status, contentType, body, retryAfter, err, time.Now())
	if !relevant {
		return accounts.HealthState{}
	}
	outcome.ObservedAt = started
	state, recErr := recorder.RecordOutcome(token, outcome)
	if recErr != nil {
		s.logger.Error("account_outcome_record_failed", "error", recErr)
	}
	return state
}
