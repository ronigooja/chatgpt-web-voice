package voice

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/accounts"
)

func TestClassifyUpstream(t *testing.T) {
	now := time.Now()
	html := `<!DOCTYPE html><html><body>Just a moment...</body></html>`
	cases := []struct {
		name        string
		status      int
		contentType string
		body        string
		retryAfter  string
		err         error
		kind        accounts.OutcomeKind
		relevant    bool
		retry       time.Duration
	}{
		{"401 json", 401, "application/json", `{"detail":"x"}`, "", nil, accounts.OutcomeUnauthorized, true, 0},
		{"401 cloudflare html", 401, "text/html", html, "", nil, accounts.OutcomeTransient, true, 0},
		{"403 cloudflare html", 403, "text/html", html, "", nil, accounts.OutcomeTransient, true, 0},
		{"429 retry-after", 429, "application/json", `{}`, "42", nil, accounts.OutcomeRateLimited, true, 42 * time.Second},
		{"429 no header", 429, "application/json", `{}`, "", nil, accounts.OutcomeRateLimited, true, 0},
		{"503", 503, "application/json", `{}`, "", nil, accounts.OutcomeTransient, true, 0},
		{"network", 0, "", "", "", errors.New("dial timeout"), accounts.OutcomeTransient, true, 0},
		{"200", 200, "application/sdp", "v=0", "", nil, accounts.OutcomeSuccess, true, 0},
		{"400", 400, "application/json", `{}`, "", nil, 0, false, 0},
		{"json 403", http.StatusForbidden, "application/json", `{}`, "", nil, 0, false, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, relevant := classifyUpstream(tc.status, tc.contentType, tc.body, tc.retryAfter, tc.err, now)
			if relevant != tc.relevant || (relevant && (got.Kind != tc.kind || got.RetryAfter != tc.retry)) {
				t.Fatalf("got %+v relevant=%v", got, relevant)
			}
		})
	}
}
