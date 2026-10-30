package voice

import (
	"encoding/json"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/accounts"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/config"
)

// These tests pin the /realtime/wm request and response shape as implemented
// today, against a synthetic local upstream. Tokens and SDP are fake.

const (
	fixtureTokenA = "synthetic-a"
	fixtureTokenB = "synthetic-b"
)

// Header names authHeaders is expected to send on /realtime/wm. Names only.
var wmRequiredHeaders = []string{
	"Accept", "Accept-Language", "Cache-Control", "Pragma", "Priority",
	"Origin", "Referer", "User-Agent",
	"Sec-Ch-Ua", "Sec-Ch-Ua-Arch", "Sec-Ch-Ua-Bitness", "Sec-Ch-Ua-Full-Version",
	"Sec-Ch-Ua-Full-Version-List", "Sec-Ch-Ua-Mobile", "Sec-Ch-Ua-Model",
	"Sec-Ch-Ua-Platform", "Sec-Ch-Ua-Platform-Version",
	"Sec-Fetch-Dest", "Sec-Fetch-Mode", "Sec-Fetch-Site",
	"Oai-Device-Id", "Oai-Session-Id", "Oai-Language", "Oai-Client-Version", "Oai-Client-Build-Number",
	"Authorization", "Content-Type",
}

type wmCapture struct {
	Method   string
	Path     string
	Query    string
	Header   http.Header
	Fields   map[string]string
	Boundary string
	ParseErr error
}

type wmFixture struct {
	svc  *Service
	pool *accounts.Pool

	mu       sync.Mutex
	captures []wmCapture
}

func (f *wmFixture) requests() []wmCapture {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]wmCapture(nil), f.captures...)
}

func (f *wmFixture) only(t *testing.T) wmCapture {
	t.Helper()
	got := f.requests()
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 upstream request, got %d", len(got))
	}
	return got[0]
}

// newWMFixture starts a TLS upstream. respond is called after the request has
// been parsed and recorded.
func newWMFixture(t *testing.T, tokens []string, attempts int, respond func(w http.ResponseWriter, r *http.Request, c wmCapture)) *wmFixture {
	t.Helper()
	f := &wmFixture{}
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := wmCapture{
			Method: r.Method,
			Path:   r.URL.Path,
			Query:  r.URL.RawQuery,
			Header: r.Header.Clone(),
			Fields: map[string]string{},
		}
		mt, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || mt != "multipart/form-data" {
			c.ParseErr = err
			if err == nil {
				c.ParseErr = io.ErrUnexpectedEOF
			}
		} else {
			c.Boundary = params["boundary"]
			mr := multipart.NewReader(r.Body, c.Boundary)
			for {
				p, perr := mr.NextPart()
				if perr == io.EOF {
					break
				}
				if perr != nil {
					c.ParseErr = perr
					break
				}
				b, _ := io.ReadAll(p)
				c.Fields[p.FormName()] = string(b)
			}
		}
		f.mu.Lock()
		f.captures = append(f.captures, c)
		f.mu.Unlock()
		respond(w, r, c)
	}))
	t.Cleanup(upstream.Close)

	pool := testPool(t)
	for _, tok := range tokens {
		jwt := testJWT(map[string]any{"exp": time.Now().Add(2 * time.Hour).Unix(), "jti": tok})
		if _, err := pool.Create(accounts.Account{AccessToken: jwt, Status: "正常"}); err != nil {
			t.Fatal(err)
		}
	}
	cfg := config.Config{
		Environment:            config.EnvironmentDevelopment,
		SkipSSLVerify:          true,
		UpstreamTransport:      config.TransportGo,
		MaxAccountAttempts:     attempts,
		DeviceID:               "synthetic-device-id",
		SessionID:              "synthetic-session-id",
		DefaultUA:              "synthetic-ua/1.0",
		SecCHUA:                `"Synthetic";v="1"`,
		SecCHUAArch:            `"x86"`,
		SecCHUABitness:         `"64"`,
		SecCHUAFullVersion:     `"1.0.0.0"`,
		SecCHUAFullVersionList: `"Synthetic";v="1.0.0.0"`,
		SecCHUAMobile:          "?0",
		SecCHUAModel:           `""`,
		SecCHUAPlatform:        `"Windows"`,
		SecCHUAPlatformVersion: `"10.0.0"`,
		ClientVersion:          "prod-synthetic",
		ClientBuildNumber:      "1",
	}
	svc := New(cfg, pool, nil)
	svc.realtimeURL = upstream.URL + "/realtime/wm?dcid=0"
	f.svc, f.pool = svc, pool
	return f
}

func readTestdata(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func answerHandler(answer string, status int) func(http.ResponseWriter, *http.Request, wmCapture) {
	return func(w http.ResponseWriter, _ *http.Request, _ wmCapture) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, answer)
	}
}

func serviceError(t *testing.T, err error) *ServiceError {
	t.Helper()
	se, ok := err.(*ServiceError)
	if !ok {
		t.Fatalf("expected *ServiceError, got %T %v", err, err)
	}
	return se
}

func keys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func decodeJSONObject(t *testing.T, raw string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatalf("session field is not a JSON object: %v", err)
	}
	return m
}

func mLines(sdp string) []string {
	var out []string
	for _, l := range strings.Split(strings.ReplaceAll(sdp, "\r\n", "\n"), "\n") {
		if strings.HasPrefix(l, "m=") {
			out = append(out, l)
		}
	}
	return out
}

func TestWMEndpointConstant(t *testing.T) {
	if wmURL != "https://chatgpt.com/realtime/wm?dcid=0" {
		t.Fatalf("wmURL changed: %s", wmURL)
	}
}

func TestWMRequestShape(t *testing.T) {
	answer := readTestdata(t, "wm_answer.sdp")
	f := newWMFixture(t, []string{fixtureTokenA}, 1, answerHandler(answer, http.StatusCreated))

	offer := readTestdata(t, "wm_offer.sdp")
	res, err := f.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: offer, Voice: "cove"})
	if err != nil {
		t.Fatal(err)
	}
	c := f.only(t)

	if c.Method != http.MethodPost || c.Path != "/realtime/wm" || c.Query != "dcid=0" {
		t.Fatalf("unexpected request line: %s %s?%s", c.Method, c.Path, c.Query)
	}
	if c.ParseErr != nil {
		t.Fatalf("body is not valid multipart/form-data: %v", c.ParseErr)
	}
	if !strings.HasPrefix(c.Boundary, "----WebKitFormBoundary") {
		t.Fatalf("unexpected boundary prefix: %q", c.Boundary)
	}
	if len(c.Fields) != 2 {
		t.Fatalf("expected exactly fields sdp and session, got %v", keys(c.Fields))
	}
	for _, name := range []string{"sdp", "session"} {
		if _, ok := c.Fields[name]; !ok {
			t.Fatalf("missing multipart field %q", name)
		}
	}

	wantSDP := strings.ReplaceAll(strings.TrimSpace(offer), "\n", "\r\n") + "\r\n"
	if c.Fields["sdp"] != wantSDP {
		t.Fatalf("sdp field is not the CRLF-normalized offer:\n%q", c.Fields["sdp"])
	}
	if !strings.HasPrefix(c.Fields["sdp"], "v=0\r\n") {
		t.Fatal("sdp field must start with v=0")
	}

	for _, name := range wmRequiredHeaders {
		if c.Header.Get(name) == "" {
			t.Errorf("missing required header %s", name)
		}
	}
	if got := c.Header.Get("Authorization"); !strings.HasPrefix(got, "Bearer ") || len(got) <= len("Bearer ") {
		t.Errorf("Authorization is not a non-empty bearer value")
	}
	if got := c.Header.Get("Origin"); got != "https://chatgpt.com" {
		t.Errorf("Origin=%q", got)
	}
	if got := c.Header.Get("Referer"); got != "https://chatgpt.com/" {
		t.Errorf("Referer=%q", got)
	}
	if c.Header.Get("Oai-Device-Id") != "synthetic-device-id" {
		t.Errorf("oai-device-id should fall back to config device id when the account has none")
	}
	if c.Header.Get("Sec-Fetch-Site") != "same-origin" || c.Header.Get("Sec-Fetch-Mode") != "cors" {
		t.Errorf("unexpected sec-fetch-* values")
	}

	if res.AnswerSDP != answer || res.AccountID == 0 || res.VoiceSessionID == "" {
		t.Fatalf("unexpected result: %+v", res)
	}
}

var upperUUID = regexp.MustCompile(`^[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}$`)

func TestWMSessionJSONStructure(t *testing.T) {
	f := newWMFixture(t, []string{fixtureTokenA}, 1, answerHandler(readTestdata(t, "wm_answer.sdp"), http.StatusCreated))

	_, err := f.svc.CreateSession(CreateSessionRequest{
		Owner: "api_key:1", OfferSDP: readTestdata(t, "wm_offer.sdp"),
		Voice: "arbor", LanguageCode: "en",
	})
	if err != nil {
		t.Fatal(err)
	}
	c := f.only(t)
	m := decodeJSONObject(t, c.Fields["session"])

	wantStrings := map[string]string{
		"backend_reasoning_effort": "instant",
		"language_code":            "en",
		"requested_default_model":  "",
		"voice":                    "fathom", // alias "arbor" resolved
		"voice_mode":               "wingman",
		"model_slug":               "",
		"model_slug_advanced":      "",
		"timezone":                 "Etc/GMT-8",
	}
	for k, want := range wantStrings {
		if got, ok := m[k].(string); !ok || got != want {
			t.Errorf("session.%s=%v want %q", k, m[k], want)
		}
	}
	if v, _ := m["timezone_offset_min"].(float64); v != -480 {
		t.Errorf("timezone_offset_min=%v", m["timezone_offset_min"])
	}
	if v, ok := m["history_and_training_disabled"].(bool); !ok || v {
		t.Errorf("history_and_training_disabled=%v", m["history_and_training_disabled"])
	}
	if v, ok := m["enable_message_streaming"].(bool); !ok || !v {
		t.Errorf("enable_message_streaming=%v", m["enable_message_streaming"])
	}
	if tools, ok := m["client_tools"].([]any); !ok || len(tools) != 0 {
		t.Errorf("client_tools=%v", m["client_tools"])
	}
	if cm, ok := m["conversation_mode"].(map[string]any); !ok || cm["kind"] != "primary_assistant" {
		t.Errorf("conversation_mode=%v", m["conversation_mode"])
	}
	sid, _ := m["voice_session_id"].(string)
	if !upperUUID.MatchString(sid) {
		t.Errorf("voice_session_id must be an uppercase UUID, got %q", sid)
	}
	if m["voice_status_request_id"] != sid {
		t.Errorf("voice_status_request_id must equal voice_session_id")
	}
	for _, k := range []string{"conversation_id", "parent_message_id"} {
		if _, ok := m[k]; ok {
			t.Errorf("%s must be absent on a fresh session", k)
		}
	}
	if len(m) != 15 {
		t.Errorf("session has %d keys, expected 15 on a fresh session: %v", len(m), keys(m))
	}
}

func TestWMSessionJSONCarriesContinuityFields(t *testing.T) {
	f := newWMFixture(t, []string{fixtureTokenA}, 1, answerHandler(readTestdata(t, "wm_answer.sdp"), http.StatusCreated))

	_, err := f.svc.CreateSession(CreateSessionRequest{
		Owner: "api_key:1", OfferSDP: readTestdata(t, "wm_offer.sdp"),
		UpstreamConversationID:  "synthetic-conv",
		UpstreamParentMessageID: "synthetic-parent",
		UpstreamVoiceSessionID:  "0a1b2c3d-0000-4000-8000-00000000abcd",
	})
	if err != nil {
		t.Fatal(err)
	}
	m := decodeJSONObject(t, f.only(t).Fields["session"])
	if m["conversation_id"] != "synthetic-conv" || m["parent_message_id"] != "synthetic-parent" {
		t.Errorf("continuity fields missing: %v", m)
	}
	if m["voice_session_id"] != "0A1B2C3D-0000-4000-8000-00000000ABCD" {
		t.Errorf("voice_session_id not upper-cased: %v", m["voice_session_id"])
	}
}

func TestEncodeMultipartRoundTrip(t *testing.T) {
	body, ct := encodeMultipart([][2]string{{"sdp", "v=0\r\nline\r\n"}, {"session", `{"a":1}`}})
	_, params, err := mime.ParseMediaType(ct)
	if err != nil {
		t.Fatal(err)
	}
	mr := multipart.NewReader(strings.NewReader(string(body)), params["boundary"])
	got := map[string]string{}
	for {
		p, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(p)
		got[p.FormName()] = string(b)
		if p.FileName() != "" {
			t.Errorf("part %s must not carry a filename", p.FormName())
		}
	}
	if got["sdp"] != "v=0\r\nline\r\n" || got["session"] != `{"a":1}` {
		t.Fatalf("round trip mismatch: %q", got)
	}
}

func TestWMSuccessStatuses(t *testing.T) {
	answer := readTestdata(t, "wm_answer.sdp")
	for _, status := range []int{http.StatusCreated, http.StatusOK} {
		f := newWMFixture(t, []string{fixtureTokenA}, 1, answerHandler(answer, status))
		res, err := f.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: readTestdata(t, "wm_offer.sdp")})
		if err != nil {
			t.Fatalf("status %d: %v", status, err)
		}
		if res.AnswerSDP != answer {
			t.Fatalf("status %d: answer must be returned verbatim", status)
		}
		if res.UpstreamVoiceSessionID == "" || res.AccountLeaseID == "" {
			t.Fatalf("status %d: incomplete result %+v", status, res)
		}
	}
}

func TestWMAnswerHasAudioAndApplicationSections(t *testing.T) {
	answer := readTestdata(t, "wm_answer.sdp")
	f := newWMFixture(t, []string{fixtureTokenA}, 1, answerHandler(answer, http.StatusCreated))
	res, err := f.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: readTestdata(t, "wm_offer.sdp")})
	if err != nil {
		t.Fatal(err)
	}
	ms := mLines(res.AnswerSDP)
	if len(ms) != 2 {
		t.Fatalf("expected 2 m-lines, got %v", ms)
	}
	if !strings.HasPrefix(ms[0], "m=audio ") || !strings.Contains(ms[0], "UDP/TLS/RTP/SAVPF") {
		t.Errorf("audio m-line: %q", ms[0])
	}
	if !strings.HasPrefix(ms[1], "m=application ") || !strings.Contains(ms[1], "UDP/DTLS/SCTP") {
		t.Errorf("application m-line: %q", ms[1])
	}
	for _, l := range strings.Split(res.AnswerSDP, "\n") {
		if strings.HasPrefix(l, "a=candidate:") && !strings.Contains(l, "typ host") {
			t.Errorf("fixture answer should carry host candidates only: %q", l)
		}
	}
}

// The gateway only checks status and the "v=0" prefix; it does not inspect
// m-lines. This pins that current behavior so a relay change is deliberate.
func TestWMAnswerMLinesAreNotValidated(t *testing.T) {
	const bare = "v=0\r\no=- 1 1 IN IP4 192.0.2.1\r\ns=-\r\nt=0 0\r\n"
	f := newWMFixture(t, []string{fixtureTokenA}, 1, answerHandler("\r\n  "+bare, http.StatusCreated))
	res, err := f.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: readTestdata(t, "wm_offer.sdp")})
	if err != nil {
		t.Fatalf("answer without m-lines is currently accepted: %v", err)
	}
	if len(mLines(res.AnswerSDP)) != 0 {
		t.Fatal("expected no m-lines")
	}
	if !strings.HasPrefix(res.AnswerSDP, "\r\n") {
		t.Fatal("answer is returned untrimmed")
	}
}

func TestWM401MarksAccountInvalid(t *testing.T) {
	f := newWMFixture(t, []string{fixtureTokenA}, 1, func(w http.ResponseWriter, _ *http.Request, _ wmCapture) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"detail":"synthetic unauthorized"}`)
	})
	_, err := f.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: readTestdata(t, "wm_offer.sdp")})
	se := serviceError(t, err)
	if se.StatusCode != http.StatusUnauthorized || se.Message != "account token invalid" {
		t.Fatalf("unexpected error: %+v", se)
	}
	if _, _, perr := f.pool.Pick("", nil); perr == nil {
		t.Fatal("401 must take the only account out of rotation")
	}
}

func TestWM401RetriesNextAccount(t *testing.T) {
	var rejected string
	var mu sync.Mutex
	answer := readTestdata(t, "wm_answer.sdp")
	f := newWMFixture(t, []string{fixtureTokenA, fixtureTokenB}, 2, func(w http.ResponseWriter, r *http.Request, c wmCapture) {
		mu.Lock()
		first := rejected == ""
		if first {
			rejected = r.Header.Get("Authorization")
		}
		mu.Unlock()
		if first {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		answerHandler(answer, http.StatusCreated)(w, r, c)
	})
	res, err := f.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: readTestdata(t, "wm_offer.sdp")})
	if err != nil {
		t.Fatal(err)
	}
	reqs := f.requests()
	if len(reqs) != 2 || reqs[0].Header.Get("Authorization") == reqs[1].Header.Get("Authorization") {
		t.Fatalf("expected two requests with different accounts, got %d", len(reqs))
	}
	if reqs[0].Fields["session"] == reqs[1].Fields["session"] {
		t.Fatal("retry after 401 must rebuild session JSON with a fresh voice_session_id")
	}
	if res.AnswerSDP != answer {
		t.Fatal("expected answer from retry")
	}
}

func assertCoolingNotInvalid(t *testing.T, f *wmFixture, label string, minCooldownUntil int64) {
	t.Helper()
	acct, err := f.pool.Get(1)
	if err != nil {
		t.Fatalf("%s: %v", label, err)
	}
	if acct.Disabled || acct.Status == "invalid" {
		t.Fatalf("%s must not disable the account: %+v", label, acct)
	}
	if acct.CooldownUntil <= time.Now().Unix() || acct.CooldownUntil < minCooldownUntil {
		t.Fatalf("%s: expected cooldown, got until=%d", label, acct.CooldownUntil)
	}
}

func TestWM429CoolsDownAccount(t *testing.T) {
	f := newWMFixture(t, []string{fixtureTokenA}, 3, func(w http.ResponseWriter, _ *http.Request, _ wmCapture) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "37")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"detail":"synthetic rate limit"}`)
	})
	_, err := f.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: readTestdata(t, "wm_offer.sdp")})
	se := serviceError(t, err)
	// 429 is still an opaque 502 with no retry, but the account now cools down
	// for Retry-After instead of staying in rotation.
	if se.StatusCode != http.StatusBadGateway || se.Message != "realtime/wm failed status=429" {
		t.Fatalf("unexpected error: %+v", se)
	}
	if detail, _ := se.Detail.(string); !strings.Contains(detail, "synthetic rate limit") {
		t.Fatalf("detail should carry upstream body, got %v", se.Detail)
	}
	if n := len(f.requests()); n != 1 {
		t.Fatalf("429 must not be retried, got %d requests", n)
	}
	assertCoolingNotInvalid(t, f, "429", time.Now().Unix()+30)
}

func TestWM5xxIs502AndCoolsDownAccount(t *testing.T) {
	for _, status := range []int{http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable} {
		f := newWMFixture(t, []string{fixtureTokenA}, 2, func(w http.ResponseWriter, _ *http.Request, _ wmCapture) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = io.WriteString(w, `{"detail":"synthetic upstream failure"}`)
		})
		_, err := f.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: readTestdata(t, "wm_offer.sdp")})
		se := serviceError(t, err)
		if se.StatusCode != http.StatusBadGateway || !strings.HasPrefix(se.Message, "realtime/wm failed status=") {
			t.Fatalf("status %d: unexpected error %+v", status, se)
		}
		if n := len(f.requests()); n != 1 {
			t.Fatalf("status %d: 5xx must not be retried, got %d requests", status, n)
		}
		assertCoolingNotInvalid(t, f, fmt.Sprintf("status %d", status), 0)
	}
}

func TestWMCloudflareChallengeClassification(t *testing.T) {
	page := readTestdata(t, "wm_cf_challenge.html")
	for _, status := range []int{http.StatusForbidden, http.StatusServiceUnavailable, http.StatusOK} {
		f := newWMFixture(t, []string{fixtureTokenA}, 2, func(w http.ResponseWriter, _ *http.Request, _ wmCapture) {
			w.Header().Set("Content-Type", "text/html; charset=UTF-8")
			w.Header().Set("Cf-Mitigated", "challenge")
			w.WriteHeader(status)
			_, _ = io.WriteString(w, page)
		})
		_, err := f.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: readTestdata(t, "wm_offer.sdp")})
		se := serviceError(t, err)
		if se.StatusCode != http.StatusBadGateway || se.Message != "upstream blocked by Cloudflare or challenge page" {
			t.Fatalf("status %d: unexpected error %+v", status, se)
		}
		if n := len(f.requests()); n != 1 {
			t.Fatalf("status %d: challenge must not be retried, got %d requests", status, n)
		}
		assertCoolingNotInvalid(t, f, fmt.Sprintf("status %d challenge", status), 0)
	}
}

func TestWMNon401HTMLWithoutContentTypeStillClassifiedByBody(t *testing.T) {
	f := newWMFixture(t, []string{fixtureTokenA}, 1, func(w http.ResponseWriter, _ *http.Request, _ wmCapture) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, "<html><body>Just a moment...</body></html>")
	})
	_, err := f.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: readTestdata(t, "wm_offer.sdp")})
	if se := serviceError(t, err); se.Message != "upstream blocked by Cloudflare or challenge page" {
		t.Fatalf("unexpected error %+v", se)
	}
}

func TestWMSuccessStatusWithNonSDPBodyIsRejected(t *testing.T) {
	f := newWMFixture(t, []string{fixtureTokenA}, 1, answerHandler(`{"error":"synthetic"}`, http.StatusCreated))
	_, err := f.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: readTestdata(t, "wm_offer.sdp")})
	se := serviceError(t, err)
	if se.StatusCode != http.StatusBadGateway || se.Message != "realtime/wm failed status=201" {
		t.Fatalf("unexpected error %+v", se)
	}
}

func TestWMOfferMustStartWithVersionLine(t *testing.T) {
	f := newWMFixture(t, []string{fixtureTokenA}, 1, answerHandler("v=0\r\n", http.StatusCreated))
	_, err := f.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: "not sdp"})
	if se := serviceError(t, err); se.StatusCode != http.StatusBadRequest {
		t.Fatalf("unexpected error %+v", se)
	}
	if n := len(f.requests()); n != 0 {
		t.Fatalf("invalid offer must not reach upstream, got %d requests", n)
	}
}
