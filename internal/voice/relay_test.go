package voice

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/accounts"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/config"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/relay"
)

const (
	fakeUpstreamOffer = "v=0\r\no=- 1 1 IN IP4 192.0.2.1\r\ns=peer-b-offer\r\nt=0 0\r\n"
	fakeClientAnswer  = "v=0\r\no=- 2 2 IN IP4 192.0.2.2\r\ns=peer-a-answer\r\nt=0 0\r\n"
)

type fakeRelay struct {
	mu       sync.Mutex
	sessions []*fakeMedia
	routes   []relay.Route
	offers   []string
	newErr   error
	acceptFn func(*fakeMedia) error
}

func (r *fakeRelay) NewSession(_ context.Context, clientOffer string, route relay.Route) (relay.Session, string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.newErr != nil {
		return nil, "", r.newErr
	}
	m := &fakeMedia{id: "media-" + string(rune('a'+len(r.sessions))), relay: r}
	r.sessions = append(r.sessions, m)
	r.routes = append(r.routes, route)
	r.offers = append(r.offers, clientOffer)
	return m, fakeUpstreamOffer, nil
}

func (r *fakeRelay) session(i int) *fakeMedia {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.sessions[i]
}

func (r *fakeRelay) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.sessions)
}

type fakeMedia struct {
	id       string
	relay    *fakeRelay
	mu       sync.Mutex
	answers  []string
	reasons  []string
	closeErr error
	handler  func(string)
	closed   bool
	// termination, when set, replaces the default all-clean close record.
	termination *relay.Termination
}

func (m *fakeMedia) ID() string { return m.id }
func (m *fakeMedia) AcceptUpstreamAnswer(_ context.Context, answer string) (string, error) {
	m.mu.Lock()
	m.answers = append(m.answers, answer)
	m.mu.Unlock()
	if m.relay.acceptFn != nil {
		if err := m.relay.acceptFn(m); err != nil {
			return "", err
		}
	}
	return fakeClientAnswer, nil
}
func (m *fakeMedia) SetCloseHandler(h func(string)) {
	m.mu.Lock()
	m.handler = h
	m.mu.Unlock()
}
func (m *fakeMedia) Close() error { return m.CloseWithReason("closed") }
func (m *fakeMedia) CloseWithReason(reason string) error {
	m.mu.Lock()
	m.reasons = append(m.reasons, reason)
	first := !m.closed
	m.closed = true
	h := m.handler
	err := m.closeErr
	m.mu.Unlock()
	if first && h != nil {
		h(reason)
	}
	return err
}
func (m *fakeMedia) Termination() (relay.Termination, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		return relay.Termination{}, false
	}
	if m.termination != nil {
		t := *m.termination
		if t.Reason == "" && len(m.reasons) > 0 {
			t.Reason = m.reasons[0]
		}
		return t, true
	}
	reason := ""
	if len(m.reasons) > 0 {
		reason = m.reasons[0]
	}
	ok := relay.ComponentResult{Closed: true}
	return relay.Termination{SessionID: m.id, Reason: reason, PeerA: ok, PeerB: ok, DataChannels: ok, Tunnels: ok}, true
}
func (m *fakeMedia) Ready() <-chan struct{} { return make(chan struct{}) }

func (m *fakeMedia) closedReasons() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.reasons...)
}

func waitClosed(t *testing.T, m *fakeMedia) []string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if r := m.closedReasons(); len(r) > 0 {
			return r
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("media %s was not closed", m.id)
	return nil
}

func newRelayFixture(t *testing.T, tokens []string, attempts int, respond func(w http.ResponseWriter, r *http.Request, c wmCapture)) (*wmFixture, *fakeRelay) {
	t.Helper()
	f := newWMFixture(t, tokens, attempts, respond)
	fr := &fakeRelay{}
	f.svc.cfg.SessionTTLSeconds = 180
	f.svc.WithRelay(fr)
	return f, fr
}

func TestRelayUpstreamGetsPeerBOfferAndClientGetsPeerAAnswer(t *testing.T) {
	upstreamAnswer := readTestdata(t, "wm_answer.sdp")
	f, fr := newRelayFixture(t, []string{fixtureTokenA}, 1, answerHandler(upstreamAnswer, http.StatusCreated))
	clientOffer := readTestdata(t, "wm_offer.sdp")

	res, err := f.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: clientOffer})
	if err != nil {
		t.Fatal(err)
	}
	if res.AnswerSDP != fakeClientAnswer {
		t.Fatalf("client must receive peer A's answer, got %q", res.AnswerSDP)
	}
	if strings.Contains(res.AnswerSDP, "synthAns") {
		t.Fatal("upstream answer must not reach the client")
	}
	c := f.only(t)
	if !strings.Contains(c.Fields["sdp"], "peer-b-offer") || strings.Contains(c.Fields["sdp"], "synthOffer") {
		t.Fatalf("upstream must receive peer B's offer, got %q", c.Fields["sdp"])
	}
	if fr.count() != 1 {
		t.Fatalf("expected one media session, got %d", fr.count())
	}
	if !strings.Contains(fr.offers[0], "v=0") {
		t.Fatal("relay must receive the client offer")
	}
	media := fr.session(0)
	if len(media.answers) != 1 || media.answers[0] != upstreamAnswer {
		t.Fatalf("relay must receive the upstream answer verbatim, got %v", media.answers)
	}
	if got := media.closedReasons(); len(got) != 0 {
		t.Fatalf("live session must not be closed: %v", got)
	}
}

func TestRelayDisabledKeepsClientDirectBehavior(t *testing.T) {
	upstreamAnswer := readTestdata(t, "wm_answer.sdp")
	f := newWMFixture(t, []string{fixtureTokenA}, 1, answerHandler(upstreamAnswer, http.StatusCreated))
	offer := readTestdata(t, "wm_offer.sdp")
	res, err := f.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: offer})
	if err != nil {
		t.Fatal(err)
	}
	if res.AnswerSDP != upstreamAnswer {
		t.Fatal("without a relay the upstream answer is returned unchanged")
	}
}

func TestRelayReleaseSessionClosesMedia(t *testing.T) {
	f, fr := newRelayFixture(t, []string{fixtureTokenA}, 1, answerHandler(readTestdata(t, "wm_answer.sdp"), http.StatusCreated))
	res, err := f.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: readTestdata(t, "wm_offer.sdp")})
	if err != nil {
		t.Fatal(err)
	}
	if !f.svc.ReleaseSession("api_key:1", res.VoiceSessionID) {
		t.Fatal("release should report true")
	}
	if got := waitClosed(t, fr.session(0)); got[0] != "released" {
		t.Fatalf("unexpected close reason %v", got)
	}
	// A second release must not close again.
	f.svc.ReleaseSession("api_key:1", res.VoiceSessionID)
	if got := fr.session(0).closedReasons(); len(got) != 1 {
		t.Fatalf("repeated release closed media %d times", len(got))
	}
}

func TestRelayReleaseByOtherOwnerKeepsMedia(t *testing.T) {
	f, fr := newRelayFixture(t, []string{fixtureTokenA}, 1, answerHandler(readTestdata(t, "wm_answer.sdp"), http.StatusCreated))
	res, err := f.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: readTestdata(t, "wm_offer.sdp")})
	if err != nil {
		t.Fatal(err)
	}
	if f.svc.ReleaseSession("api_key:2", res.VoiceSessionID) {
		t.Fatal("other owner must not release")
	}
	if got := fr.session(0).closedReasons(); len(got) != 0 {
		t.Fatalf("media closed by non-owner: %v", got)
	}
}

func TestRelayReexchangeClosesSupersededMedia(t *testing.T) {
	f, fr := newRelayFixture(t, []string{fixtureTokenA}, 1, answerHandler(readTestdata(t, "wm_answer.sdp"), http.StatusCreated))
	offer := readTestdata(t, "wm_offer.sdp")
	first, err := f.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: offer})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: offer, VoiceSessionID: first.VoiceSessionID}); err != nil {
		t.Fatal(err)
	}
	if fr.count() != 2 {
		t.Fatalf("expected two media sessions, got %d", fr.count())
	}
	if got := waitClosed(t, fr.session(0)); got[0] != "superseded" {
		t.Fatalf("old media close reason %v", got)
	}
	if got := fr.session(1).closedReasons(); len(got) != 0 {
		t.Fatalf("new media must stay open: %v", got)
	}
}

func TestRelayIdleTTLDoesNotCutLiveMedia(t *testing.T) {
	f, fr := newRelayFixture(t, []string{fixtureTokenA}, 1, answerHandler(readTestdata(t, "wm_answer.sdp"), http.StatusCreated))
	f.svc.cfg.SessionTTLSeconds = 1
	res, err := f.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: readTestdata(t, "wm_offer.sdp")})
	if err != nil {
		t.Fatal(err)
	}
	f.svc.mu.Lock()
	f.svc.bindings[res.VoiceSessionID].UpdatedAt = time.Now().Add(-time.Hour)
	f.svc.mu.Unlock()
	if b, _ := f.svc.boundVoiceSession("api_key:1", res.VoiceSessionID); b == nil {
		t.Fatal("binding with live media must survive idle TTL")
	}
	if got := fr.session(0).closedReasons(); len(got) != 0 {
		t.Fatalf("live media was closed by TTL: %v", got)
	}
}

func TestRelayMediaEndDropsBindingAndMarksReleased(t *testing.T) {
	f, fr := newRelayFixture(t, []string{fixtureTokenA}, 1, answerHandler(readTestdata(t, "wm_answer.sdp"), http.StatusCreated))
	res, err := f.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: readTestdata(t, "wm_offer.sdp")})
	if err != nil {
		t.Fatal(err)
	}
	_ = fr.session(0).CloseWithReason("peer_a_failed") // client vanished
	if b, _ := f.svc.boundVoiceSession("api_key:1", res.VoiceSessionID); b != nil {
		t.Fatal("binding must be dropped when its media ends")
	}
}

func TestRelayStaleMediaEndKeepsNewerBinding(t *testing.T) {
	f, fr := newRelayFixture(t, []string{fixtureTokenA}, 1, answerHandler(readTestdata(t, "wm_answer.sdp"), http.StatusCreated))
	offer := readTestdata(t, "wm_offer.sdp")
	first, err := f.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: offer})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: offer, VoiceSessionID: first.VoiceSessionID}); err != nil {
		t.Fatal(err)
	}
	waitClosed(t, fr.session(0)) // superseded media closes; its handler must not drop the new binding
	time.Sleep(50 * time.Millisecond)
	if b, _ := f.svc.boundVoiceSession("api_key:1", first.VoiceSessionID); b == nil {
		t.Fatal("closing superseded media removed the newer binding")
	}
}

func TestRelayUpstreamFailuresCloseMedia(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		ct      string
		body    string
		wantMsg string
	}{
		{"429", http.StatusTooManyRequests, "application/json", `{"detail":"x"}`, "realtime/wm failed status=429"},
		{"5xx", http.StatusBadGateway, "application/json", `{"detail":"x"}`, "realtime/wm failed status=502"},
		{"challenge", http.StatusForbidden, "text/html", "<html>Just a moment...</html>", "upstream blocked by Cloudflare or challenge page"},
		{"non-sdp-200", http.StatusOK, "text/plain", "not an sdp", "realtime/wm failed status=200"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, fr := newRelayFixture(t, []string{fixtureTokenA}, 1, func(w http.ResponseWriter, _ *http.Request, _ wmCapture) {
				w.Header().Set("Content-Type", tc.ct)
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			})
			_, err := f.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: readTestdata(t, "wm_offer.sdp")})
			se := serviceError(t, err)
			if se.Message != tc.wantMsg {
				t.Fatalf("message %q want %q", se.Message, tc.wantMsg)
			}
			if got := waitClosed(t, fr.session(0)); got[0] != "upstream_rejected" {
				t.Fatalf("close reason %v", got)
			}
		})
	}
}

func TestRelay401RetriesOnAnotherAccountWithFreshMedia(t *testing.T) {
	answer := readTestdata(t, "wm_answer.sdp")
	var mu sync.Mutex
	rejected := ""
	f, fr := newRelayFixture(t, []string{fixtureTokenA, fixtureTokenB}, 2, func(w http.ResponseWriter, r *http.Request, c wmCapture) {
		auth := r.Header.Get("Authorization")
		mu.Lock()
		if rejected == "" {
			rejected = auth
		}
		reject := auth == rejected
		mu.Unlock()
		if reject {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"detail":"bad token"}`))
			return
		}
		answerHandler(answer, http.StatusCreated)(w, r, c)
	})
	res, err := f.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: readTestdata(t, "wm_offer.sdp")})
	if err != nil {
		t.Fatal(err)
	}
	if fr.count() != 2 {
		t.Fatalf("expected one media session per attempt, got %d", fr.count())
	}
	if got := waitClosed(t, fr.session(0)); got[0] != "upstream_unauthorized" {
		t.Fatalf("failed attempt's media close reason %v", got)
	}
	if got := fr.session(1).closedReasons(); len(got) != 0 {
		t.Fatalf("successful attempt's media must stay open: %v", got)
	}
	if res.AnswerSDP != fakeClientAnswer {
		t.Fatal("client must get peer A's answer from the retry")
	}
}

func TestRelayAcceptAnswerFailureClosesMediaAndReturns502(t *testing.T) {
	f, fr := newRelayFixture(t, []string{fixtureTokenA}, 1, answerHandler(readTestdata(t, "wm_answer.sdp"), http.StatusCreated))
	fr.acceptFn = func(*fakeMedia) error { return errors.New("synthetic negotiation failure") }
	_, err := f.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: readTestdata(t, "wm_offer.sdp")})
	se := serviceError(t, err)
	if se.StatusCode != http.StatusBadGateway || se.Message != "media relay negotiation failed" {
		t.Fatalf("unexpected error %+v", se)
	}
	if got := waitClosed(t, fr.session(0)); got[0] != "accept_answer_failed" {
		t.Fatalf("close reason %v", got)
	}
	f.svc.mu.Lock()
	n := len(f.svc.bindings)
	f.svc.mu.Unlock()
	if n != 0 {
		t.Fatalf("failed negotiation must not leave a binding, got %d", n)
	}
}

func TestRelayCapacityMapsTo503AndNeverCallsUpstream(t *testing.T) {
	f, fr := newRelayFixture(t, []string{fixtureTokenA}, 1, answerHandler(readTestdata(t, "wm_answer.sdp"), http.StatusCreated))
	fr.newErr = relay.ErrCapacity
	_, err := f.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: readTestdata(t, "wm_offer.sdp")})
	se := serviceError(t, err)
	if se.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("capacity must be 503, got %+v", se)
	}
	if n := len(f.requests()); n != 0 {
		t.Fatalf("upstream must not be called when relay is full, got %d requests", n)
	}
}

func TestRelayCreateFailureMapsTo502(t *testing.T) {
	f, fr := newRelayFixture(t, []string{fixtureTokenA}, 1, answerHandler(readTestdata(t, "wm_answer.sdp"), http.StatusCreated))
	fr.newErr = errors.New("synthetic relay failure")
	_, err := f.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: readTestdata(t, "wm_offer.sdp")})
	if se := serviceError(t, err); se.StatusCode != http.StatusBadGateway || se.Message != "media relay unavailable" {
		t.Fatalf("unexpected error %+v", se)
	}
}

func TestRelayMediaRouteSwitch(t *testing.T) {
	cases := []struct {
		name      string
		route     string
		proxy     string
		wantProxy string
	}{
		{"default uses account proxy", "", "socks5://u:p@203.0.113.1:1080", "socks5://u:p@203.0.113.1:1080"},
		{"account uses account proxy", config.RelayMediaRouteAccount, "socks5://u:p@203.0.113.1:1080", "socks5://u:p@203.0.113.1:1080"},
		{"direct bypasses account proxy", config.RelayMediaRouteDirect, "socks5://u:p@203.0.113.1:1080", ""},
		{"account with no proxy stays direct", config.RelayMediaRouteAccount, "", ""},
		{"account marked direct stays direct", config.RelayMediaRouteAccount, "direct", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc := New(config.Config{RelayMediaRoute: tc.route}, nil, nil)
			got := svc.relayRoute(accounts.Account{Proxy: tc.proxy})
			if got.ProxyURL != tc.wantProxy {
				t.Fatalf("ProxyURL=%q want %q", got.ProxyURL, tc.wantProxy)
			}
		})
	}
}

func TestRelayMediaRouteDoesNotChangeSignalingProxy(t *testing.T) {
	// Signaling always follows the account proxy; the switch only affects media.
	// The fixture uses the "go" transport with an unusable proxy, so a signaling
	// request that honored the proxy would fail rather than reach the fixture.
	f, fr := newRelayFixture(t, []string{fixtureTokenA}, 1, answerHandler(readTestdata(t, "wm_answer.sdp"), http.StatusCreated))
	f.svc.cfg.RelayMediaRoute = config.RelayMediaRouteDirect
	list, err := f.pool.List()
	if err != nil || len(list) != 1 {
		t.Fatalf("setup: %v %d", err, len(list))
	}
	if _, err := f.svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: readTestdata(t, "wm_offer.sdp")}); err != nil {
		t.Fatal(err)
	}
	if fr.routes[0].ProxyURL != "" {
		t.Fatalf("direct media route must pass no proxy, got %q", fr.routes[0].ProxyURL)
	}
}
