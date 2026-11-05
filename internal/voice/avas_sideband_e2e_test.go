package voice

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	http "github.com/bogdanfinn/fhttp"
	"github.com/bogdanfinn/fhttp/httptest"
	"github.com/bogdanfinn/websocket"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/accounts"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/callsessions"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/config"
)

type avasUpstream struct {
	mu      sync.Mutex
	frames  []map[string]any
	conn    *websocket.Conn
	ready   chan struct{}
	release chan struct{}
}

func (u *avasUpstream) received() []map[string]any {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]map[string]any(nil), u.frames...)
}

func (u *avasUpstream) waitFrames(t *testing.T, n int) []map[string]any {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if got := u.received(); len(got) >= n {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("sideband received %d frames, want %d", len(u.received()), n)
	return nil
}

func (u *avasUpstream) push(t *testing.T, event any) {
	t.Helper()
	<-u.ready
	b, _ := json.Marshal(event)
	u.mu.Lock()
	defer u.mu.Unlock()
	if err := u.conn.WriteMessage(websocket.TextMessage, b); err != nil {
		t.Fatal(err)
	}
}

type avasRig struct {
	svc      *Service
	media    *fakeMedia
	upstream *avasUpstream
	id       string
}

func newAVASRig(t *testing.T, configure ...func(*Service)) *avasRig {
	t.Helper()
	up := &avasUpstream{ready: make(chan struct{}), release: make(chan struct{})}
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	var once sync.Once
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "https://api.openai.com/v1/live/call-1")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("v=0\r\n"))
	}))
	t.Cleanup(server.Close)
	sideband := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		up.mu.Lock()
		up.conn = conn
		up.mu.Unlock()
		once.Do(func() { close(up.ready) })
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var v map[string]any
			if json.Unmarshal(data, &v) == nil {
				up.mu.Lock()
				up.frames = append(up.frames, v)
				up.mu.Unlock()
			}
		}
	}))
	t.Cleanup(sideband.Close)

	pool := testPool(t)
	jwt := testJWT(map[string]any{"exp": time.Now().Add(2 * time.Hour).Unix(), "jti": "avas-e2e"})
	if _, err := pool.Create(accounts.Account{AccessToken: jwt, Status: "正常"}); err != nil {
		t.Fatal(err)
	}
	svc := New(config.Config{
		Environment:        config.EnvironmentDevelopment,
		SkipSSLVerify:      true,
		UpstreamTransport:  config.TransportGo,
		MaxAccountAttempts: 1,
		DeviceID:           "d",
		SessionID:          "s",
		DefaultUA:          "ua",
		SessionTTLSeconds:  180,
		VoiceUpstreamRoute: "avas",
		ZoeFinalTurn:       true,
	}, pool, nil)
	svc.avasURL = server.URL + "/calls"
	u, _ := url.Parse(sideband.URL)
	svc.avasSidebandURL = "ws://" + u.Host + "/live/"
	fr := &fakeRelay{}
	svc.WithRelay(fr)
	svc.WithCallSessions(&testCallSessionStore{rows: make(map[string]callsessions.Session)})
	for _, apply := range configure {
		apply(svc)
	}
	res, err := svc.CreateSession(CreateSessionRequest{Owner: "api_key:1", OfferSDP: readTestdata(t, "wm_offer.sdp")})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-up.ready:
	case <-time.After(5 * time.Second):
		t.Fatal("sideband never connected")
	}
	return &avasRig{svc: svc, media: fr.session(0), upstream: up, id: res.VoiceSessionID}
}

func (r *avasRig) delegate(t *testing.T, id, text string) {
	t.Helper()
	r.upstream.push(t, map[string]any{"type": "delegation.created", "item": map[string]any{"type": "delegation", "id": id, "target": "client", "content": []map[string]string{{"type": "input_text", "text": text}}}})
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		r.svc.mu.Lock()
		got := r.svc.bindings[r.id].openDelegation
		r.svc.mu.Unlock()
		if got == id {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("delegation %q never became open", id)
}

func contentText(t *testing.T, frame map[string]any) string {
	t.Helper()
	parts, _ := frame["content"].([]any)
	if len(parts) != 1 {
		t.Fatalf("frame content=%v", frame["content"])
	}
	return parts[0].(map[string]any)["text"].(string)
}

func TestAVASContextGoesToSidebandNotDataChannel(t *testing.T) {
	r := newAVASRig(t)
	r.delegate(t, "deleg-1", "research the weather")
	if _, err := r.svc.UpdateSessionContext("api_key:1", r.id, UpstreamContext{Text: "Tokyo is rainy"}); err != nil {
		t.Fatal(err)
	}
	f := r.upstream.waitFrames(t, 1)[0]
	if f["type"] != "delegation.context.append" || f["delegation_item_id"] != "deleg-1" || f["channel"] != "speakable" || contentText(t, f) != "Tokyo is rainy" {
		t.Fatalf("sideband frame=%v", f)
	}
	if len(r.media.upstreamEvents) != 0 {
		t.Fatalf("data channel received %v, answers must only use the sideband", r.media.upstreamEvents)
	}
}

func TestAVASSidebandDelegationEmitsFinalTurnDownstreamAndOpensContext(t *testing.T) {
	r := newAVASRig(t)
	r.delegate(t, "deleg-weather", "research the weather")
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		r.media.mu.Lock()
		injected := append([]string(nil), r.media.injected...)
		r.media.mu.Unlock()
		for _, raw := range injected {
			var event map[string]any
			if json.Unmarshal([]byte(raw), &event) == nil && event["type"] == "user_audio_turn.final" {
				if event["transcript"] != "research the weather" || event["turn_id"] != "deleg-weather" {
					t.Fatalf("final event=%v", event)
				}
				if _, err := r.svc.UpdateSessionContext("api_key:1", r.id, UpstreamContext{Text: "Tokyo is rainy"}); err != nil {
					t.Fatal(err)
				}
				frame := r.upstream.waitFrames(t, 1)[0]
				if frame["type"] != "delegation.context.append" || frame["delegation_item_id"] != "deleg-weather" {
					t.Fatalf("sideband context frame=%v", frame)
				}
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("user_audio_turn.final was not injected downstream")
}

func TestAVASSidebandMapperInjectionDoesNotReenterObserver(t *testing.T) {
	r := newAVASRig(t)
	r.delegate(t, "deleg-one", "research the weather")
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		r.media.mu.Lock()
		observed := append([]string(nil), r.media.observed...)
		injected := append([]string(nil), r.media.injected...)
		r.media.mu.Unlock()
		if len(injected) >= 2 {
			if len(observed) != 1 {
				t.Fatalf("observer saw %d events: %v", len(observed), observed)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("mapper did not inject final turn")
}

func TestAVASContextWithoutDelegationIsSessionAppend(t *testing.T) {
	r := newAVASRig(t)
	if _, err := r.svc.UpdateSessionContext("api_key:1", r.id, UpstreamContext{Text: "note"}); err != nil {
		t.Fatal(err)
	}
	f := r.upstream.waitFrames(t, 1)[0]
	if f["type"] != "session.context.append" || f["delegation_item_id"] != nil || f["channel"] != "speakable" {
		t.Fatalf("frame=%v", f)
	}
}

func TestAVASContextUsesNewestDelegationAndDropsOldOnUserTurn(t *testing.T) {
	r := newAVASRig(t)
	r.delegate(t, "old", "one")
	r.delegate(t, "new", "two")
	if _, err := r.svc.UpdateSessionContext("api_key:1", r.id, UpstreamContext{Text: "a"}); err != nil {
		t.Fatal(err)
	}
	r.upstream.push(t, map[string]any{"type": "turn.created", "turn": map[string]any{"role": "user"}})
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		r.svc.mu.Lock()
		open := r.svc.bindings[r.id].openDelegation
		r.svc.mu.Unlock()
		if open == "" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := r.svc.UpdateSessionContext("api_key:1", r.id, UpstreamContext{Text: "b"}); err != nil {
		t.Fatal(err)
	}
	frames := r.upstream.waitFrames(t, 2)
	if frames[0]["delegation_item_id"] != "new" || frames[0]["type"] != "delegation.context.append" {
		t.Fatalf("first=%v", frames[0])
	}
	if frames[1]["type"] != "session.context.append" || frames[1]["delegation_item_id"] != nil {
		t.Fatalf("after a new user turn the old delegation must not be answered: %v", frames[1])
	}
}

func TestAVASSidebandUserTurnClearsDelegationThroughServiceObserver(t *testing.T) {
	r := newAVASRig(t)
	r.delegate(t, "deleg-clear", "research the weather")
	r.upstream.push(t, map[string]any{"type": "turn.created", "turn": map[string]any{"role": "user"}})
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		r.svc.mu.Lock()
		open := r.svc.bindings[r.id].openDelegation
		r.svc.mu.Unlock()
		if open == "" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("sideband user turn did not clear open delegation")
}

func TestAVASContextLongMultibyteTextArrivesAsOrderedChunks(t *testing.T) {
	r := newAVASRig(t)
	r.delegate(t, "d", "x")
	text := strings.Repeat("东京天气", 400)
	if _, err := r.svc.UpdateSessionContext("api_key:1", r.id, UpstreamContext{Text: text}); err != nil {
		t.Fatal(err)
	}
	want := (len(text) + 499) / 500
	frames := r.upstream.waitFrames(t, want)
	var got strings.Builder
	for _, f := range frames {
		part := contentText(t, f)
		if len(part) > 500 {
			t.Fatalf("chunk of %d bytes", len(part))
		}
		got.WriteString(part)
	}
	if got.String() != text {
		t.Fatal("chunks do not reassemble to the original text in order")
	}
}

func TestAVASContextAfterSidebandClosedIsBadGatewayAndClearsDelegation(t *testing.T) {
	r := newAVASRig(t)
	r.delegate(t, "d", "x")
	r.upstream.mu.Lock()
	_ = r.upstream.conn.Close()
	r.upstream.mu.Unlock()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		r.svc.mu.Lock()
		open := r.svc.bindings[r.id].openDelegation
		r.svc.mu.Unlock()
		if open == "" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	_, err := r.svc.UpdateSessionContext("api_key:1", r.id, UpstreamContext{Text: "late"})
	if se := serviceError(t, err); se.StatusCode != 502 {
		t.Fatalf("status=%d", se.StatusCode)
	}
	r.svc.mu.Lock()
	open := r.svc.bindings[r.id].openDelegation
	r.svc.mu.Unlock()
	if open != "" {
		t.Fatalf("delegation %q stayed open after the sideband closed", open)
	}
}

func TestAVASContextRejectsWrongOwnerOversizeAndInvalidUTF8WithoutSending(t *testing.T) {
	r := newAVASRig(t)
	if _, err := r.svc.UpdateSessionContext("api_key:2", r.id, UpstreamContext{Text: "x"}); serviceError(t, err).StatusCode != 403 {
		t.Fatal("wrong owner must be 403")
	}
	if _, err := r.svc.UpdateSessionContext("api_key:1", r.id, UpstreamContext{Text: strings.Repeat("a", avasContextMaxBytes+1)}); serviceError(t, err).StatusCode != 413 {
		t.Fatal("oversize must be 413")
	}
	if _, err := r.svc.UpdateSessionContext("api_key:1", r.id, UpstreamContext{Text: "bad\xff"}); serviceError(t, err).StatusCode != 400 {
		t.Fatal("invalid UTF-8 must be 400")
	}
	if _, err := r.svc.UpdateSessionContext("api_key:1", r.id, UpstreamContext{Text: "  \n "}); err != nil {
		t.Fatalf("whitespace-only must be a no-op: %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	if got := r.upstream.received(); len(got) != 0 {
		t.Fatalf("rejected or blank requests reached the sideband: %v", got)
	}
}

func TestAVASContextWriteDoesNotHoldServiceMutex(t *testing.T) {
	r := newAVASRig(t)
	r.delegate(t, "d", "x")
	r.svc.mu.Lock()
	b := r.svc.bindings[r.id]
	w := b.avasSideband
	r.svc.mu.Unlock()

	// Hold the writer's own mutex so the context write blocks inside send().
	w.mu.Lock()
	done := make(chan error, 1)
	go func() {
		_, err := r.svc.UpdateSessionContext("api_key:1", r.id, UpstreamContext{Text: "blocked"})
		done <- err
	}()
	time.Sleep(100 * time.Millisecond)
	locked := make(chan struct{})
	go func() {
		r.svc.mu.Lock()
		r.svc.mu.Unlock()
		close(locked)
	}()
	select {
	case <-locked:
	case <-time.After(2 * time.Second):
		w.mu.Unlock()
		t.Fatal("service mutex was held while the sideband write was blocked")
	}
	w.mu.Unlock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("blocked write never completed")
	}
	if len(r.upstream.waitFrames(t, 1)) != 1 {
		t.Fatal("blocked frame was not delivered")
	}
}

func TestAVASContextConcurrentWritesAreWholeFrames(t *testing.T) {
	r := newAVASRig(t)
	const n = 20
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := r.svc.UpdateSessionContext("api_key:1", r.id, UpstreamContext{Text: fmt.Sprintf("message-%02d", i)}); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	seen := map[string]bool{}
	for _, f := range r.upstream.waitFrames(t, n) {
		seen[contentText(t, f)] = true
	}
	for i := 0; i < n; i++ {
		if !seen[fmt.Sprintf("message-%02d", i)] {
			t.Fatalf("message-%02d lost or corrupted", i)
		}
	}
}

func TestWMRouteIgnoresContextTextAndSendsNothingUpstream(t *testing.T) {
	r := newAVASRig(t)
	r.svc.cfg.VoiceUpstreamRoute = "wm"
	if _, err := r.svc.UpdateSessionContext("api_key:1", r.id, UpstreamContext{Text: "hello"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	if len(r.upstream.received()) != 0 || len(r.media.upstreamEvents) != 0 {
		t.Fatal("wm route must not forward context text")
	}
}
