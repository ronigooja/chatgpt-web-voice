package api

import (
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/voice"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type resultHTTPStub struct {
	downstreamVoiceStub
	posts int
	gets  int
}

func (s *resultHTTPStub) ResultDeliveryCapability() bool { return true }
func (s *resultHTTPStub) SubmitResultDelivery(owner, session string, p voice.ResultDeliveryRequest) (voice.ResultReceipt, error) {
	if err := voice.ValidateResultDelivery(p); err != nil {
		return voice.ResultReceipt{}, err
	}
	s.posts++
	return voice.ResultReceipt{MediaGeneration: p.MediaGeneration, DeliveryID: p.DeliveryID, ContentHash: p.ContentHash, State: "applied"}, nil
}
func (s *resultHTTPStub) LookupResultDelivery(owner, session string, g int64, id string) (voice.ResultReceipt, error) {
	s.gets++
	return voice.ResultReceipt{MediaGeneration: g, DeliveryID: id, ContentHash: "hash", State: "unknown"}, nil
}
func TestResultDeliveryHTTP(t *testing.T) {
	stub := &resultHTTPStub{}
	handler, key, _ := newDownstreamTestHandler(t, stub)
	path := "/v1/voice/sessions/s/result-deliveries"
	if r := performDownstreamRequest(t, handler, http.MethodPost, path, "", nil); r.Code != 401 {
		t.Fatal(r.Code)
	}
	valid := `{"media_generation":1,"delivery_id":"d","content_hash":"2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824","text":"hello"}`
	for _, raw := range []string{valid, valid + ` {}`, strings.Replace(valid, `"text":"hello"`, `"text":"hello","extra":true`, 1)} {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(raw))
		req.Header.Set("Authorization", "Bearer "+key)
		r := httptest.NewRecorder()
		handler.ServeHTTP(r, req)
		if raw == valid {
			if r.Code != 200 || !strings.Contains(r.Body.String(), `"state":"applied"`) {
				t.Fatal(r.Code, r.Body.String())
			}
		} else if r.Code != 400 {
			t.Fatal(r.Code)
		}
	}
	if stub.posts != 1 {
		t.Fatal(stub.posts)
	}
	r := performDownstreamRequest(t, handler, http.MethodGet, path+"/d?media_generation=1", key, nil)
	if r.Code != 200 || stub.gets != 1 || stub.posts != 1 {
		t.Fatal(r.Code, stub)
	}
	health := performDownstreamRequest(t, handler, http.MethodGet, "/v1/health", key, nil)
	if !strings.Contains(health.Body.String(), `"applied_evidence":"local_transport_writes_and_durable_receipt"`) {
		t.Fatal(health.Body.String())
	}
}

func TestResultDeliveryStrictJSON(t *testing.T) {
	for _, raw := range []string{`{"text":"\ud800"}`, `{"text":"\udc00"}`, `{"text":"a","text":"b"}`, "{\"text\":\"\xff\"}"} {
		if validResultJSON([]byte(raw)) {
			t.Fatalf("accepted %q", raw)
		}
	}
	for _, raw := range []string{`{"text":"hello"}`, `{"text":"\ud83d\ude00"}`, `{"text":"\\ud800"}`} {
		if !validResultJSON([]byte(raw)) {
			t.Fatalf("rejected %q", raw)
		}
	}
}
