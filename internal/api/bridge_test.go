package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/accounts"
)

func TestBridgeHTTPContract(t *testing.T) {
	pool, _ := newAPITestServer(t)
	mux := http.NewServeMux()
	New(Dependencies{Accounts: pool}).RegisterBridge(mux)
	response := performJSONRequest(t, mux, "GET", "/bridge/v1/capabilities", nil)
	if response.Code != 200 || !strings.Contains(response.Body.String(), `"explicit_route":true`) {
		t.Fatal("capability contract unavailable")
	}
	response = performJSONRequest(t, mux, "GET", "/bridge/v1/accounts/missing", nil)
	if response.Code != 404 {
		t.Fatal("missing record not 404")
	}
	payload := accounts.BridgePayload{Identity: "synthetic@example.test", CredentialType: "chatgpt_web", Credential: accounts.BridgeCredential{AccessToken: "synthetic-token"}, Route: accounts.BridgeRoute{Type: "direct"}}
	raw, _ := json.Marshal(payload)
	put := func(body []byte, key string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("PUT", "/bridge/v1/accounts/bridge-http", bytes.NewReader(body))
		r.Header.Set("Idempotency-Key", key)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		return w
	}
	response = put(raw, "key")
	if response.Code != 200 {
		t.Fatalf("PUT failed: %d %s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "synthetic-token") {
		t.Fatal("credential exposed")
	}
	response = put(raw, "key")
	if response.Code != 200 {
		t.Fatal("repeated write failed")
	}
	payload.Credential.AccessToken = "changed-token"
	changed, _ := json.Marshal(payload)
	response = put(changed, "key")
	if response.Code != 409 {
		t.Fatal("changed idempotency request accepted")
	}
	read := performJSONRequest(t, mux, "GET", "/bridge/v1/accounts/bridge-http", nil)
	if read.Code != 200 {
		t.Fatal("readback failed")
	}
	var view accounts.BridgeView
	if err := json.Unmarshal(read.Body.Bytes(), &view); err != nil || view.Route.Type != "direct" {
		t.Fatal("readback mismatch")
	}
	expected, _ := accounts.BridgeDigest(accounts.BridgeCredential{AccessToken: "synthetic-token"})
	if view.CredentialDigest != expected {
		t.Fatal("readback does not match stored credential")
	}
	for _, bad := range [][]byte{[]byte("{"), []byte(`{"refresh_token":"forbidden"}`), []byte(`{} {}`), bytes.Repeat([]byte(" "), (1<<20)+1)} {
		if got := put(bad, "bad"); got.Code != 400 {
			t.Fatalf("invalid body status=%d", got.Code)
		}
	}
	if got := put(raw, ""); got.Code != 400 {
		t.Fatal("missing idempotency key accepted")
	}
}
