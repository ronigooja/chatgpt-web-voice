package voice

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/accounts"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/config"
)

type deviceAccountRepository struct {
	account accounts.Account
	missing bool
}

func (p *deviceAccountRepository) Get(id int64) (accounts.Account, error) {
	if p.missing {
		return accounts.Account{}, errors.New("missing")
	}
	return p.account, nil
}
func (p *deviceAccountRepository) Pick(token string, excluded map[string]struct{}) (string, accounts.Account, error) {
	if p.missing {
		return "", accounts.Account{}, errors.New("missing")
	}
	return p.account.AccessToken, p.account, nil
}
func (p *deviceAccountRepository) PickByID(id int64, excluded map[string]struct{}) (string, accounts.Account, error) {
	return p.Pick("", excluded)
}
func (p *deviceAccountRepository) MarkInvalid(token string) error { return nil }

func TestAccountDeviceUsedByEveryVoiceRequest(t *testing.T) {
	repo := &deviceAccountRepository{account: accounts.Account{ID: 1, AccessToken: "synthetic-token", Proxy: "direct", DeviceID: "device-one"}}
	expectedDevice, expectedToken := "device-one", "synthetic-token"
	seen := map[string]int{}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Oai-Device-Id"); got != expectedDevice {
			t.Errorf("%s device=%q want %q", r.URL.Path, got, expectedDevice)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer "+expectedToken {
			t.Errorf("%s credential mismatch", r.URL.Path)
		}
		seen[r.URL.Path]++
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/wm":
			w.Header().Set("Content-Type", "application/sdp")
			_, _ = w.Write([]byte("v=0\r\n"))
		case "/files":
			_, _ = w.Write([]byte(`{"file_id":"file-test","upload_url":"https://synthetic.invalid/blob"}`))
		case "/files/file-test/uploaded":
			w.WriteHeader(http.StatusNotFound)
		case "/conversation/conv-test":
			_, _ = w.Write([]byte(`{"title":"synthetic title"}`))
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	defer upstream.Close()
	svc := New(config.Config{DeviceID: "global-fallback", UpstreamTransport: "go", SessionTTLSeconds: 3600, MaxAccountAttempts: 1}, repo, nil)
	svc.realtimeURL = upstream.URL + "/wm"
	svc.settingsUserURL = upstream.URL + "/settings"
	svc.filesAPIURL = upstream.URL + "/files"
	svc.conversationURLPrefix = upstream.URL + "/conversation"
	result, err := svc.CreateSession(CreateSessionRequest{Owner: "test", OfferSDP: "v=0\r\n"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = svc.ProbeAccountToken(1); err != nil {
		t.Fatal(err)
	}
	// A live binding must pick up edited credentials and device/proxy policy.
	repo.account.DeviceID = "device-two"
	repo.account.AccessToken = "synthetic-new-token"
	expectedDevice, expectedToken = repo.account.DeviceID, repo.account.AccessToken
	if _, err = svc.CreateImageUploadCredential("test", result.VoiceSessionID, ImageUploadRequest{FileName: "test.png", FileSize: 1, MimeType: "image/png"}); err != nil {
		t.Fatal(err)
	}
	if _, err = svc.CompleteImageUpload("test", result.VoiceSessionID, "file-test"); err != nil {
		t.Fatal(err)
	}
	if _, err = svc.FetchUpstreamTitle("test", result.VoiceSessionID, "conv-test"); err != nil {
		t.Fatal(err)
	}
	if _, err = svc.CreateSession(CreateSessionRequest{Owner: "test", OfferSDP: "v=0\r\n", VoiceSessionID: result.VoiceSessionID}); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/wm", "/settings", "/files", "/files/file-test/uploaded", "/files/process_upload_stream", "/conversation/conv-test"} {
		if seen[path] == 0 {
			t.Errorf("missing request %s", path)
		}
	}
	repo.missing = true
	if _, err = svc.CreateImageUploadCredential("test", result.VoiceSessionID, ImageUploadRequest{FileName: "test.png", FileSize: 1, MimeType: "image/png"}); err == nil {
		t.Fatal("deleted account retained stale credentials")
	}
	if got := svc.authHeaders("synthetic", "", nil).Get("Oai-Device-Id"); got != "global-fallback" {
		t.Fatalf("native fallback=%q", got)
	}
}

type reassignedTokenRepository struct {
	deviceAccountRepository
	wrong accounts.Account
}

func (p *reassignedTokenRepository) Pick(token string, excluded map[string]struct{}) (string, accounts.Account, error) {
	if token == p.wrong.AccessToken {
		return token, p.wrong, nil
	}
	return p.deviceAccountRepository.Pick(token, excluded)
}

func TestSessionResumePrefersStableAccountAfterTokenReassignment(t *testing.T) {
	repo := &reassignedTokenRepository{
		deviceAccountRepository: deviceAccountRepository{account: accounts.Account{ID: 1, AccessToken: "rotated-token", DeviceID: "original-device", Proxy: "direct"}},
		wrong:                   accounts.Account{ID: 2, AccessToken: "old-token", DeviceID: "other-device", Proxy: "http://127.0.0.1:1"},
	}
	requests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Header.Get("Authorization") != "Bearer rotated-token" || r.Header.Get("Oai-Device-Id") != "original-device" {
			t.Error("session used reassigned token owner identity")
		}
		w.Header().Set("Content-Type", "application/sdp")
		_, _ = w.Write([]byte("v=0\r\n"))
	}))
	defer upstream.Close()
	svc := New(config.Config{UpstreamTransport: "go", SessionTTLSeconds: 3600, MaxAccountAttempts: 1}, repo, nil)
	svc.realtimeURL = upstream.URL
	sessionID := svc.bindVoiceSession("test", "", "old-token", accounts.Account{ID: 1, DeviceID: "original-device", Proxy: "direct"}, UpstreamContext{})
	result, err := svc.CreateSession(CreateSessionRequest{Owner: "test", VoiceSessionID: sessionID, OfferSDP: "v=0\r\n"})
	if err != nil {
		t.Fatal(err)
	}
	if result.AccountID != 1 || requests != 1 {
		t.Fatal("session changed stable account")
	}
	repo.missing = true
	if _, err := svc.CreateSession(CreateSessionRequest{Owner: "test", VoiceSessionID: sessionID, OfferSDP: "v=0\r\n"}); err == nil {
		t.Fatal("deleted sticky account fell back to reassigned token")
	}
	if requests != 1 {
		t.Fatal("deleted account sent an upstream request")
	}
}
