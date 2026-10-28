package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestBridgeBasicAuthentication(t *testing.T) {
	manager := NewWithLimits("synthetic-user", "synthetic-password", time.Hour, Limits{MaxFailures: 100}, nil)
	called := false
	handler := manager.RequireBasic(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true; w.WriteHeader(204) }))
	for _, password := range []string{"", "wrong", "synthetic-password"} {
		called = false
		r := httptest.NewRequest("GET", "/bridge/v1/capabilities", nil)
		if password != "" {
			r.SetBasicAuth("synthetic-user", password)
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if password == "synthetic-password" {
			if w.Code != 204 || !called {
				t.Fatalf("valid Basic rejected: %d", w.Code)
			}
		} else if w.Code != 401 || called {
			t.Fatalf("invalid Basic accepted: %d", w.Code)
		}
	}
	// A valid browser session never authorizes the bridge credential surface.
	manager.sessions["synthetic-cookie"] = session{Username: "synthetic-user", ExpiresAt: time.Now().Add(time.Hour)}
	r := httptest.NewRequest("GET", "/bridge/v1/capabilities", nil)
	r.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "synthetic-cookie"})
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatalf("cookie accepted: %d", w.Code)
	}
}
