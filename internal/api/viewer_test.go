package api

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/voice/eventlog"
)

func TestViewerSnapshotAndStream(t *testing.T) {
	store := eventlog.New(eventlog.Config{MaxEvents: 2100})
	key := eventlog.Key{SessionID: "s", Generation: 1}
	store.Start(key, "zoe", 7)
	for i := 0; i < 2050; i++ {
		store.Append(key, "upstream_to_client", "test", "queued", []byte(`{"type":"data_message","data":{"type":"delegation.created"}}`), false)
	}
	mux := http.NewServeMux()
	New(Dependencies{Events: store}).RegisterViewer(mux)
	for _, path := range []string{"/api/call-sessions/missing/generations", "/api/call-sessions/s/generations/2/events"} {
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 404 {
			t.Fatal(path, w.Code)
		}
	}
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("GET", "/api/call-sessions/s/generations/1/events?limit=9000", nil))
	var snap eventlog.Snapshot
	if err := json.Unmarshal(w.Body.Bytes(), &snap); err != nil || len(snap.Events) != 2000 || snap.ZoeGeneration != 7 {
		t.Fatal(err, len(snap.Events), snap.ZoeGeneration)
	}
	if snap.Events[0].Type != "delegation.created" || snap.Events[0].Wrapper != "data_message" || !strings.Contains(w.Body.String(), `"wrapper":"data_message"`) {
		t.Fatal(snap.Events[0])
	}
	store.End(key)
	server := httptest.NewServer(mux)
	defer server.Close()
	req, _ := http.NewRequest("GET", server.URL+"/api/call-sessions/s/generations/1/events/stream?after=0", nil)
	req.Header.Set("Last-Event-ID", "2049")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	scanner := bufio.NewScanner(resp.Body)
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	body := strings.Join(lines, "\n")
	if strings.Count(body, "event: event") != 1 || !strings.Contains(body, "id: 2050") || !strings.Contains(body, `"type":"delegation.created"`) || !strings.Contains(body, `"wrapper":"data_message"`) || !strings.Contains(body, "event: end") {
		t.Fatal(body)
	}
}

func TestViewerStreamHandoffAndGap(t *testing.T) {
	store := eventlog.New(eventlog.Config{MaxEvents: 2})
	key := eventlog.Key{SessionID: "s", Generation: 1}
	store.Start(key, "")
	for i := 0; i < 4; i++ {
		store.Append(key, "d", "s", "queued", []byte("x"), false)
	}
	mux := http.NewServeMux()
	New(Dependencies{Events: store}).RegisterViewer(mux)
	server := httptest.NewServer(mux)
	defer server.Close()
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("GET", "/api/call-sessions/s/generations/1/events", nil))
	var snapshot eventlog.Snapshot
	if json.Unmarshal(w.Body.Bytes(), &snapshot) != nil || !snapshot.Gap || snapshot.Dropped != 2 {
		t.Fatal(w.Body.String())
	}
	if strings.Contains(w.Body.String(), `"wrapper"`) {
		t.Fatal(w.Body.String())
	}
	resp, err := server.Client().Get(server.URL + "/api/call-sessions/s/generations/1/events/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	scan := bufio.NewScanner(resp.Body)
	got := make(chan string, 20)
	go func() {
		for scan.Scan() {
			got <- scan.Text()
		}
		close(got)
	}()
	var lines []string
	for {
		select {
		case line := <-got:
			lines = append(lines, line)
			if line == "id: 4" {
				goto live
			}
		case <-time.After(3 * time.Second):
			t.Fatal(lines)
		}
	}
live:
	store.Append(key, "d", "s", "queued", []byte("y"), false)
	store.End(key)
	for line := range got {
		lines = append(lines, line)
	}
	text := strings.Join(lines, "\n")
	if !strings.Contains(text, `"from":1,"to":2`) || strings.Count(text, "id: 5") != 1 || !strings.Contains(text, "event: end") {
		t.Fatal(fmt.Sprint(lines))
	}
}

func TestViewerHeartbeatAndDisconnect(t *testing.T) {
	old := viewerHeartbeatInterval
	viewerHeartbeatInterval = 20 * time.Millisecond
	defer func() { viewerHeartbeatInterval = old }()
	store := eventlog.New(eventlog.Defaults())
	key := eventlog.Key{SessionID: "s", Generation: 1}
	store.Start(key, "")
	mux := http.NewServeMux()
	New(Dependencies{Events: store}).RegisterViewer(mux)
	server := httptest.NewServer(mux)
	defer server.Close()
	resp, err := server.Client().Get(server.URL + "/api/call-sessions/s/generations/1/events/stream")
	if err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(resp.Body).ReadString('\n')
	if err != nil || line != ": heartbeat\n" {
		t.Fatal(line, err)
	}
	resp.Body.Close()
}

func TestViewerSourceKindJSON(t *testing.T) {
	store := eventlog.New(eventlog.Defaults())
	key := eventlog.Key{SessionID: "s", Generation: 1}
	store.Start(key, "")
	store.AppendWithSourceKind(key, "gateway_to_upstream", "avas_sideband", "sent", []byte(`{"type":"session.context.append"}`), false, "plan_note")
	store.Append(key, "gateway_to_upstream", "avas_sideband", "sent", []byte(`{"type":"session.context.append"}`), false)
	store.End(key)
	mux := http.NewServeMux()
	New(Dependencies{Events: store}).RegisterViewer(mux)
	for _, suffix := range []string{"events", "events/stream"} {
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest("GET", "/api/call-sessions/s/generations/1/"+suffix, nil))
		if w.Code != 200 || strings.Count(w.Body.String(), `"source_kind":"plan_note"`) != 1 || strings.Count(w.Body.String(), `"source_kind"`) != 1 {
			t.Fatal(suffix, w.Code, w.Body.String())
		}
	}
}
