package main

// One-off live probe for the ChatGPT web voice DataChannel protocol.
// Output is deliberately limited to event types and short assistant text.

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/accounts"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/config"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/httpclient"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/relay"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/secretbox"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/store"

	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

type eventLog struct {
	mu     sync.Mutex
	types  []string
	texts  []string
	unsafe bool
}

func (l *eventLog) add(raw []byte) {
	var outer struct { Type string `json:"type"`; Data string `json:"data"` }
	if json.Unmarshal(raw, &outer) != nil { return }
	data := outer.Data
	if data == "" { data = string(raw) }
	var inner map[string]any
	if json.Unmarshal([]byte(data), &inner) != nil { return }
	t, _ := inner["type"].(string)
	if t == "" { t = outer.Type }
	l.mu.Lock()
	l.types = append(l.types, t)
	if t == "error" || t == "goodbye" { l.unsafe = true }
	if t == "chat_message_delta" {
		if s := findText(inner); s != "" && len(s) <= 300 { l.texts = append(l.texts, s) }
	}
	l.mu.Unlock()
}

func findText(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case map[string]any:
		for _, k := range []string{"text", "delta", "content", "message"} { if y, ok := x[k]; ok { if s := findText(y); s != "" { return s } } }
	case []any:
		for _, y := range x { if s := findText(y); s != "" { return s } }
	}
	return ""
}

func clientAPI() (*webrtc.API, error) {
	me := &webrtc.MediaEngine{}
	if err := me.RegisterCodec(webrtc.RTPCodecParameters{RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2, SDPFmtpLine: "minptime=10;useinbandfec=1"}, PayloadType: 111}, webrtc.RTPCodecTypeAudio); err != nil { return nil, err }
	ir := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(me, ir); err != nil { return nil, err }
	return webrtc.NewAPI(webrtc.WithMediaEngine(me), webrtc.WithInterceptorRegistry(ir))
}

func offer(api *webrtc.API) (*webrtc.PeerConnection, string, *webrtc.DataChannel, *webrtc.TrackLocalStaticRTP, error) {
	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil { return nil, "", nil, nil, err }
	track, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2, SDPFmtpLine: "minptime=10;useinbandfec=1"}, "audio", "probe")
	if err != nil { return nil, "", nil, nil, err }
	if _, err = pc.AddTrack(track); err != nil { return nil, "", nil, nil, err }
	if _, err = pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvOnly}); err != nil { return nil, "", nil, nil, err }
	neg := true; id := uint16(0)
	dc, err := pc.CreateDataChannel("oai-events", &webrtc.DataChannelInit{Negotiated: &neg, ID: &id})
	if err != nil { return nil, "", nil, nil, err }
	o, err := pc.CreateOffer(nil); if err != nil { return nil, "", nil, nil, err }
	if err = pc.SetLocalDescription(o); err != nil { return nil, "", nil, nil, err }
	<-webrtc.GatheringCompletePromise(pc)
	return pc, pc.LocalDescription().SDP, dc, track, nil
}

func postWM(cfg config.Config, token, proxy, device, sdp, session string) (string, error) {
	body, ct := multipartBody(sdp, session)
	client := httpclient.New(httpclient.FromConfig(cfg), proxy)
	req, err := http.NewRequest(http.MethodPost, "https://chatgpt.com/realtime/wm?dcid=0", strings.NewReader(body)); if err != nil { return "", err }
	req.Header.Set("content-type", ct); req.Header.Set("accept", "*/*"); req.Header.Set("origin", "https://chatgpt.com"); req.Header.Set("referer", "https://chatgpt.com/"); req.Header.Set("user-agent", cfg.DefaultUA); req.Header.Set("authorization", "Bearer "+token); if device != "" { req.Header.Set("oai-device-id", device) }
	resp, err := client.Do(req); if err != nil { return "", err }; defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20)); if err != nil { return "", err }
	if resp.StatusCode != 200 && resp.StatusCode != 201 { return "", fmt.Errorf("wm status %d", resp.StatusCode) }
	return string(b), nil
}

func multipartBody(sdp, session string) (string, string) {
	b := "----ProbeBoundary"
	return "--"+b+"\r\nContent-Disposition: form-data; name=\"sdp\"\r\n\r\n"+sdp+"\r\n--"+b+"\r\nContent-Disposition: form-data; name=\"session\"\r\n\r\n"+session+"\r\n--"+b+"--\r\n", "multipart/form-data; boundary="+b
}

func envelope(inner any) []byte { b, _ := json.Marshal(inner); o, _ := json.Marshal(map[string]any{"type":"data_message", "data":string(b)}); return o }
func relayText(s string) map[string]any { return map[string]any{"type":"relay_message", "payload":map[string]any{"type":"relay_message", "message":s}} }

func send(dc *webrtc.DataChannel, x any) error { return dc.SendText(string(envelope(x))) }
func silence(track *webrtc.TrackLocalStaticRTP, stop <-chan struct{}) { ticker := time.NewTicker(20*time.Millisecond); defer ticker.Stop(); var seq uint16; var ts uint32; for { select { case <-stop: return; case <-ticker.C: _ = track.WriteRTP(&rtp.Packet{Header: rtp.Header{Version:2, PayloadType:111, SequenceNumber:seq, Timestamp:ts, SSRC:42}, Payload: make([]byte, 20)}); seq++; ts += 960 } } }

func main() {
	var db, key string; flag.StringVar(&db, "db", os.Getenv("VOICE_DATABASE_FILE"), "copy database"); flag.StringVar(&key, "key", os.Getenv("VOICE_TOKEN_ENCRYPTION_KEY"), "token key"); flag.Parse()
	if db == "" || key == "" { log.Fatal("db and key required") }
	cfg := config.Load(); cfg.DatabaseFile = db; cfg.Environment = config.EnvironmentProduction; cfg.UpstreamTransport = config.TransportGo; cfg.MaxAccountAttempts = 1
	k, err := secretbox.ParseKey(key); if err != nil { log.Fatal(err) }; box, err := secretbox.New(k); if err != nil { log.Fatal(err) }
	d, err := store.Open(db); if err != nil { log.Fatal(err) }; defer d.Close(); pool := accounts.NewPoolFromDB(d).WithBox(box)
	_, acct, err := pool.Pick("", nil); if err != nil { log.Fatal(err) }
	fmt.Printf("account_selected status=ok proxy_present=%t\n", acct.Proxy != "")
	api, err := clientAPI(); if err != nil { log.Fatal(err) }; client, sdp, dc, track, err := offer(api); if err != nil { log.Fatal(err) }; defer client.Close()
	stop := make(chan struct{}); defer close(stop); go silence(track, stop)
	session := `{"backend_reasoning_effort":"instant","language_code":"en","requested_default_model":"","voice":"juniper","voice_session_id":"PROBE","voice_status_request_id":"PROBE","timezone_offset_min":0,"timezone":"UTC","voice_mode":"conversation","model_slug":"","model_slug_advanced":"","client_tools":[],"history_and_training_disabled":false,"conversation_mode":{"kind":"primary_assistant"},"enable_message_streaming":true}`
	r, err := relay.New(relay.Config{}, nil); if err != nil { log.Fatal(err) }
	rs, wmOffer, err := r.NewSession(context.Background(), sdp, relay.Route{}); if err != nil { log.Fatal(err) }
	logEvents := &eventLog{}
	rs.SetEventObserver(logEvents.add)
	answer, err := postWM(cfg, acct.AccessToken, acct.Proxy, acct.DeviceID, wmOffer, session); if err != nil { log.Fatal(err) }
	clientAnswer, err := rs.AcceptUpstreamAnswer(context.Background(), answer); if err != nil { log.Fatal(err) }
	if err := client.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: clientAnswer}); err != nil { log.Fatal(err) }
	open := make(chan struct{}); dc.OnOpen(func(){ close(open) }); select { case <-open: case <-time.After(25*time.Second): log.Fatal("client data channel timeout") }
	fmt.Println("call_ready")
	step := func(name string, x any, wait time.Duration) { logEvents.mu.Lock(); before := len(logEvents.types); logEvents.mu.Unlock(); fmt.Printf("sent %s %s\n", name, safeSent(x)); if err := send(dc, x); err != nil { log.Fatal(err) }; time.Sleep(wait); logEvents.mu.Lock(); fmt.Printf("result %s event_types=%s assistant_text=%s\n", name, strings.Join(logEvents.types[before:], ","), strings.Join(logEvents.texts, " | ")); logEvents.mu.Unlock() }
	step("E1", relayText("Reply with exactly the word BASELINE."), 7*time.Second)
	step("E2_update_wrapped", map[string]any{"type":"session.update","session":map[string]any{"instructions":"Always answer with exactly the word PIRATE and nothing else."}}, 2*time.Second)
	step("E2_question", relayText("What is two plus two?"), 7*time.Second)
	step("E2_update_bare", map[string]any{"type":"session.update","session":map[string]any{"instructions":"Always answer with exactly the word PIRATE and nothing else."}}, 2*time.Second)
	step("E3_tools", map[string]any{"type":"session.update","session":map[string]any{"tools":[]any{map[string]any{"type":"function","name":"create_task","description":"Create a task for the user. Call this whenever the user asks to create a document or file.","parameters":map[string]any{"type":"object","properties":map[string]any{"title":map[string]any{"type":"string"}},"required":[]string{"title"}}}},"tool_choice":"auto"}}, 2*time.Second)
	step("E3_question", relayText("Please create an HTML document called hello."), 8*time.Second)
	step("E4_context", relayText("System note: the user's project is called ORCA-7. Do not answer this message."), 3*time.Second)
	step("E4_question", relayText("What is my project called?"), 7*time.Second)
	step("E5_response_create", map[string]any{"type":"response.create"}, 6*time.Second)
	// E6 is represented by a fresh call using client_tools in the session JSON.
	fmt.Println("verdict live_call_complete")
	_ = rs.Close()
}

func safeSent(x any) string {
	b, _ := json.Marshal(x); s := string(b); if len(s) > 220 { return s[:220]+"..." }; return s
}
