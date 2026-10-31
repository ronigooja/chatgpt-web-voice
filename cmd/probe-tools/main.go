package main

// One-off live probe for the ChatGPT web voice DataChannel protocol.
// Output is deliberately limited to event types and short assistant text.

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"
	"github.com/google/uuid"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/accounts"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/config"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/relay"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/secretbox"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/store"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/voice"

	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

type eventLog struct {
	mu     sync.Mutex
	types  []string
	texts  []string
	unsafe bool
	frames int
	bytes int
	textFrames int
	binaryFrames int
	rawTypes []string
	rawTexts []string
	functionCalls []map[string]string
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

func (l *eventLog) addRaw(data []byte, isString bool) {
	if !isString { return }
	var outer struct { Type string `json:"type"`; Data string `json:"data"` }
	if json.Unmarshal(data, &outer) != nil { return }
	innerRaw := outer.Data; if innerRaw == "" { innerRaw = string(data) }
	var inner map[string]any
	if json.Unmarshal([]byte(innerRaw), &inner) != nil { return }
	t, _ := inner["type"].(string); if t == "" { t = outer.Type }
	l.mu.Lock(); l.rawTypes = append(l.rawTypes, t)
	if isConversationLike(t) { if text := findText(inner); text != "" && len(text) <= 400 { l.rawTexts = append(l.rawTexts, text) } }
	collectFunctionCalls(inner, &l.functionCalls)
	if t == "error" || t == "goodbye" { l.unsafe = true }
	l.mu.Unlock()
}

func isConversationLike(t string) bool { return strings.Contains(t, "chat") || strings.Contains(t, "conversation") || strings.Contains(t, "transcript") || strings.Contains(t, "function_call") || strings.Contains(t, "response") }

func collectFunctionCalls(v any, out *[]map[string]string) {
	switch x := v.(type) {
	case map[string]any:
		if typ, _ := x["type"].(string); typ == "function_call" || strings.Contains(typ, "function_call") {
			item := map[string]string{}; for _, k := range []string{"call_id", "id", "name", "arguments"} { if s, ok := x[k].(string); ok { item[k] = s } }; *out = append(*out, item)
		}
		for _, y := range x { collectFunctionCalls(y, out) }
	case []any:
		for _, y := range x { collectFunctionCalls(y, out) }
	}
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
	return webrtc.NewAPI(webrtc.WithMediaEngine(me), webrtc.WithInterceptorRegistry(ir)), nil
}

func offer(api *webrtc.API) (*webrtc.PeerConnection, string, *webrtc.DataChannel, *webrtc.TrackLocalStaticRTP, error) {
	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil { return nil, "", nil, nil, err }
	track, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2, SDPFmtpLine: "minptime=10;useinbandfec=1"}, "audio", "probe")
	if err != nil { return nil, "", nil, nil, err }
	if _, err = pc.AddTrack(track); err != nil { return nil, "", nil, nil, err }
	if _, err = pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil { return nil, "", nil, nil, err }
	neg := true; id := uint16(0)
	dc, err := pc.CreateDataChannel("oai-events", &webrtc.DataChannelInit{Negotiated: &neg, ID: &id})
	if err != nil { return nil, "", nil, nil, err }
	o, err := pc.CreateOffer(nil); if err != nil { return nil, "", nil, nil, err }
	if err = pc.SetLocalDescription(o); err != nil { return nil, "", nil, nil, err }
	<-webrtc.GatheringCompletePromise(pc)
	return pc, pc.LocalDescription().SDP, dc, track, nil
}

func envelope(inner any) []byte { b, _ := json.Marshal(inner); o, _ := json.Marshal(map[string]any{"type":"data_message", "data":string(b)}); return o }
func relayText(s string) map[string]any {
	return map[string]any{"type":"relay_message", "payload":map[string]any{"type":"relay_message", "message":map[string]any{
		"id": uuid.NewString(), "author": map[string]any{"role":"user"}, "create_time": float64(time.Now().UnixNano()) / 1e9,
		"content": map[string]any{"content_type":"text", "parts":[]any{s}},
		"metadata": map[string]any{"serialization_metadata":map[string]any{"custom_symbol_offsets":[]any{}}},
		"clientMetadata": map[string]any{"isOptimistic":true},
	}}}
}

func send(dc *webrtc.DataChannel, x any) error { return dc.SendText(string(envelope(x))) }
func sendTyped(dc *webrtc.DataChannel, text string) error {
	if err := send(dc, map[string]any{"type":"action_request", "payload":map[string]any{"action":"stop_speaking"}}); err != nil { return err }
	return send(dc, relayText(text))
}
func silence(track *webrtc.TrackLocalStaticRTP, payloadType uint8, stop <-chan struct{}) { ticker := time.NewTicker(20*time.Millisecond); defer ticker.Stop(); var seq uint16; var ts uint32; for { select { case <-stop: return; case <-ticker.C: _ = track.WriteRTP(&rtp.Packet{Header: rtp.Header{Version:2, PayloadType:payloadType, SequenceNumber:seq, Timestamp:ts, SSRC:42}, Payload: []byte{0xf8, 0xff, 0xfe}}); seq++; ts += 960 } } }

func redactFrame(b []byte) string { s := string(b); if len(s) > 120 { s = s[:120] }; for _, marker := range []string{"Bearer ", "access_token", "token", "cookie", "session_token"} { if strings.Contains(strings.ToLower(s), strings.ToLower(marker)) { return "[redacted]" } }; return s }

func audioPayloadTypes(sdp string) []string {
	var out []string
	for _, line := range strings.Split(sdp, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "a=rtpmap:") && strings.Contains(strings.ToLower(line), "opus/") {
			parts := strings.SplitN(strings.TrimPrefix(line, "a=rtpmap:"), " ", 2)
			if len(parts) == 2 { out = append(out, parts[0]) }
		}
	}
	return out
}

func realtimeSessionUpdate(instructions string) map[string]any {
	return map[string]any{"type":"session.update", "session":map[string]any{"type":"realtime", "instructions":instructions}}
}

func realtimeSessionUpdateWithTools() map[string]any {
	s := realtimeSessionUpdate("")
	s["session"].(map[string]any)["tools"] = []any{map[string]any{"type":"function","name":"create_task","description":"Create a task for the user. Call this whenever the user asks to create a document or file.","parameters":map[string]any{"type":"object","properties":map[string]any{"title":map[string]any{"type":"string"}},"required":[]string{"title"}}}}
	s["session"].(map[string]any)["tool_choice"] = "auto"
	return s
}

func runCall(cfg config.Config, pool *accounts.Pool, rl relay.Relay, clientTools []any, label string) (int, error) {
	api, err := clientAPI(); if err != nil { return 0, err }
	client, sdp, dc, track, err := offer(api); if err != nil { return 0, err }; defer client.Close()
	stop := make(chan struct{}); defer close(stop)
	logEvents := &eventLog{}
	diagnostics := relay.Diagnostics{
		OnFrame: func(side string, data []byte, isString bool) { if side == "upstream" { logEvents.addRaw(data, isString) }; logEvents.mu.Lock(); logEvents.frames++; logEvents.bytes += len(data); if isString { logEvents.textFrames++ } else { logEvents.binaryFrames++ }; n := logEvents.frames; logEvents.mu.Unlock(); if isString { fmt.Printf("raw_frame side=%s kind=text bytes=%d index=%d prefix=%q\n", side, len(data), n, redactFrame(data)) } else { fmt.Printf("raw_frame side=%s kind=binary bytes=%d index=%d\n", side, len(data), n) } },
		OnState: func(side, layer, state string) { fmt.Printf("peer_state side=%s layer=%s state=%s\n", side, layer, state) },
		OnChannel: func(side, state string) { fmt.Printf("channel_state side=%s state=%s\n", side, state) },
	}
	svc := voice.New(cfg, pool, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))).WithRelay(rl).WithEventObserver(logEvents.add).WithRelayDiagnostics(diagnostics)
	defaults := voice.Config().Defaults
	req := voice.CreateSessionRequest{Owner: "probe", OfferSDP: sdp, Voice: defaults.Voice, VoiceMode: defaults.VoiceMode, LanguageCode: defaults.LanguageCode, ClientTools: clientTools}
	if err := voice.ValidateCreateSessionInput(req); err != nil { return 0, fmt.Errorf("local input validation: %w", err) }
	fmt.Printf("local_input_validation=ok voice=%s voice_mode=%s language=%s\n", defaults.Voice, defaults.VoiceMode, defaults.LanguageCode)
	res, err := svc.CreateSession(req)
	if err != nil { return 0, err }
	fmt.Printf("%s_connected wm_status=accepted session=%s\n", label, res.SessionID[:8])
	fmt.Printf("negotiated_audio_payload_types=%s\n", strings.Join(audioPayloadTypes(res.AnswerSDP), ","))
	if err := client.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: res.AnswerSDP}); err != nil { return 0, err }
	payloadTypes := audioPayloadTypes(res.AnswerSDP); payloadType := uint8(111); if len(payloadTypes) > 0 { var n int; if _, err := fmt.Sscanf(payloadTypes[0], "%d", &n); err == nil && n >= 0 && n <= 127 { payloadType = uint8(n) } }; go silence(track, payloadType, stop)
	open := make(chan struct{}); dc.OnOpen(func(){ close(open) }); select { case <-open: case <-time.After(25*time.Second): return 0, fmt.Errorf("client data channel timeout") }
	time.Sleep(15 * time.Second)
	logEvents.mu.Lock(); initialTypes := append([]string(nil), logEvents.rawTypes...); fmt.Printf("initial_counts frames=%d bytes=%d text=%d binary=%d raw_event_types=%s\n", logEvents.frames, logEvents.bytes, logEvents.textFrames, logEvents.binaryFrames, strings.Join(initialTypes, ",")); logEvents.mu.Unlock()
	if len(initialTypes) == 0 || initialTypes[0] != "state_update" { return 0, fmt.Errorf("raw precondition failed: first event type=%q", func() string { if len(initialTypes)>0{return initialTypes[0]};return "" }()) }
	step := func(name string, x any, wait time.Duration) error { logEvents.mu.Lock(); before := len(logEvents.rawTypes); textBefore := len(logEvents.rawTexts); callsBefore := len(logEvents.functionCalls); logEvents.mu.Unlock(); fmt.Printf("sent %s %s\n", name, safeSent(x)); var err error; if msg, ok := x.(map[string]any); ok && msg["type"] == "relay_message" { var text string; if p, ok := msg["payload"].(map[string]any); ok { if m, ok := p["message"].(map[string]any); ok { if c, ok := m["content"].(map[string]any); ok { if parts, ok := c["parts"].([]any); ok && len(parts)>0 { text, _ = parts[0].(string) } } } }; err = sendTyped(dc, text) } else { err = send(dc, x) }; if err != nil { return err }; time.Sleep(wait); logEvents.mu.Lock(); types := append([]string(nil), logEvents.rawTypes[before:]...); texts := append([]string(nil), logEvents.rawTexts[textBefore:]...); calls := append([]map[string]string(nil), logEvents.functionCalls[callsBefore:]...); unsafe := logEvents.unsafe; logEvents.mu.Unlock(); fmt.Printf("result %s event_types=%s assistant_text=%s function_calls=%d unsafe=%t\n", name, strings.Join(types, ","), strings.Join(texts, " | "), len(calls), unsafe); if len(calls)>0 { for _, call := range calls { id:=call["call_id"]; if id=="" { id=call["id"] }; if id!="" { _ = send(dc, map[string]any{"type":"conversation.item.create","item":map[string]any{"type":"function_call_output","call_id":id,"output":"done"}}); _ = send(dc, map[string]any{"type":"response.create"}); fmt.Printf("sent function_call_output call_id_present=true\n") } } }; return nil }
	if label == "E1-E5" {
		if err := step("E1", relayText("Reply with exactly the word BASELINE."), 10*time.Second); err != nil { return 0, err }
		logEvents.mu.Lock(); replied := len(logEvents.rawTexts) > 0 || hasReplyEvent(logEvents.rawTypes); logEvents.mu.Unlock()
		if !replied { return 0, fmt.Errorf("S1 produced no assistant reply event") }
		if err := step("E2_update_wrapped", realtimeSessionUpdate("Always answer with exactly the word PIRATE and nothing else."), 2*time.Second); err != nil { return 0, err }
		if err := step("E2_question", relayText("What is two plus two?"), 10*time.Second); err != nil { return 0, err }
		if err := step("E3_tools", realtimeSessionUpdateWithTools(), 2*time.Second); err != nil { return 0, err }
		if err := step("E3_question", relayText("Please create an HTML document called hello."), 10*time.Second); err != nil { return 0, err }
		if err := step("E4_context", relayText("System note: the user's project is called ORCA-7. Do not answer this message."), 10*time.Second); err != nil { return 0, err }
		if err := step("E4_question", relayText("What is my project called?"), 10*time.Second); err != nil { return 0, err }
		if err := step("E5_response_create", map[string]any{"type":"response.create"}, 10*time.Second); err != nil { return 0, err }
	}
	if label == "E6" {
		if err := step("E6_question", relayText("Please create an HTML document called hello."), 8*time.Second); err != nil { return 0, err }
	}
	if !svc.ReleaseSession("probe", res.VoiceSessionID) { return 0, fmt.Errorf("session release not acknowledged") }
	return 201, nil
}

func hasReplyEvent(types []string) bool {
	for _, t := range types { if strings.Contains(t, "chat_message_delta") || strings.Contains(t, "transcript") || strings.Contains(t, "conversation") || strings.Contains(t, "audio") { return true } }
	return false
}

func main() {
	var db, key string; flag.StringVar(&db, "db", os.Getenv("VOICE_DATABASE_FILE"), "copy database"); flag.StringVar(&key, "key", os.Getenv("VOICE_TOKEN_ENCRYPTION_KEY"), "token key"); flag.Parse()
	if db == "" || key == "" { log.Fatal("db and key required") }
	cfg := config.Load(); cfg.DatabaseFile = db; cfg.Environment = config.EnvironmentProduction; cfg.MaxAccountAttempts = 1
	k, err := secretbox.ParseKey(key); if err != nil { log.Fatal(err) }; box, err := secretbox.New(k); if err != nil { log.Fatal(err) }
	d, err := store.Open(db); if err != nil { log.Fatal(err) }; defer d.Close(); pool := accounts.NewPoolFromDB(d).WithBox(box)
	stats, err := pool.Stats(); if err != nil { log.Fatal(err) }; fmt.Printf("pool_structural total=%d available=%d\n", stats.Total, stats.Available)
	rl, err := relay.New(relay.Config{}, nil); if err != nil { log.Fatal(err) }
	if _, err := runCall(cfg, pool, rl, nil, "E1-E5"); err != nil { log.Fatal(err) }
	tools := []any{map[string]any{"type":"function","name":"create_task","description":"Create a task for the user.","parameters":map[string]any{"type":"object","properties":map[string]any{"title":map[string]any{"type":"string"}},"required":[]string{"title"}}}}
	if _, err := runCall(cfg, pool, rl, tools, "E6"); err != nil { log.Fatal(err) }
	stats, err = pool.Stats(); if err == nil { fmt.Printf("pool_structural_after total=%d available=%d\n", stats.Total, stats.Available) }
	fmt.Println("verdict live_call_complete")
}

func safeSent(x any) string {
	b, _ := json.Marshal(x); s := string(b); if len(s) > 220 { return s[:220]+"..." }; return s
}
