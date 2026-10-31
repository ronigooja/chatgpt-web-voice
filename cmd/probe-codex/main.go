// Disposable one-account Codex AVAS/V3 probe. Output is structural and redacted.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/accounts"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/config"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/httpclient"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/secretbox"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/store"
	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

const endpoint = "https://chatgpt.com/backend-api/codex/realtime/calls?intent=quicksilver&architecture=avas"

type session struct {
	Model        string `json:"model"`
	Instructions string `json:"instructions"`
	Audio        struct {
		Output struct {
			Voice string `json:"voice"`
		} `json:"output"`
	} `json:"audio"`
	Delegation struct {
		Type string `json:"type"`
	} `json:"delegation"`
	InitialItems []item `json:"initial_items"`
}
type item struct {
	Type    string    `json:"type"`
	Role    string    `json:"role"`
	Content []content `json:"content"`
}
type content struct {
	Type string `json:"type"`
	Text string `json:"text"`
}
type requestBody struct {
	SDP     string  `json:"sdp"`
	Session session `json:"session"`
}

func buildSession() session {
	var s session
	s.Model = "gpt-live-1-codex"
	s.Instructions = "You are a test assistant. If the user asks to create a file, delegate it. Remember the project is ORCA-7."
	s.Audio.Output.Voice = "cove"
	s.Delegation.Type = "client"
	s.InitialItems = []item{
		{Type: "message", Role: "user", Content: []content{{Type: "input_text", Text: "My project is called ORCA-7."}}},
		{Type: "message", Role: "assistant", Content: []content{{Type: "output_text", Text: "Understood."}}},
	}
	return s
}

func requestHeaders(token string) map[string]string {
	return map[string]string{
		"Authorization": "Bearer " + token,
		"Content-Type":  "application/json",
		"openai-alpha":  "quicksilver=v2",
		"originator":    "codex_cli_rs",
		"User-Agent":    "codex_cli_rs/0.0.0 (darwin unknown; arm64) Codex",
	}
}

func redactedDump(body requestBody, headers map[string]string) string {
	clone := body
	clone.SDP = "<sdp>"
	cloneHeaders := make([]string, 0, len(headers))
	for k := range headers {
		cloneHeaders = append(cloneHeaders, k)
	}
	sort.Strings(cloneHeaders)
	b, _ := json.Marshal(clone)
	return fmt.Sprintf("method=POST url=%s header_names=%s body=%s", endpoint, strings.Join(cloneHeaders, ","), b)
}

func clientAPI() (*webrtc.API, error) {
	me := &webrtc.MediaEngine{}
	if err := me.RegisterCodec(webrtc.RTPCodecParameters{RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2, SDPFmtpLine: "minptime=10;useinbandfec=1"}, PayloadType: 111}, webrtc.RTPCodecTypeAudio); err != nil {
		return nil, err
	}
	ir := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(me, ir); err != nil {
		return nil, err
	}
	return webrtc.NewAPI(webrtc.WithMediaEngine(me), webrtc.WithInterceptorRegistry(ir)), nil
}

func makeOffer(api *webrtc.API) (*webrtc.PeerConnection, *webrtc.DataChannel, *webrtc.TrackLocalStaticRTP, string, error) {
	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		return nil, nil, nil, "", err
	}
	track, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2, SDPFmtpLine: "minptime=10;useinbandfec=1"}, "audio", "codex-probe")
	if err != nil {
		return nil, nil, nil, "", err
	}
	if _, err = pc.AddTrack(track); err != nil {
		return nil, nil, nil, "", err
	}
	if _, err = pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
		return nil, nil, nil, "", err
	}
	dc, err := pc.CreateDataChannel("oai-events", nil)
	if err != nil {
		return nil, nil, nil, "", err
	}
	o, err := pc.CreateOffer(nil)
	if err != nil {
		return nil, nil, nil, "", err
	}
	if err = pc.SetLocalDescription(o); err != nil {
		return nil, nil, nil, "", err
	}
	<-webrtc.GatheringCompletePromise(pc)
	return pc, dc, track, pc.LocalDescription().SDP, nil
}

func opusPT(sdp string) uint8 {
	r := regexp.MustCompile(`(?mi)^a=rtpmap:(\d+) opus/`)
	m := r.FindStringSubmatch(sdp)
	if len(m) != 2 {
		return 111
	}
	var n uint8
	_, _ = fmt.Sscanf(m[1], "%d", &n)
	return n
}
func hasApplication(sdp string) bool {
	return regexp.MustCompile(`(?mi)^m=application\s`).MatchString(sdp)
}
func sendSilence(t *webrtc.TrackLocalStaticRTP, pt uint8, stop <-chan struct{}) {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	var seq uint16
	var ts uint32
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			_ = t.WriteRTP(&rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: pt, SequenceNumber: seq, Timestamp: ts, SSRC: 77}, Payload: []byte{0xf8, 0xff, 0xfe}})
			seq++
			ts += 960
		}
	}
}

type frames struct {
	sync.Mutex
	types          []string
	delegationID   string
	delegationText string
	assistant      []string
}

func (f *frames) add(raw string) {
	var v map[string]any
	if json.Unmarshal([]byte(raw), &v) != nil {
		return
	}
	t, _ := v["type"].(string)
	if t == "" {
		return
	}
	f.Lock()
	defer f.Unlock()
	f.types = append(f.types, t)
	if t == "delegation.created" {
		if x, ok := v["item"].(map[string]any); ok {
			f.delegationID, _ = x["id"].(string)
			f.delegationText = findText(x)
		}
	}
	if strings.HasPrefix(t, "output_") || strings.Contains(t, "assistant") {
		if s := findText(v); s != "" && len(s) <= 200 {
			f.assistant = append(f.assistant, s)
		}
	}
	prefix := raw
	if len(prefix) > 120 {
		prefix = prefix[:120]
	}
	if strings.Contains(strings.ToLower(prefix), "token") || strings.Contains(strings.ToLower(prefix), "cookie") {
		prefix = "[redacted]"
	}
	fmt.Printf("data_frame type=%s prefix=%q\n", t, prefix)
}
func findText(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case map[string]any:
		for _, k := range []string{"text", "transcript", "content"} {
			if y, ok := x[k]; ok {
				if s := findText(y); s != "" {
					return s
				}
			}
		}
	case []any:
		for _, y := range x {
			if s := findText(y); s != "" {
				return s
			}
		}
	}
	return ""
}
func sendText(dc *webrtc.DataChannel, text string) error {
	b, _ := json.Marshal(map[string]any{"type": "conversation.item.create", "item": map[string]any{"type": "message", "role": "user", "content": []any{map[string]any{"type": "input_text", "text": text}}}})
	return dc.SendText(string(b))
}
func sendDelegation(dc *webrtc.DataChannel, id, text string) error {
	b, _ := json.Marshal(map[string]any{"type": "delegation.context.append", "delegation_item_id": id, "content": []any{map[string]any{"type": "input_text", "text": text}}})
	return dc.SendText(string(b))
}

func main() {
	var db, key string
	flag.StringVar(&db, "db", os.Getenv("VOICE_DATABASE_FILE"), "copied database")
	flag.StringVar(&key, "key", os.Getenv("VOICE_TOKEN_ENCRYPTION_KEY"), "database key")
	flag.Parse()
	if db == "" || key == "" {
		log.Fatal("db and key required")
	}
	k, err := secretbox.ParseKey(key)
	if err != nil {
		log.Fatal(err)
	}
	box, err := secretbox.New(k)
	if err != nil {
		log.Fatal(err)
	}
	d, err := store.Open(db)
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()
	pool := accounts.NewPoolFromDB(d).WithBox(box)
	token, account, err := pool.Pick("", nil)
	if err != nil {
		log.Fatal(err)
	}
	_ = token
	fmt.Printf("pool_structural selected=true proxy_present=%t\n", account.Proxy != "")
	api, err := clientAPI()
	if err != nil {
		log.Fatal(err)
	}
	pc, dc, track, offer, err := makeOffer(api)
	if err != nil {
		log.Fatal(err)
	}
	defer pc.Close()
	body, _ := json.Marshal(requestBody{SDP: offer, Session: buildSession()})
	opts := httpclient.FromConfig(config.Load())
	client := httpclient.New(opts, account.Proxy)
	req, _ := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
	for k, v := range requestHeaders(token) {
		req.Header.Set(k, v)
	}
	fmt.Printf("request_dump %s\n", redactedDump(requestBody{SDP: offer, Session: buildSession()}, requestHeaders("<token>")))
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	location := resp.Header.Get("Location")
	shape := "absent"
	if location != "" {
		last := location[strings.LastIndex(location, "/")+1:]
		prefix := last
		if len(prefix) > 4 {
			prefix = prefix[:4]
		}
		shape = fmt.Sprintf("prefix=%s length=%d", prefix, len(last))
	}
	name := make([]string, 0, len(resp.Header))
	for k := range resp.Header {
		name = append(name, k)
	}
	sort.Strings(name)
	isSDP := resp.StatusCode >= 200 && resp.StatusCode < 300 && strings.HasPrefix(strings.TrimSpace(string(raw)), "v=")
	fmt.Printf("step1 status=%d sdp=%t location=%s response_header_names=%s\n", resp.StatusCode, isSDP, shape, strings.Join(name, ","))
	if resp.StatusCode == 401 || resp.StatusCode == 403 || resp.StatusCode == 429 || !isSDP {
		var e map[string]any
		if json.Unmarshal(raw, &e) == nil {
			typ, _ := e["type"].(string)
			code, _ := e["code"].(string)
			fmt.Printf("step1_error type=%s code=%s\n", typ, code)
		}
		return
	}
	answer := string(raw)
	fmt.Printf("answer_application_mline=%t\n", hasApplication(answer))
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: answer}); err != nil {
		log.Fatal(err)
	}
	stop := make(chan struct{})
	defer close(stop)
	go sendSilence(track, opusPT(answer), stop)
	logf := &frames{}
	dc.OnMessage(func(m webrtc.DataChannelMessage) {
		if m.IsString {
			logf.add(string(m.Data))
		} else {
			fmt.Printf("data_frame binary=true bytes=%d\n", len(m.Data))
		}
	})
	open := make(chan struct{})
	dc.OnOpen(func() { close(open) })
	select {
	case <-open:
		fmt.Println("data_channel_open=true")
	case <-time.After(15 * time.Second):
		fmt.Println("data_channel_open=false")
		return
	}
	time.Sleep(5 * time.Second)
	_ = sendText(dc, "Please create an HTML file called hello.")
	fmt.Println("sent typed_text=true")
	time.Sleep(15 * time.Second)
	logf.Lock()
	id, txt := logf.delegationID, logf.delegationText
	logf.Unlock()
	if id != "" {
		_ = sendDelegation(dc, id, "Done: hello.html created.")
		fmt.Printf("delegation_created=true text=%q\n", txt)
		time.Sleep(15 * time.Second)
	} else {
		fmt.Println("delegation_created=false")
	}
	_ = sendText(dc, "What is my project called?")
	time.Sleep(15 * time.Second)
	logf.Lock()
	fmt.Printf("event_types=%s assistant_text=%q\n", strings.Join(logf.types, ","), strings.Join(logf.assistant, " | "))
	logf.Unlock()
	fmt.Println("sideband=not_attempted_not_derivable_from_codex_source")
}
