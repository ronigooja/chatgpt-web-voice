// Disposable one-account Codex AVAS/V3 probe. Output is structural and redacted.
package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	"github.com/bogdanfinn/websocket"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/accounts"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/config"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/httpclient"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/secretbox"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/store"
	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media/oggreader"
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

const chatGPTAccountIDHeader = "ChatGPT-Account-ID"

func buildSession() session {
	var s session
	s.Model = "gpt-live-1-codex"
	s.Instructions = "You are a test assistant. If the user asks to create a file, delegate it to the background agent. Remember the project is ORCA-7."
	s.Audio.Output.Voice = "cove"
	s.Delegation.Type = "client"
	s.InitialItems = []item{
		{Type: "message", Role: "user", Content: []content{{Type: "input_text", Text: "My project is called ORCA-7."}}},
		{Type: "message", Role: "assistant", Content: []content{{Type: "output_text", Text: "Understood."}}},
	}
	return s
}

func requestHeaders(token string) map[string]string {
	h := map[string]string{
		"Authorization": "Bearer " + token,
		"Content-Type":  "application/json",
		"openai-alpha":  "quicksilver=v2",
		"originator":    "codex_cli_rs",
		"User-Agent":    "codex_cli_rs/0.0.0 (darwin unknown; arm64) Codex",
	}
	if id := accountIDFromToken(token); id != "" {
		h[chatGPTAccountIDHeader] = id
	}
	return h
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

type audioClock struct {
	mu  sync.Mutex
	seq uint16
	ts  uint32
}

func (c *audioClock) write(t *webrtc.TrackLocalStaticRTP, pt uint8, payload []byte) error {
	c.mu.Lock()
	p := &rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: pt, SequenceNumber: c.seq, Timestamp: c.ts, SSRC: 77}, Payload: payload}
	c.seq++
	c.ts += 960
	c.mu.Unlock()
	return t.WriteRTP(p)
}

func sendSilence(t *webrtc.TrackLocalStaticRTP, pt uint8, clock *audioClock, stop <-chan struct{}) {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			_ = clock.write(t, pt, []byte{0xf8, 0xff, 0xfe})
		}
	}
}

func readOpusPackets(path string) ([][]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r, _, err := oggreader.NewWith(f)
	if err != nil {
		return nil, err
	}
	var packets [][]byte
	for {
		payload, _, err := r.ParseNextPage()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(payload) >= 8 && (string(payload[:8]) == "OpusHead" || string(payload[:8]) == "OpusTags") {
			continue
		}
		if len(payload) > 0 {
			packets = append(packets, payload)
		}
	}
	return packets, nil
}

func sendOpus(t *webrtc.TrackLocalStaticRTP, pt uint8, clock *audioClock, packets [][]byte, stop <-chan struct{}) error {
	for _, payload := range packets {
		select {
		case <-stop:
			return nil
		default:
		}
		if err := clock.write(t, pt, payload); err != nil {
			return err
		}
		time.Sleep(20 * time.Millisecond)
	}
	return nil
}

type frames struct {
	sync.Mutex
	types          []string
	delegationID   string
	delegationText string
	assistant      []string
	audioFrames    int
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
	if t == "output_audio.delta" {
		f.audioFrames++
	}
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
	if strings.Contains(t, "input_transcript") {
		fmt.Printf("data_frame type=%s text_length=%d\n", t, len(findText(v)))
	} else if strings.Contains(t, "output_transcript") || t == "delegation.created" {
		text := findText(v)
		if len(text) > 200 {
			text = text[:200]
		}
		fmt.Printf("data_frame type=%s text=%q\n", t, text)
	} else {
		fmt.Printf("data_frame type=%s\n", t)
	}
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

func accountIDFromToken(token string) string {
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return ""
	}
	b, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return ""
	}
	var claims map[string]any
	if json.Unmarshal(b, &claims) != nil {
		return ""
	}
	for _, key := range []string{"chatgpt_account_id", "account_id"} {
		if v, ok := claims[key].(string); ok {
			return v
		}
	}
	if auth, ok := claims["https://api.openai.com/auth"].(map[string]any); ok {
		for _, key := range []string{"chatgpt_account_id", "account_id"} {
			if v, ok := auth[key].(string); ok {
				return v
			}
		}
	}
	return ""
}

func sidebandURL(callID string) string {
	return "wss://api.openai.com/v1/live/" + url.PathEscape(callID)
}

type sidebandResult struct {
	conn                 *websocket.Conn
	status               int
	errorType, errorCode string
}

func connectSideband(callID, token, accountProxy string) sidebandResult {
	h := fhttp.Header{}
	h.Set("Authorization", "Bearer "+token)
	if accountID := accountIDFromToken(token); accountID != "" {
		h.Set(chatGPTAccountIDHeader, accountID)
	}
	h.Set("originator", "codex_cli_rs")
	h.Set("User-Agent", "codex_cli_rs/0.0.0 (linux unknown; amd64) Codex")
	d := websocket.Dialer{HandshakeTimeout: 20 * time.Second, Proxy: func(*fhttp.Request) (*url.URL, error) { return url.Parse(strings.TrimSpace(accountProxy)) }}
	if strings.TrimSpace(accountProxy) == "" || strings.EqualFold(strings.TrimSpace(accountProxy), "direct") {
		d.Proxy = nil
	}
	conn, resp, err := d.Dial(sidebandURL(callID), h)
	result := sidebandResult{conn: conn}
	if resp != nil {
		result.status = resp.StatusCode
	}
	if err != nil {
		if resp != nil {
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
			var e map[string]any
			if json.Unmarshal(b, &e) == nil {
				result.errorType, _ = e["type"].(string)
				result.errorCode, _ = e["code"].(string)
			}
		}
	}
	fmt.Printf("sideband_headers_sent=Authorization,%s,originator,User-Agent attestation=false\n", chatGPTAccountIDHeader)
	if err != nil {
		fmt.Printf("sideband_handshake status=%d error_type=%s error_code=%s\n", result.status, result.errorType, result.errorCode)
	} else {
		fmt.Printf("sideband_handshake status=%d\n", result.status)
	}
	return result
}

func sendSideband(conn *websocket.Conn, value any) error {
	b, _ := json.Marshal(value)
	return conn.WriteMessage(websocket.TextMessage, b)
}
func sendText(dc *webrtc.DataChannel, text string) error {
	for len(text) > 0 {
		n := 500
		if len(text) < n {
			n = len(text)
		}
		for n > 0 && n < len(text) && text[n]&0xc0 == 0x80 {
			n--
		}
		b, _ := json.Marshal(map[string]any{"type": "session.context.append", "content": []any{map[string]any{"type": "input_text", "text": text[:n]}}})
		if err := dc.SendText(string(b)); err != nil {
			return err
		}
		text = text[n:]
	}
	return nil
}
func sendDelegation(dc *webrtc.DataChannel, id, text string) error {
	b, _ := json.Marshal(map[string]any{"type": "delegation.context.append", "delegation_item_id": id, "content": []any{map[string]any{"type": "input_text", "text": text}}})
	return dc.SendText(string(b))
}

func main() {
	var db, key, u1Path, u2Path string
	flag.StringVar(&db, "db", os.Getenv("VOICE_DATABASE_FILE"), "copied database")
	flag.StringVar(&key, "key", os.Getenv("VOICE_TOKEN_ENCRYPTION_KEY"), "database key")
	flag.StringVar(&u1Path, "u1", "", "Ogg Opus U1")
	flag.StringVar(&u2Path, "u2", "", "Ogg Opus U2")
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
	sb := connectSideband(location[strings.LastIndex(location, "/")+1:], token, account.Proxy)
	if sb.conn != nil {
		defer sb.conn.Close()
		go func() {
			for {
				_ = sb.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
				_, b, err := sb.conn.ReadMessage()
				if err != nil {
					if strings.Contains(err.Error(), "i/o timeout") {
						continue
					}
					return
				}
				var v map[string]any
				if json.Unmarshal(b, &v) != nil {
					continue
				}
				t, _ := v["type"].(string)
				if t == "" {
					continue
				}
				if t == "delegation.created" {
					fmt.Printf("sideband_frame type=%s keys=item,type text=%q\n", t, truncate(findText(v), 200))
				} else if strings.Contains(t, "transcript") {
					fmt.Printf("sideband_frame type=%s text_length=%d\n", t, len(findText(v)))
				} else {
					fmt.Printf("sideband_frame type=%s\n", t)
				}
			}
		}()
	}
	stop := make(chan struct{})
	defer close(stop)
	pt := opusPT(answer)
	clock := &audioClock{}
	go sendSilence(track, pt, clock, stop)
	var u1, u2 [][]byte
	if u1Path != "" {
		u1, err = readOpusPackets(filepath.Clean(u1Path))
		if err != nil {
			log.Fatal(err)
		}
	}
	if u2Path != "" {
		u2, err = readOpusPackets(filepath.Clean(u2Path))
		if err != nil {
			log.Fatal(err)
		}
	}
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
	time.Sleep(2 * time.Second)
	if len(u1) > 0 {
		if err := sendOpus(track, pt, clock, u1, stop); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("sent_audio utterance=U1")
	time.Sleep(20 * time.Second)
	logf.Lock()
	id, txt := logf.delegationID, logf.delegationText
	logf.Unlock()
	if id != "" {
		_ = sendDelegation(dc, id, "Done: hello.html created.")
		if sb.conn != nil {
			_ = sendSideband(sb.conn, map[string]any{"type": "delegation.context.append", "delegation_item_id": id, "content": []any{map[string]any{"type": "input_text", "text": "Done: hello.html was created."}}})
		}
		fmt.Printf("delegation_created=true text=%q keys=item,type\n", txt)
		time.Sleep(20 * time.Second)
	} else {
		fmt.Println("delegation_created=false")
		_ = sendText(dc, "Reminder: the project is ORCA-7.")
		fmt.Println("sent_context_probe=true")
		time.Sleep(5 * time.Second)
	}
	if len(u2) > 0 {
		if err := sendOpus(track, pt, clock, u2, stop); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("sent_audio utterance=U2")
	time.Sleep(20 * time.Second)
	logf.Lock()
	fmt.Printf("event_types=%s assistant_text=%q audio_frames=%d\n", strings.Join(logf.types, ","), strings.Join(logf.assistant, " | "), logf.audioFrames)
	logf.Unlock()
	fmt.Println("sideband=not_attempted_not_derivable_from_codex_source")
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}
