package voice

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/config"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/httpclient"
)

type forwardedTurn struct {
	Seq          int64  `json:"seq"`
	Role         string `json:"role"`
	Text         string `json:"text"`
	At           string `json:"at"`
	DelegationID string `json:"delegation_id,omitempty"`
}

type markedTurn struct {
	turn     forwardedTurn
	deadline time.Time
	reject   bool
}

type transcriptBatch struct {
	Session    string          `json:"gateway_session_id"`
	Generation int64           `json:"media_generation"`
	Turns      []forwardedTurn `json:"turns"`
}

// transcriptForward owns only memory. The observer performs parsing and bounded
// enqueueing; only run performs HTTP, waits or logging. No text or credentials
// are ever logged, even when other gateway debug switches are enabled.
type transcriptForward struct {
	mu                                             sync.Mutex
	queue                                          []forwardedTurn
	cur                                            transcriptTurn
	userCompleted                                  bool
	userText                                       string
	userID                                         string
	userTurn                                       *forwardedTurn
	userMarked                                     bool
	marked                                         []markedTurn
	markedInFlight                                 int
	markedWake                                     chan struct{}
	delegations                                    map[string]bool
	delegationIDs                                  []string
	markedWindow                                   time.Duration
	failed                                         func(string)
	boundOnly                                      bool
	seq                                            int64
	seen                                           map[string]bool
	seenIDs                                        []string
	closed                                         bool
	dropped                                        int
	capacity                                       int
	batch                                          transcriptBatch
	url, token                                     string
	client                                         *http.Client
	logger                                         *slog.Logger
	wake                                           chan struct{}
	done                                           chan struct{}
	ctx                                            context.Context
	cancel                                         context.CancelFunc
	interval, retryBase, flushDeadline, bindWindow time.Duration
	closeTimer                                     *time.Timer
}

func newTranscriptForward(cfg config.Config, token, session string, generation int64, logger *slog.Logger) *transcriptForward {
	if cfg.ZoeURL == "" || token == "" || generation < 1 {
		return nil
	}
	client := httpclient.New(httpclient.Options{Transport: config.TransportGo, Environment: config.EnvironmentProduction}, "")
	client.Timeout = 15 * time.Second
	// Never leak the Authorization header to a redirect destination.
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	ctx, cancel := context.WithCancel(context.Background())
	return &transcriptForward{capacity: 1000, batch: transcriptBatch{Session: session, Generation: generation},
		url: strings.TrimRight(cfg.ZoeURL, "/") + "/runner/v1/voice/turns", token: token, client: client, logger: logger,
		seen: map[string]bool{}, wake: make(chan struct{}, 1), done: make(chan struct{}), ctx: ctx, cancel: cancel,
		markedWake: make(chan struct{}, 1), markedWindow: shortRetryWindow, delegations: map[string]bool{},
		interval: 500 * time.Millisecond, retryBase: time.Second, flushDeadline: 60 * time.Second, bindWindow: 2 * time.Minute}
}

func (f *transcriptForward) signal() {
	select {
	case f.wake <- struct{}{}:
	default:
	}
}

// Close is asynchronous: hangup never waits for the destination. Pending turns
// get at most 60 seconds, including a currently blocked HTTP request.
func (f *transcriptForward) Close() {
	if f == nil {
		return
	}
	f.mu.Lock()
	if !f.closed {
		f.flushLocked()
		f.closed = true
		f.closeTimer = time.AfterFunc(f.flushDeadline, f.cancel)
	}
	f.mu.Unlock()
	f.signal()
}

func (f *transcriptForward) abort() {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closeTimer != nil {
		f.closeTimer.Stop()
	}
	f.cancel()
	f.client.CloseIdleConnections()
}

func (f *transcriptForward) emitLocked(role, id, text string) {
	if (role != "user" && role != "assistant") || strings.TrimSpace(text) == "" {
		return
	}
	key := role + ":" + id
	if id != "" && f.seen[key] {
		return
	}
	if id != "" {
		f.seen[key] = true
		f.seenIDs = append(f.seenIDs, key)
		if len(f.seenIDs) > 2048 {
			delete(f.seen, f.seenIDs[0])
			f.seenIDs = f.seenIDs[1:]
		}
	}
	text = boundedTurnText(text)
	t := forwardedTurn{Seq: f.seq, Role: role, Text: text, At: time.Now().UTC().Format(time.RFC3339Nano)}
	f.seq++
	if role == "user" {
		f.userTurn = &t
		f.userMarked = false
	}
	f.enqueueLocked(t)
}

func (f *transcriptForward) enqueueLocked(t forwardedTurn) {
	if len(f.queue) >= f.capacity {
		f.queue[0] = forwardedTurn{}
		f.queue = f.queue[1:]
		f.dropped++
	}
	f.queue = append(f.queue, t)
	f.signal()
}

func (f *transcriptForward) markLocked(id, text string) {
	if id == "" || strings.TrimSpace(text) == "" || f.delegations[id] {
		return
	}
	f.delegations[id] = true
	f.delegationIDs = append(f.delegationIDs, id)
	if len(f.delegationIDs) > 2048 {
		delete(f.delegations, f.delegationIDs[0])
		f.delegationIDs = f.delegationIDs[1:]
	}
	if f.cur.Role == "assistant" {
		f.flushLocked()
	}
	if f.userTurn != nil && f.userMarked && f.cur.Role != "user" {
		// Some AVAS sessions deliver delegation items without turn.created.
		// A new item after an already correlated user turn starts a new turn.
		f.userTurn = nil
	}
	turnID := f.cur.ID
	if turnID == "" {
		turnID = id
	}
	f.cur = transcriptTurn{}
	if f.userTurn == nil {
		f.emitLocked("user", turnID, text)
	}
	if f.userTurn == nil {
		return
	}
	f.userMarked = true
	f.userCompleted, f.userText, f.userID = true, f.userTurn.Text, turnID
	turn := *f.userTurn
	// A mark always uses the original sequence. A different transcript may
	// already be committed or in flight, so don't execute mismatched text.
	reject := utf8.RuneCountInString(text) > zoeFinalTurnMax || len(text) > 64<<10 || utf8.RuneCountInString(id) > 256 || turn.Text != text
	for i, queued := range f.queue {
		if queued.Seq == turn.Seq {
			copy(f.queue[i:], f.queue[i+1:])
			f.queue[len(f.queue)-1] = forwardedTurn{}
			f.queue = f.queue[:len(f.queue)-1]
			break
		}
	}
	turn.DelegationID = id
	f.marked = append(f.marked, markedTurn{turn, time.Now().Add(f.markedWindow), reject})
	select {
	case f.markedWake <- struct{}{}:
	default:
	}
}

func (f *transcriptForward) flushLocked() {
	f.emitLocked(f.cur.Role, f.cur.ID, f.cur.Text)
	f.cur = transcriptTurn{}
}

// turnText handles AVAS transcript strings and typed content parts, without
// treating arbitrary event metadata as conversation text.
func turnText(raw json.RawMessage) string {
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return text
	}
	var parts []json.RawMessage
	if json.Unmarshal(raw, &parts) == nil {
		var b strings.Builder
		for _, p := range parts {
			b.WriteString(turnText(p))
		}
		return b.String()
	}
	var obj map[string]json.RawMessage
	if json.Unmarshal(raw, &obj) == nil {
		for _, key := range []string{"transcript", "text", "content", "parts"} {
			if s := turnText(obj[key]); s != "" {
				return s
			}
		}
	}
	return ""
}

func (f *transcriptForward) Observe(data []byte) {
	if f == nil {
		return
	}
	var outer struct {
		Type string          `json:"type"`
		Data json.RawMessage `json:"data"`
	}
	if json.Unmarshal(data, &outer) != nil {
		return
	}
	in := json.RawMessage(data)
	if outer.Type == "data_message" {
		in = outer.Data
		var s string
		if json.Unmarshal(in, &s) == nil {
			in = json.RawMessage(s)
		}
	}
	var e struct {
		Type string `json:"type"`
		Turn struct {
			ID, Role                  string
			Transcript, Content, Text json.RawMessage
		} `json:"turn"`
		Item struct {
			ID, Target string
			Content    json.RawMessage
		} `json:"item"`
		Delta   json.RawMessage `json:"delta"`
		Payload struct {
			Delta struct {
				V json.RawMessage `json:"v"`
			} `json:"delta"`
		} `json:"payload"`
	}
	if json.Unmarshal(in, &e) != nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return
	}
	switch e.Type {
	case "turn.created":
		if e.Turn.Role == "user" && e.Turn.ID != "" && (f.cur.Role == "user" && f.cur.ID == e.Turn.ID || f.seen["user:"+e.Turn.ID]) {
			return
		}
		f.flushLocked()
		if e.Turn.Role == "user" {
			f.userCompleted, f.userText, f.userID = false, "", ""
			f.userTurn = nil
		}
		text := turnText(e.Turn.Transcript)
		if text == "" {
			text = turnText(e.Turn.Content)
		}
		if text == "" {
			text = turnText(e.Turn.Text)
		}
		f.cur = transcriptTurn{ID: e.Turn.ID, Role: e.Turn.Role, Text: boundedTurnText(text)}
	case "turn.done":
		text := turnText(e.Turn.Transcript)
		if text == "" {
			text = turnText(e.Turn.Content)
		}
		if text == "" {
			text = turnText(e.Turn.Text)
		}
		role, id := e.Turn.Role, e.Turn.ID
		if role == "" {
			role = f.cur.Role
		}
		if id == "" {
			id = f.cur.ID
		}
		if id != "" && f.seen[role+":"+id] {
			return
		}
		if text == "" {
			text = f.cur.Text
		}
		if f.cur.Role != "" && (f.cur.Role != role || (id != "" && f.cur.ID != "" && f.cur.ID != id)) {
			f.flushLocked()
		}
		f.cur = transcriptTurn{}
		if role == "user" {
			if f.userCompleted && f.userText == boundedTurnText(text) && (id == "" || f.userID == "" || id == f.userID) {
				return
			}
			f.userCompleted, f.userText, f.userID = true, boundedTurnText(text), id
		}
		f.emitLocked(role, id, text)
	case "delegation.created":
		if e.Item.Target == "client" {
			f.markLocked(e.Item.ID, delegationText(e.Item.Content))
		}
	case "turn.text.delta", "output_text.delta", "output_audio_transcript.delta":
		if f.cur.Role != "" {
			f.cur.Text = boundedTurnText(f.cur.Text + turnText(e.Delta))
		}
	case "chat_message_delta":
		f.observeLegacyLocked(e.Payload.Delta.V)
	}
}

// When a turn exceeds Zoe's byte limit, retain only its complete UTF-8 prefix.
// No suffix is appended: even an ellipsis could exceed the wire contract.
func boundedTurnText(s string) string {
	if len(s) > 64<<10 {
		end := 64 << 10
		for end > 0 && !utf8.RuneStart(s[end]) {
			end--
		}
		return s[:end]
	}
	return s
}

func (f *transcriptForward) observeLegacyLocked(raw json.RawMessage) {
	var v struct {
		Message *struct {
			ID     string `json:"id"`
			Author struct {
				Role string `json:"role"`
			} `json:"author"`
			Content struct {
				Parts []json.RawMessage `json:"parts"`
			} `json:"content"`
		} `json:"message"`
	}
	if json.Unmarshal(raw, &v) == nil && v.Message != nil {
		m := v.Message
		role := m.Author.Role
		var parts []string
		for _, p := range m.Content.Parts {
			// Direction inference matches transcriptTail's audio transcriptions.
			var meta struct {
				ContentType string `json:"content_type"`
				Direction   string `json:"direction"`
			}
			_ = json.Unmarshal(p, &meta)
			if role != "user" && role != "assistant" && meta.ContentType == "audio_transcription" {
				if meta.Direction == "in" {
					role = "user"
				}
				if meta.Direction == "out" {
					role = "assistant"
				}
			}
			text := turnText(p)
			parts = append(parts, text)
		}
		if role != "user" && role != "assistant" {
			return
		}
		if f.cur.ID != m.ID || f.cur.Role != role {
			f.flushLocked()
		}
		f.cur = transcriptTurn{ID: m.ID, Role: role, Text: boundedTurnText(strings.Join(parts, "\n"))}
		return
	}
	var ops []struct {
		O, P string
		V    json.RawMessage
	}
	if json.Unmarshal(raw, &ops) == nil {
		for _, op := range ops {
			if op.O == "append" && appendTextPath.MatchString(op.P) {
				f.cur.Text = boundedTurnText(f.cur.Text + turnText(op.V))
			}
		}
	}
}

func (f *transcriptForward) run() {
	defer close(f.done)
	defer f.cancel()
	defer func() {
		f.mu.Lock()
		if f.closeTimer != nil {
			f.closeTimer.Stop()
		}
		f.mu.Unlock()
		f.client.CloseIdleConnections()
		f.mu.Lock()
		pending := len(f.queue)
		f.mu.Unlock()
		if pending > 0 {
			f.logger.Warn("zoe_transcript_flush_expired", "pending", pending)
		}
	}()
	ticker := time.NewTicker(f.interval)
	defer ticker.Stop()
	markedDone := make(chan struct{})
	go func() { defer close(markedDone); f.runMarked() }()
	defer func() { f.cancel(); <-markedDone }()
	var next time.Time
	var first404 time.Time
	var authLogged time.Time
	backoff := f.retryBase
	single := false
	for {
		select {
		case <-f.ctx.Done():
			return
		case <-f.wake:
		case <-ticker.C:
		}
		f.mu.Lock()
		dropped := f.dropped
		f.dropped = 0
		n, closed, marked := len(f.queue), f.closed, len(f.marked)+f.markedInFlight
		f.mu.Unlock()
		if dropped > 0 {
			f.logger.Warn("zoe_transcript_overflow", "dropped", dropped)
		}
		if n == 0 {
			if closed && marked == 0 {
				return
			}
			continue
		}
		if time.Now().Before(next) {
			continue
		}
		if !closed && n < 20 {
			// Enqueue notifications do not bypass the 500 ms batch window.
			select {
			case <-ticker.C:
			case <-f.ctx.Done():
				return
			}
		}
		f.mu.Lock()
		batch := f.batch
		limit := 20
		if single {
			limit = 1
		}
		if len(f.queue) < limit {
			limit = len(f.queue)
		}
		candidates := append([]forwardedTurn(nil), f.queue[:limit]...)
		f.mu.Unlock()
		batch.Turns = nil
		for _, turn := range candidates {
			if len(turn.Text) > 64<<10 {
				if len(batch.Turns) == 0 {
					batch.Turns = append(batch.Turns, turn)
				}
				break
			}
			batch.Turns = append(batch.Turns, turn)
			encoded, _ := json.Marshal(batch)
			if len(encoded) > 512<<10 {
				batch.Turns = batch.Turns[:len(batch.Turns)-1]
				break
			}
		}
		if len(batch.Turns) == 0 {
			continue
		}
		status := http.StatusRequestEntityTooLarge
		valid := true
		for _, turn := range batch.Turns {
			if len(turn.Text) > 64<<10 {
				valid = false
			}
		}
		if valid {
			status = f.post(f.ctx, batch)
		}
		// Isolate bad turns instead of dropping innocent members of a rejected batch.
		if (status == 400 || status == 413) && len(batch.Turns) > 1 {
			single = true
			f.signal()
			continue
		}
		drop := status == 200 || status == 400 || status == 413 || status == 409 || (status >= 300 && status < 400)
		if status == 404 {
			if first404.IsZero() {
				first404 = time.Now()
			}
			drop = f.boundOnly || time.Since(first404) >= f.bindWindow
		}
		if status == 401 || status == 403 {
			if authLogged.IsZero() || time.Since(authLogged) >= time.Minute {
				f.logger.Warn("zoe_transcript_auth", "status", status)
				authLogged = time.Now()
			}
		} else if drop && status != 200 {
			for _, turn := range batch.Turns {
				f.logger.Warn("zoe_transcript_dropped", "seq", turn.Seq, "status", status)
			}
		}
		if drop {
			removed := make(map[int64]bool, len(batch.Turns))
			for _, turn := range batch.Turns {
				removed[turn.Seq] = true
			}
			f.mu.Lock()
			f.queue = removeForwarded(f.queue, removed)
			f.mu.Unlock()
			backoff = f.retryBase
			next = time.Time{}
			first404 = time.Time{}
			f.signal()
		} else {
			next = time.Now().Add(backoff)
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
}

func removeForwarded(queue []forwardedTurn, removed map[int64]bool) []forwardedTurn {
	var n int
	for _, turn := range queue {
		if !removed[turn.Seq] {
			queue[n] = turn
			n++
		}
	}
	clear(queue[n:])
	return queue[:n]
}

func (f *transcriptForward) post(ctx context.Context, batch transcriptBatch) int {
	body, _ := json.Marshal(batch)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.url, bytes.NewReader(body))
	if err != nil {
		return -1
	}
	req.Header.Set("Authorization", "Bearer "+f.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := f.client.Do(req)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	return resp.StatusCode
}

// Marked work bypasses ordinary batching/backoff and stays in a separate
// overflow-exempt lane, including during its in-flight retry window.
func (f *transcriptForward) runMarked() {
	var active sync.WaitGroup
	defer active.Wait()
	for {
		if f.ctx.Err() != nil {
			return
		}
		f.mu.Lock()
		if len(f.marked) == 0 {
			f.mu.Unlock()
			select {
			case <-f.ctx.Done():
				return
			case <-f.markedWake:
				continue
			}
		}
		marked := f.marked
		f.marked = nil
		f.markedInFlight += len(marked)
		f.mu.Unlock()
		for _, m := range marked {
			active.Go(func() { f.deliverMarked(m) })
		}
	}
}

func (f *transcriptForward) deliverMarked(m markedTurn) {
	status := 0
	if !m.reject {
		status = shortRetry(f.ctx, m.deadline, func(ctx context.Context) int {
			ctx, cancel := context.WithTimeout(ctx, zoeRequestTimeout)
			defer cancel()
			b := f.batch
			b.Turns = []forwardedTurn{m.turn}
			return f.post(ctx, b)
		})
	}
	if status != 200 {
		f.logger.Error("zoe_delegation_handoff_failed", "seq", m.turn.Seq, "status", status)
		if f.failed != nil {
			f.failed(m.turn.DelegationID)
		}
	}
	f.mu.Lock()
	f.markedInFlight--
	if status != 200 {
		m.turn.DelegationID = ""
		f.enqueueLocked(m.turn)
	}
	f.mu.Unlock()
	f.signal()
}
