package voice

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/voice/eventlog"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/config"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/mediaevidence"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/store"
)

type resultMedia struct {
	*fakeMedia
	sends     atomic.Int32
	attempted bool
	err       error
	entered   chan struct{}
	proceed   chan struct{}
}

func (m *resultMedia) SendResultText(string) (bool, error) {
	m.sends.Add(1)
	if m.entered != nil {
		close(m.entered)
		<-m.proceed
	}
	return m.attempted, m.err
}

func TestResultReceiptsGenerationReplacementWaitsForDispatch(t *testing.T) {
	s, _, m, p := resultFixture(t)
	m.entered = make(chan struct{})
	m.proceed = make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err := s.SubmitResultDelivery("api_key:1", "s", p)
		if err != nil {
			t.Error(err)
		}
	}()
	<-m.entered
	replacing := make(chan struct{})
	replaced := make(chan struct{})
	go func() {
		close(replacing)
		s.mu.Lock()
		s.bindings["s"] = &sessionBinding{Owner: "api_key:1", mediaID: mediaevidence.Identity{MediaGeneration: 2}}
		s.mu.Unlock()
		close(replaced)
	}()
	<-replacing
	select {
	case <-replaced:
		t.Fatal("replacement crossed active dispatch")
	default:
	}
	close(m.proceed)
	<-done
	<-replaced
	if m.sends.Load() != 1 {
		t.Fatal(m.sends.Load())
	}
	if _, err := s.SubmitResultDelivery("api_key:1", "s", p); err != nil {
		t.Fatal("old receipt must reconcile", err)
	}
	p.DeliveryID = "new-delivery"
	if _, err := s.SubmitResultDelivery("api_key:1", "s", p); err == nil {
		t.Fatal("stale generation sent")
	}
}
func resultFixture(t *testing.T) (*Service, *store.DB, *resultMedia, ResultDeliveryRequest) {
	t.Helper()
	db, err := store.Open(filepath.Join(t.TempDir(), "gateway.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	s := New(config.Config{VoiceUpstreamRoute: "avas"}, nil, nil).WithResultReceipts(db)
	m := &resultMedia{fakeMedia: &fakeMedia{}, attempted: true}
	s.bindings["s"] = &sessionBinding{Owner: "api_key:1", route: "wm", media: m, mediaID: mediaevidence.Identity{MediaGeneration: 1}}
	text := "hello result"
	hash := sha256.Sum256([]byte(text))
	return s, db, m, ResultDeliveryRequest{MediaGeneration: 1, DeliveryID: "delivery", ContentHash: hex.EncodeToString(hash[:]), Text: text}
}

func TestResultReceiptsConcurrentAndConflict(t *testing.T) {
	s, _, m, p := resultFixture(t)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := s.SubmitResultDelivery("api_key:1", "s", p)
			if err != nil || r.State != "applied" {
				t.Errorf("receipt=%+v err=%v", r, err)
			}
		}()
	}
	wg.Wait()
	if m.sends.Load() != 1 {
		t.Fatalf("sends=%d", m.sends.Load())
	}
	p.Text = "changed"
	h := sha256.Sum256([]byte(p.Text))
	p.ContentHash = hex.EncodeToString(h[:])
	if _, err := s.SubmitResultDelivery("api_key:1", "s", p); err == nil {
		t.Fatal("expected conflict")
	}
}
func TestResultReceiptsOwnerGenerationAndEndedLookup(t *testing.T) {
	s, db, m, p := resultFixture(t)
	if _, err := s.SubmitResultDelivery("api_key:2", "s", p); err == nil {
		t.Fatal("cross owner")
	}
	p.MediaGeneration = 2
	if _, err := s.SubmitResultDelivery("api_key:1", "s", p); err == nil {
		t.Fatal("cross generation")
	}
	p.MediaGeneration = 1
	if _, err := s.SubmitResultDelivery("api_key:1", "s", p); err != nil {
		t.Fatal(err)
	}
	delete(s.bindings, "s")
	for i := 0; i < 3; i++ {
		r, err := s.LookupResultDelivery("api_key:1", "s", 1, p.DeliveryID)
		if err != nil || r.State != "applied" {
			t.Fatal(r, err)
		}
	}
	if _, err := s.LookupResultDelivery("api_key:2", "s", 1, p.DeliveryID); err == nil {
		t.Fatal("cross owner lookup")
	}
	if m.sends.Load() != 1 {
		t.Fatal("GET sent")
	}
	var state string
	db.Conn().QueryRow(`SELECT state FROM voice_result_receipts`).Scan(&state)
	if state != "applied" {
		t.Fatal(state)
	}
}
func TestResultReceiptsTransportFailure(t *testing.T) {
	for _, attempted := range []bool{false, true} {
		t.Run(map[bool]string{false: "before", true: "partial"}[attempted], func(t *testing.T) {
			s, _, m, p := resultFixture(t)
			m.attempted = attempted
			m.err = errors.New("transport failed")
			r, err := s.SubmitResultDelivery("api_key:1", "s", p)
			want := "rejected"
			if attempted {
				want = "unknown"
			}
			if err != nil || r.State != want {
				t.Fatal(r, err)
			}
			s.SubmitResultDelivery("api_key:1", "s", p)
			if m.sends.Load() != 1 {
				t.Fatal("resent")
			}
		})
	}
}
func TestResultReceiptsCrashRecovery(t *testing.T) {
	for _, claim := range []bool{false, true} {
		t.Run(map[bool]string{false: "pending", true: "inflight"}[claim], func(t *testing.T) {
			s, db, m, p := resultFixture(t)
			r := ResultReceipt{MediaGeneration: 1, DeliveryID: p.DeliveryID, ContentHash: p.ContentHash}
			if _, err := db.InsertResultReceipt("api_key:1", "s", r, p.Text); err != nil {
				t.Fatal(err)
			}
			if claim {
				db.ClaimResultReceipt("api_key:1", "s", r, "abandoned")
			}
			if err := db.RecoverResultReceipts(); err != nil {
				t.Fatal(err)
			}
			got, err := s.SubmitResultDelivery("api_key:1", "s", p)
			if err != nil || got.State != "unknown" || m.sends.Load() != 0 {
				t.Fatal(got, err, m.sends.Load())
			}
		})
	}
}

type failResultFinish struct{ *store.DB }

func (f failResultFinish) FinishResultReceipt(string, string, ResultReceipt, string, string) error {
	return errors.New("disk failure")
}
func TestResultReceiptsPostWritePersistenceFailure(t *testing.T) {
	s, db, m, p := resultFixture(t)
	s.resultReceipts = failResultFinish{db}
	r, err := s.SubmitResultDelivery("api_key:1", "s", p)
	if err == nil || r.State != "unknown" {
		t.Fatal(r, err)
	}
	r, err = s.LookupResultDelivery("api_key:1", "s", 1, p.DeliveryID)
	if err != nil || r.State != "unknown" {
		t.Fatal(r, err)
	}
	s.SubmitResultDelivery("api_key:1", "s", p)
	if m.sends.Load() != 1 {
		t.Fatal("resent")
	}
}
func TestResultAVASPartial(t *testing.T) {
	for _, failure := range []int{1, 2} {
		calls := 0
		sender := &resultAVASSender{write: func([]byte) (bool, error) {
			calls++
			if calls == failure {
				return false, errors.New("closed before write")
			}
			return true, nil
		}}
		err := sendAVASContext(sender, "", strings.Repeat("a", 1001))
		if err == nil || sender.attempted != (failure == 2) || calls != failure {
			t.Fatal(calls, sender.attempted, err)
		}
	}
}

type sourceKindSocket struct{ frames [][]byte }

func (*sourceKindSocket) SetWriteDeadline(time.Time) error { return nil }
func (s *sourceKindSocket) WriteMessage(_ int, b []byte) error {
	s.frames = append(s.frames, append([]byte(nil), b...))
	return nil
}
func (*sourceKindSocket) Close() error { return nil }

func TestResultSourceKindCapture(t *testing.T) {
	for _, delegation := range []string{"", "delegation"} {
		for _, kind := range []string{"", "user_text", "plan_note", "Bad", strings.Repeat("a", 33), "a-b", "1bad"} {
			t.Run(delegation+"/"+kind, func(t *testing.T) {
				s, _, _, p := resultFixture(t)
				p.Text = strings.Repeat("x", 1001)
				h := sha256.Sum256([]byte(p.Text))
				p.ContentHash = hex.EncodeToString(h[:])
				p.SourceKind = kind
				log := eventlog.New(eventlog.Defaults())
				key := eventlog.Key{SessionID: "s", Generation: 1}
				log.Start(key, "")
				sock := &sourceKindSocket{}
				w := &avasSidebandWriter{conn: sock, capture: func(data []byte, delivery, sourceKind string) {
					log.AppendWithSourceKind(key, "gateway_to_upstream", "avas_sideband", delivery, data, false, sourceKind)
				}}
				b := s.bindings["s"]
				b.route = "avas"
				b.avasSideband = w
				b.openDelegation = delegation
				r, err := s.SubmitResultDelivery("api_key:1", "s", p)
				if err != nil || r.State != "applied" {
					t.Fatal(r, err)
				}
				p.SourceKind = "different"
				if _, err := s.SubmitResultDelivery("api_key:1", "s", p); err != nil {
					t.Fatal(err)
				}
				snap, _ := log.Snapshot(key, 0, 10)
				if len(snap.Events) != 3 || len(sock.frames) != 3 {
					t.Fatal(snap.Events, len(sock.frames))
				}
				expected := sanitizeSourceKind(kind)
				for _, event := range snap.Events {
					typ := "session.context.append"
					if delegation != "" {
						typ = "delegation.context.append"
					}
					if event.SourceKind != expected || event.Type != typ {
						t.Fatal(event)
					}
					raw, _ := json.Marshal(event)
					if bytes.Contains(raw, []byte(`"source_kind"`)) != (expected != "") {
						t.Fatal(string(raw))
					}
				}
				baseline := &sourceKindSocket{}
				if err := sendAVASContext(&avasSidebandWriter{conn: baseline}, delegation, p.Text); err != nil {
					t.Fatal(err)
				}
				for i := range sock.frames {
					if !bytes.Equal(sock.frames[i], baseline.frames[i]) {
						t.Fatal("upstream frame changed")
					}
				}
			})
		}
	}
}
