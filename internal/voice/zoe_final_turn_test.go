package voice

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

type zoeFakeInjector struct{ events [][]byte }

func (f *zoeFakeInjector) InjectEvent(b []byte) error {
	f.events = append(f.events, append([]byte(nil), b...))
	return nil
}

type zoeFakeLogger struct{ logs []string }

func (f *zoeFakeLogger) Info(msg string, args ...any) { f.logs = append(f.logs, fmt.Sprint(msg, args)) }
func zframe(v string) []byte {
	return []byte(`{"type":"data_message","data":` + mustJSON(`{"type":"chat_message_delta","payload":{"delta":`+v+`}}`) + `}`)
}
func mustJSON(s string) string { b, _ := json.Marshal(s); return string(b) }
func zadd(id, role, status string) []byte {
	return zframe(fmt.Sprintf(`{"v":{"message":{"id":%q,"status":%q,"author":{"role":%q},"content":{"parts":[""]}}}}`, id, status, role))
}
func zops(ops string) []byte { return zframe(`{"v":[` + ops + `]}`) }
func zstatusTop() []byte {
	return zframe(`{"o":"replace","p":"/message/status","v":"finished_successfully"}`)
}
func zstatusOp() []byte {
	return zops(`{"o":"replace","p":"/message/status","v":"finished_successfully"}`)
}
func newZ() *wmFinalTurnMapper {
	f := &zoeFakeInjector{}
	return newWMFinalTurnMapper("s", &zoeFakeLogger{}, f)
}
func runZ(m *wmFinalTurnMapper, fs ...[]byte) {
	for _, f := range fs {
		m.Observe(f)
	}
}
func TestWMFinalTurnUserTurnEmitsConcatenatedText(t *testing.T) {
	in := &zoeFakeInjector{}
	m := newWMFinalTurnMapper("s", &zoeFakeLogger{}, in)
	runZ(m, zadd("U1", "user", "in_progress"), zops(`{"o":"append","p":"/message/content/parts/0/text","v":" hello"}`), zops(`{"o":"append","p":"/message/content/parts/0/text","v":" brave"}`), zops(`{"o":"append","p":"/message/content/parts/0/text","v":" world "},{"o":"replace","p":"/message/metadata/av_app_service_bidi_turn_end_time_s","v":1.5}`), zstatusTop())
	if len(in.events) != 1 {
		t.Fatalf("events=%d", len(in.events))
	}
	var got map[string]any
	json.Unmarshal(in.events[0], &got)
	if got["turn_id"] != "U1" || got["transcript"] != "hello brave world" {
		t.Fatalf("%v", got)
	}
}

func TestWMFinalTurnConsecutiveTurnsResetText(t *testing.T) {
	in := &zoeFakeInjector{}
	m := newWMFinalTurnMapper("s", &zoeFakeLogger{}, in)
	runZ(m, zadd("U1", "user", "in_progress"), zops(`{"o":"append","p":"/message/content/parts/0/text","v":"你好"}`), zstatusOp(), zadd("A1", "assistant", "finished_successfully"))
	runZ(m, zaddWithPart("U2", "今天天气", "user", "in_progress"), zops(`{"o":"append","p":"/message/content/parts/0/text","v":"怎么样"}`), zstatusOp(), zadd("A2", "assistant", "finished_successfully"))
	runZ(m, zadd("U3", "user", "in_progress"), zops(`{"o":"append","p":"/message/content/parts/0/text","v":"行,帮我总结"}`), zstatusOp())
	assertTranscripts(t, in, "你好", "今天天气怎么样", "行,帮我总结")
}

func TestWMFinalTurnAmbiguousDropResetsNextTurn(t *testing.T) {
	in := &zoeFakeInjector{}
	m := newWMFinalTurnMapper("s", &zoeFakeLogger{}, in)
	runZ(m, zadd("U1", "user", "in_progress"), zops(`{"o":"append","p":"/message/content/parts/0/text","v":"stale"}`), zadd("U2", "user", "in_progress"), zops(`{"o":"append","p":"/message/content/parts/0/text","v":"clean"}`), zstatusOp())
	assertTranscripts(t, in, "clean")
}

func TestWMFinalTurnAssistantAddCommitThenNextTurnIsClean(t *testing.T) {
	in := &zoeFakeInjector{}
	m := newWMFinalTurnMapper("s", &zoeFakeLogger{}, in)
	runZ(m, zadd("U1", "user", "in_progress"), zops(`{"o":"append","p":"/message/content/parts/0/text","v":"stale"}`), zadd("A1", "assistant", "in_progress"), zadd("U2", "user", "in_progress"), zops(`{"o":"append","p":"/message/content/parts/0/text","v":"clean"}`), zstatusOp())
	assertTranscripts(t, in, "stale", "clean")
}

func TestWMFinalTurnToolAddDropsActiveUser(t *testing.T) {
	l := &zoeFakeLogger{}
	in := &zoeFakeInjector{}
	m := newWMFinalTurnMapper("s", l, in)
	runZ(m, zadd("U", "user", "in_progress"), zops(`{"o":"append","p":"/message/content/parts/0/text","v":"text"}`), zadd("T", "tool", "in_progress"), zstatusOp())
	if len(in.events) != 0 || !strings.Contains(strings.Join(l.logs, " "), "ambiguous_interleave") {
		t.Fatal("tool add must drop the active user turn")
	}
}

func TestWMFinalTurnLateStatusAfterAssistantAddCommitDoesNotEmitAgain(t *testing.T) {
	in := &zoeFakeInjector{}
	m := newWMFinalTurnMapper("s", &zoeFakeLogger{}, in)
	runZ(m, zadd("U", "user", "in_progress"), zops(`{"o":"append","p":"/message/content/parts/0/text","v":"once"}`), zadd("A", "assistant", "in_progress"), zstatusTop(), zstatusTop())
	assertTranscripts(t, in, "once")
}

func TestWMFinalTurnEmptyDropResetsNextTurn(t *testing.T) {
	in := &zoeFakeInjector{}
	m := newWMFinalTurnMapper("s", &zoeFakeLogger{}, in)
	runZ(m, zadd("U1", "user", "in_progress"), zstatusOp(), zadd("U2", "user", "in_progress"), zops(`{"o":"append","p":"/message/content/parts/0/text","v":"clean"}`), zstatusOp())
	assertTranscripts(t, in, "clean")
}

func TestWMFinalTurnOversizedDropResetsNextTurn(t *testing.T) {
	in := &zoeFakeInjector{}
	m := newWMFinalTurnMapper("s", &zoeFakeLogger{}, in)
	runZ(m, zadd("U1", "user", "in_progress"), zops(fmt.Sprintf(`{"o":"append","p":"/message/content/parts/0/text","v":%q}`, strings.Repeat("x", zoeFinalTurnMax+1))), zstatusOp(), zadd("U2", "user", "in_progress"), zops(`{"o":"append","p":"/message/content/parts/0/text","v":"clean"}`), zstatusOp())
	assertTranscripts(t, in, "clean")
}

func zaddWithPart(id, part, role, status string) []byte {
	return []byte(fmt.Sprintf(`{"type":"data_message","data":{"type":"chat_message_delta","payload":{"delta":{"v":{"message":{"id":%q,"status":%q,"author":{"role":%q},"content":{"parts":[%q]}}}}}}}`, id, status, role, part))
}

func assertTranscripts(t *testing.T, in *zoeFakeInjector, want ...string) {
	t.Helper()
	if len(in.events) != len(want) {
		t.Fatalf("events=%d want=%d", len(in.events), len(want))
	}
	for i, b := range in.events {
		var got map[string]any
		if err := json.Unmarshal(b, &got); err != nil || got["transcript"] != want[i] {
			t.Fatalf("event %d=%v want %q", i, got, want[i])
		}
	}
}
func TestWMFinalTurnFirstFinishedThenNormalWorks(t *testing.T) {
	in := &zoeFakeInjector{}
	m := newWMFinalTurnMapper("s", &zoeFakeLogger{}, in)
	runZ(m, zadd("first", "user", "finished_successfully"), zadd("U2", "user", "in_progress"), zops(`{"o":"append","p":"/message/content/parts/0/text","v":"ok"}`), zstatusOp())
	if len(in.events) != 1 {
		t.Fatal("later turn missing")
	}
}
func TestWMFinalTurnAssistantNeverEmits(t *testing.T) {
	in := &zoeFakeInjector{}
	m := newWMFinalTurnMapper("s", &zoeFakeLogger{}, in)
	runZ(m, zadd("A", "assistant", "in_progress"), zops(`{"o":"append","p":"/message/content/parts/0/text","v":"x"}`), zstatusOp())
	if len(in.events) != 0 {
		t.Fatal("assistant emitted")
	}
}
func TestWMFinalTurnAssistantAddDropsUser(t *testing.T) {
	l := &zoeFakeLogger{}
	in := &zoeFakeInjector{}
	m := newWMFinalTurnMapper("s", l, in)
	runZ(m, zadd("U", "user", "in_progress"), zadd("A", "assistant", "in_progress"), zstatusOp())
	if len(in.events) != 0 || !strings.Contains(strings.Join(l.logs, " "), "empty") {
		t.Fatal("empty user turn not dropped")
	}
}

func TestWMFinalTurnAssistantAddCommitsUserBeforeLateStatus(t *testing.T) {
	l := &zoeFakeLogger{}
	in := &zoeFakeInjector{}
	m := newWMFinalTurnMapper("s", l, in)
	runZ(m, zaddWithPart("U", "帮我", "user", "in_progress"),
		zops(`{"o":"append","p":"/message/content/parts/0/text","v":"研究"}`),
		zops(`{"o":"append","p":"/message/content/parts/0/text","v":"一下"}`),
		zops(`{"o":"append","p":"/message/content/parts/0/text","v":"今天的"}`),
		zops(`{"o":"append","p":"/message/content/parts/0/text","v":"天气"}`),
		zadd("A", "assistant", "in_progress"), zstatusTop())
	if len(in.events) != 1 {
		t.Fatalf("events=%d", len(in.events))
	}
	var got map[string]any
	json.Unmarshal(in.events[0], &got)
	if got["transcript"] != "帮我研究一下今天的天气" {
		t.Fatalf("%v", got)
	}
	if !strings.Contains(strings.Join(l.logs, " "), "commit_reason assistant_add") {
		t.Fatal("assistant-add commit reason missing")
	}
}

func TestWMFinalTurnAssistantAddWithEmptyUserTextDrops(t *testing.T) {
	l := &zoeFakeLogger{}
	m := newWMFinalTurnMapper("s", l, &zoeFakeInjector{})
	runZ(m, zadd("U", "user", "in_progress"), zadd("A", "assistant", "in_progress"))
	if !strings.Contains(strings.Join(l.logs, " "), "empty") {
		t.Fatal("empty user text not dropped")
	}
}

func TestWMFinalTurnMixedAssistantAndStatusOrdering(t *testing.T) {
	in := &zoeFakeInjector{}
	m := newWMFinalTurnMapper("s", &zoeFakeLogger{}, in)
	runZ(m, zaddWithPart("U1", "first", "user", "in_progress"), zadd("A1", "assistant", "in_progress"), zstatusTop(),
		zaddWithPart("U2", "second", "user", "in_progress"), zstatusTop())
	assertTranscripts(t, in, "first", "second")
}
func TestWMFinalTurnAssistantStatusDoesNotFinishUser(t *testing.T) {
	in := &zoeFakeInjector{}
	m := newWMFinalTurnMapper("s", &zoeFakeLogger{}, in)
	runZ(m, zadd("U", "user", "in_progress"), zadd("A", "assistant", "in_progress"), zstatusOp())
	if len(in.events) != 0 {
		t.Fatal("empty user turn emitted")
	}
}
func TestWMFinalTurnDuplicateStatusEmitsOnce(t *testing.T) {
	in := &zoeFakeInjector{}
	m := newWMFinalTurnMapper("s", &zoeFakeLogger{}, in)
	runZ(m, zadd("U", "user", "in_progress"), zops(`{"o":"append","p":"/message/content/parts/0/text","v":"x"}`), zstatusOp(), zstatusOp())
	if len(in.events) != 1 {
		t.Fatal("duplicate emitted")
	}
}
func TestWMFinalTurnEmptyTextDropped(t *testing.T) {
	l := &zoeFakeLogger{}
	m := newWMFinalTurnMapper("s", l, &zoeFakeInjector{})
	runZ(m, zadd("U", "user", "in_progress"), zstatusOp())
	if !strings.Contains(strings.Join(l.logs, " "), "empty") {
		t.Fatal("empty not dropped")
	}
}
func TestWMFinalTurnOversizedDropped(t *testing.T) {
	l := &zoeFakeLogger{}
	m := newWMFinalTurnMapper("s", l, &zoeFakeInjector{})
	runZ(m, zadd("U", "user", "in_progress"), zops(fmt.Sprintf(`{"o":"append","p":"/message/content/parts/0/text","v":%q}`, strings.Repeat("x", 20001))), zstatusOp())
	if !strings.Contains(strings.Join(l.logs, " "), "oversized") {
		t.Fatal("oversized not dropped")
	}
}
func TestWMFinalTurnNoiseDoesNotBreakTurn(t *testing.T) {
	in := &zoeFakeInjector{}
	m := newWMFinalTurnMapper("s", &zoeFakeLogger{}, in)
	runZ(m, zadd("U", "user", "in_progress"), zframe(`{"type":"url_moderation"}`), zframe(`{"type":"usage_update"}`), zframe(`{"type":"conversation_update"}`), zframe(`{"type":"spawn_update"}`), zframe(`{"type":"thinking_update"}`), zops(`{"o":"append","p":"/message/content/parts/0/text","v":"x"}`), zstatusOp())
	if len(in.events) != 1 {
		t.Fatal("noise broke turn")
	}
}
func TestWMFinalTurnMalformedAndOversizedIgnored(t *testing.T) {
	m := newZ()
	runZ(m, []byte("bad"), []byte(`{"type":"other"}`), []byte(strings.Repeat("x", 1<<20+1)))
	if m.active {
		t.Fatal("malformed changed state")
	}
}
func TestWMFinalTurnRingEvictsFIFO(t *testing.T) {
	in := &zoeFakeInjector{}
	m := newWMFinalTurnMapper("s", &zoeFakeLogger{}, in)
	for i := 0; i < 300; i++ {
		id := fmt.Sprintf("%03d", i)
		runZ(m, zadd(id, "user", "in_progress"), zops(fmt.Sprintf(`{"o":"append","p":"/message/content/parts/0/text","v":%q}`, "x")), zstatusOp())
	}
	n := len(in.events)
	runZ(m, zadd("000", "user", "in_progress"), zops(`{"o":"append","p":"/message/content/parts/0/text","v":"x"}`), zstatusOp())
	if len(in.events) != n+1 {
		t.Fatal("old id did not evict")
	}
	runZ(m, zadd("299", "user", "in_progress"), zops(`{"o":"append","p":"/message/content/parts/0/text","v":"x"}`), zstatusOp())
	if len(in.events) != n+1 {
		t.Fatal("latest id evicted")
	}
}
func TestWMFinalTurnLogsDoNotContainTranscriptOrID(t *testing.T) {
	l := &zoeFakeLogger{}
	runZ(newWMFinalTurnMapper("s", l, &zoeFakeInjector{}), zadd("SECRET-ID", "user", "in_progress"), zops(`{"o":"append","p":"/message/content/parts/0/text","v":"SECRET-TEXT"}`), zstatusOp())
	if strings.Contains(strings.Join(l.logs, " "), "SECRET") {
		t.Fatal("sensitive data logged")
	}
}
