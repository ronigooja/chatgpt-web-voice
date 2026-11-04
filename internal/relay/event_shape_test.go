package relay

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestEventShapeSamplerLogsStructureWithoutTranscriptOrIDs(t *testing.T) {
	var logs bytes.Buffer
	s := newEventShapeSampler(slog.New(slog.NewTextHandler(&logs, nil)), "media-1", "wm", 4)
	s.setMetadata("voice-1", "wm")
	secret := "DISTINCTIVE_SECRET_TRANSCRIPT_NEVER_LOGGED"
	s.observe([]byte(`{"type":"data_message","data":"{\"type\":\"chat_message_delta\",\"payload\":{\"delta\":{\"v\":{\"message\":{\"id\":\"message-id-never-log\",\"author\":{\"role\":\"user\"},\"content\":{\"parts\":[{"\"text\":\"`+secret+`\"}]}}}}}}"}`), "datachannel", "b-to-a")
	s.close()
	got := logs.String()
	if !strings.Contains(got, "oai_event_shape") || !strings.Contains(got, "route=wm") {
		t.Fatalf("missing shape log: %s", got)
	}
	if strings.Contains(got, secret) || strings.Contains(got, "message-id-never-log") {
		t.Fatalf("sensitive value disclosed: %s", got)
	}
}

func TestEventShapeSamplerLabelsAVASSideband(t *testing.T) {
	var logs bytes.Buffer
	s := newEventShapeSampler(slog.New(slog.NewTextHandler(&logs, nil)), "media-2", "avas", 2)
	s.setMetadata("voice-2", "avas")
	s.observe([]byte(`{"type":"data_message","data":"{\"type\":\"usage\",\"status\":\"complete\"}"}`), "sideband", "b-to-a")
	s.close()
	got := logs.String()
	if !strings.Contains(got, "route=avas") || !strings.Contains(got, "source=sideband") {
		t.Fatalf("missing AVAS labels: %s", got)
	}
}

func TestEventShapeSamplerHandlesMalformedOversizedAndCap(t *testing.T) {
	var logs bytes.Buffer
	s := newEventShapeSampler(slog.New(slog.NewTextHandler(&logs, nil)), "media-3", "wm", 1)
	s.observe([]byte("{"), "datachannel", "b-to-a")
	s.observe(bytes.Repeat([]byte("x"), maxEventShapeParseBytes+1), "datachannel", "b-to-a")
	s.observe([]byte(`{"type":"one"}`), "datachannel", "b-to-a")
	s.close()
	if strings.Count(logs.String(), "oai_event_shape ") != 1 {
		t.Fatalf("frame cap not applied: %s", logs.String())
	}
}

func TestEventShapeSamplerUsesBoundedPathsAndPatchNormalization(t *testing.T) {
	paths, finalish := collectShapePaths(map[string]any{"o": "append", "p": "/message/content/parts/12/text", "final": true, "text": "secret"})
	if !finalish || len(paths) == 0 || !strings.Contains(strings.Join(paths, " "), "p=string=/message/content/parts/##/text") {
		t.Fatalf("unexpected paths: %#v", paths)
	}
}

func TestEventShapeSamplerRecognizesRealisticDeltaEnvelope(t *testing.T) {
	outer := []byte(`{"type":"data_message","data":"{\"type\":\"chat_message_delta\",\"payload\":{\"delta\":{\"v\":\"[{\\\"o\\\":\\\"append\\\",\\\"p\\\":\\\"/message/content/parts/0/text\\\",\\\"v\\\":\\\"spoken secret\\\"}]\"}}}"}`)
	outerType, innerType, paths, _ := shapeFrame(outer)
	if outerType != "data_message" || innerType != "chat_message_delta" || len(paths) == 0 {
		t.Fatalf("unexpected delta shape: outer=%q inner=%q paths=%#v", outerType, innerType, paths)
	}
}

func TestEventShapeSamplerCloseIsIdempotent(t *testing.T) {
	s := newEventShapeSampler(slog.Default(), "m", "wm", 1)
	s.close()
	s.close()
	select {
	case <-s.done:
	case <-time.After(time.Second):
		t.Fatal("sampler did not stop")
	}
}

func TestEventShapeSamplerDisabledByDefaultHasNoOutput(t *testing.T) {
	var logs bytes.Buffer
	var s *eventShapeSampler
	if s != nil {
		s.observe([]byte(`{"type":"data_message"}`), "datachannel", "b-to-a")
	}
	if logs.Len() != 0 {
		t.Fatalf("disabled sampler emitted output: %q", logs.String())
	}
}
