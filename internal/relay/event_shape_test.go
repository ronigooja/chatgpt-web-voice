package relay

import (
	"bytes"
	"fmt"
	"log/slog"
	"reflect"
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

func TestEventShapeSamplerRedactsIDLikeTypes(t *testing.T) {
	var logs bytes.Buffer
	s := newEventShapeSampler(slog.New(slog.NewTextHandler(&logs, nil)), "media-types", "wm", 1)
	s.observe([]byte(`{"type":"0123456789abcdef0123456789abcdef0123456789","data":{"type":"550e8400-e29b-41d4-a716-446655440000"}}`), "datachannel", "b-to-a")
	s.close()
	got := logs.String()
	if strings.Contains(got, "0123456789abcdef0123456789abcdef0123456789") || strings.Contains(got, "550e8400-e29b-41d4-a716-446655440000") {
		t.Fatalf("ID-like type disclosed: %s", got)
	}
}

func TestEventShapeSamplerSanitizesDynamicKeys(t *testing.T) {
	var logs bytes.Buffer
	s := newEventShapeSampler(slog.New(slog.NewTextHandler(&logs, nil)), "media-keys", "wm", 1)
	s.observe([]byte(`{"type":"data_message","data":{"550e8400-e29b-41d4-a716-446655440000":"uuid secret","hello world用户":"user secret","role":"user"}}`), "datachannel", "b-to-a")
	s.close()
	got := logs.String()
	if strings.Contains(got, "550e8400-e29b-41d4-a716-446655440000") || strings.Contains(got, "hello world用户") || !strings.Contains(got, "role=string=user") {
		t.Fatalf("dynamic key disclosed or normal key missing: %s", got)
	}
}

func TestEventShapeSamplerLogsNormalDeltaAndFinalish(t *testing.T) {
	var logs bytes.Buffer
	s := newEventShapeSampler(slog.New(slog.NewTextHandler(&logs, nil)), "media-positive", "wm", 1)
	s.observe([]byte(`{"type":"data_message","data":{"type":"chat_message_delta","end_turn":true,"is_final":true}}`), "datachannel", "b-to-a")
	s.close()
	got := logs.String()
	if !strings.Contains(got, "outer_type=data_message") || !strings.Contains(got, "inner_type=chat_message_delta") || !strings.Contains(got, "type=string=chat_message_delta") || !strings.Contains(got, "finalish_key_observed=true") {
		t.Fatalf("normal delta/finalish shape missing: %s", got)
	}
}

func TestEventShapeSamplerDisclosesDeepRolesDirectionsBooleansAndSafePatchValues(t *testing.T) {
	var logs bytes.Buffer
	s := newEventShapeSampler(slog.New(slog.NewTextHandler(&logs, nil)), "media-deep", "wm", 1)
	s.observe([]byte(`{"type":"data_message","data":{"message":{"author":{"role":"user","assistant":{"role":"assistant"}},"content":{"parts":[{"content_type":"audio_transcription","direction":"in"}]},"end_turn":true,"patches":[{"p":"/message/status","v":"finished_successfully"},{"p":"/message/end_turn","v":false},{"p":"/message/content/parts/0/text","v":"a distinctive secret sentence"},{"p":"/message/metadata/foo","v":"secretvalue"}]}}}`), "datachannel", "b-to-a")
	s.close()
	got := logs.String()
	for _, want := range []string{"role=string=user", "role=string=assistant", "content_type=string=audio_transcription", "direction=string=in", "end_turn=bool=true", "v=string=finished_successfully", "v=bool=false"} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in %s", want, got)
		}
	}
	for _, forbidden := range []string{"a distinctive secret sentence", "secretvalue"} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("secret disclosed: %s", got)
		}
	}
}

func TestEventShapeSamplerCollectsDeterministicallyAndCapsPaths(t *testing.T) {
	frame := map[string]any{}
	for i := 0; i < 200; i++ {
		frame[fmt.Sprintf("key%03d", i)] = i
	}
	one, _ := collectShapePaths(frame)
	two, _ := collectShapePaths(frame)
	if !reflect.DeepEqual(one, two) {
		t.Fatalf("path order differs: %v vs %v", one, two)
	}
	if len(one) != maxEventShapePaths {
		t.Fatalf("path cap=%d, want %d", len(one), maxEventShapePaths)
	}
}
