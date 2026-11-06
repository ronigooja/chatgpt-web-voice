package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDownstreamSessionInstructionsValidation(t *testing.T) {
	for _, tc := range []struct {
		name, instructions, raw, wantError string
		status                             int
	}{
		{name: "verbatim", instructions: "  常驻\"规则\"\nnext\n", status: 200},
		{name: "omitted", status: 200},
		{name: "byte limit", instructions: strings.Repeat("界", 2000), status: 200},
		{name: "oversize bytes", instructions: strings.Repeat("界", 2001), status: 400, wantError: "6000 bytes"},
		{name: "invalid UTF-8", raw: "{\"offer_sdp\":\"v=0\",\"instructions\":\"\xff\"}", status: 400, wantError: "UTF-8"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := &downstreamVoiceStub{}
			h, key, _ := newDownstreamTestHandler(t, stub)
			body := tc.raw
			if body == "" {
				payload := map[string]any{"offer_sdp": "v=0"}
				if tc.instructions != "" {
					payload["instructions"] = tc.instructions
				}
				b, _ := json.Marshal(payload)
				body = string(b)
			}
			r := httptest.NewRequest(http.MethodPost, "/v1/voice/sessions", strings.NewReader(body))
			r.Header.Set("Authorization", "Bearer "+key)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tc.status {
				t.Fatalf("status=%d want=%d body=%s", w.Code, tc.status, w.Body.String())
			}
			if tc.status == 400 {
				if stub.createRequest.Owner != "" || !strings.Contains(w.Body.String(), tc.wantError) {
					t.Fatalf("must reject before service call with clear error: request=%+v response=%s", stub.createRequest, w.Body.String())
				}
				return
			}
			encoded, _ := json.Marshal(stub.createRequest)
			var fields map[string]any
			_ = json.Unmarshal(encoded, &fields)
			got, _ := fields["Instructions"].(string)
			if got != tc.instructions {
				t.Fatalf("forwarded instructions=%q want=%q", got, tc.instructions)
			}
		})
	}
}
