package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"unicode/utf8"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/auth"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/voice"
)

type resultDeliveryService interface {
	SubmitResultDelivery(string, string, voice.ResultDeliveryRequest) (voice.ResultReceipt, error)
	LookupResultDelivery(string, string, int64, string) (voice.ResultReceipt, error)
}

func (s *Server) downstreamResultDelivery(w http.ResponseWriter, r *http.Request) {
	owner := auth.APIKeyOwner(r.Context())
	if owner == "" {
		writeDownstreamError(w, 401, "invalid_api_key", "valid Bearer API key required")
		return
	}
	service, ok := s.voice.(resultDeliveryService)
	if !ok {
		writeDownstreamError(w, 503, "unavailable", "result receipts unavailable")
		return
	}
	session := r.PathValue("id")
	if len(session) == 0 || len(session) > 256 {
		writeDownstreamError(w, 400, "invalid_request", "invalid session id")
		return
	}
	var receipt voice.ResultReceipt
	var err error
	if r.Method == http.MethodGet {
		generation, e := strconv.ParseInt(r.URL.Query().Get("media_generation"), 10, 64)
		id := r.PathValue("delivery_id")
		if e != nil || generation <= 0 || !voice.ValidDeliveryID(id) {
			writeDownstreamError(w, 400, "invalid_request", "invalid receipt identity")
			return
		}
		receipt, err = service.LookupResultDelivery(owner, session, generation, id)
	} else {
		raw, e := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<10))
		if e != nil {
			writeDownstreamError(w, 413, "invalid_request", "body too large")
			return
		}
		if !validResultJSON(raw) {
			writeDownstreamError(w, 400, "invalid_request", "invalid JSON body")
			return
		}
		var p voice.ResultDeliveryRequest
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&p) != nil || decoder.Decode(new(any)) != io.EOF {
			writeDownstreamError(w, 400, "invalid_request", "invalid JSON body")
			return
		}
		receipt, err = service.SubmitResultDelivery(owner, session, p)
	}
	if err != nil {
		writeDownstreamServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, receipt)
}

// validResultJSON applies before Go's decoder can replace malformed UTF-8 or
// unpaired UTF-16 escapes. Duplicate keys are rejected, not last-value-wins.
func validResultJSON(raw []byte) bool {
	if !utf8.Valid(raw) {
		return false
	}
	for i := 0; i < len(raw); i++ {
		if raw[i] != '\\' {
			continue
		}
		i++
		if i >= len(raw) {
			return false
		}
		if raw[i] != 'u' {
			continue
		}
		if i+4 >= len(raw) {
			return false
		}
		n, e := strconv.ParseUint(string(raw[i+1:i+5]), 16, 16)
		if e != nil {
			return false
		}
		i += 4
		if n >= 0xDC00 && n <= 0xDFFF {
			return false
		}
		if n >= 0xD800 && n <= 0xDBFF {
			if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
				return false
			}
			low, e := strconv.ParseUint(string(raw[i+3:i+7]), 16, 16)
			if e != nil || low < 0xDC00 || low > 0xDFFF {
				return false
			}
			i += 6
		}
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('{') {
		return false
	}
	keys := map[string]bool{}
	for dec.More() {
		tok, err = dec.Token()
		if err != nil {
			return false
		}
		key, ok := tok.(string)
		if !ok || keys[key] {
			return false
		}
		switch key {
		case "media_generation", "delivery_id", "content_hash", "text":
		default:
			return false
		}
		keys[key] = true
		var value json.RawMessage
		if dec.Decode(&value) != nil {
			return false
		}
	}
	_, err = dec.Token()
	return err == nil
}
