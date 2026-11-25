package voice

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"log/slog"
	"net"
	"syscall"
	"time"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/httpclient"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/store"
)

type creationStage string
type creationErrorClass string

const (
	stageAdmission         creationStage = "admission"
	stageZoeStart          creationStage = "zoe_start"
	stageClaim             creationStage = "creation_claim"
	stageValidation        creationStage = "request_validation"
	stageAccount           creationStage = "account_reservation"
	stageRelayConstruct    creationStage = "relay_construct"
	stageRelayOffer        creationStage = "relay_apply_offer"
	stageUpstreamPost      creationStage = "upstream_post"
	stageUpstreamResponse  creationStage = "upstream_response"
	stageUpstreamSideband  creationStage = "upstream_sideband"
	stageAnswerNegotiation creationStage = "answer_negotiation"
	stageRelayAttach       creationStage = "relay_attach"
	stageOutcomeWrite      creationStage = "outcome_write"
	stageBindingAck        creationStage = "binding_ack"
)

const (
	classTimeout            creationErrorClass = "transport_timeout"
	classRefused            creationErrorClass = "transport_refused"
	classEOF                creationErrorClass = "transport_eof"
	classTLS                creationErrorClass = "tls"
	classHTTP4xx            creationErrorClass = "http_4xx"
	classHTTP5xx            creationErrorClass = "http_5xx"
	classMalformedResponse creationErrorClass = "malformed_response"
	classUnexpectedStatus  creationErrorClass = "unexpected_status"
	classCanceled           creationErrorClass = "context_canceled"
	classDBWriteFailed      creationErrorClass = "db_write_failed"
	classEvidence           creationErrorClass = "evidence_failed"
	classNoAccount          creationErrorClass = "pool_no_account"
	classQuarantined        creationErrorClass = "pool_quarantined"
	classCapacity           creationErrorClass = "capacity"
	classValidation         creationErrorClass = "validation"
	classInternal           creationErrorClass = "internal"
	classReplay             creationErrorClass = "claim_conflict"
)

// Owned by the dialing goroutine. No request or error strings are stored here.
type creationDiagnostics struct {
	stage             creationStage
	class             creationErrorClass
	attempts          int
	lastStatus        int
	rejectionStatus   int
	relayStarted      bool
	relayCloseClean   *bool // nil until a failed attempt's close has been checked
	claimStarted      time.Time
	storedOutcome     string
	outcomeWriteOK    bool
	outcomeWriteError bool
}

func (d *creationDiagnostics) fail(stage creationStage, class creationErrorClass) {
	if d != nil {
		d.stage, d.class = stage, class
	}
}

func (d *creationDiagnostics) postStarted() {
	if d != nil {
		d.attempts++
	}
}

func (d *creationDiagnostics) closed(clean bool) {
	if d != nil {
		clean = clean && (d.relayCloseClean == nil || *d.relayCloseClean)
		d.relayCloseClean = &clean
	}
}

func (d *creationDiagnostics) outcomeWritten(outcome string, err error) {
	if d == nil {
		return
	}
	if err != nil {
		d.outcomeWriteError = true
		d.outcomeWriteOK = false
		return
	}
	d.storedOutcome = outcome
	d.outcomeWriteOK = !d.outcomeWriteError
}

func (d *creationDiagnostics) closeAttrs() []slog.Attr {
	if d == nil {
		return nil
	}
	return []slog.Attr{slog.String("creation_stage", string(d.stage)), slog.String("error_class", string(d.class))}
}

// Read the raw claim, not the public snapshot (which derives open/released
// from media evidence). A diagnostic read must never affect admission.
func (d *creationDiagnostics) log(logger *slog.Logger, claims CreationOutcomeStore, owner, requestID string) {
	if reader, ok := claims.(interface {
		GetVoiceCreationOutcome(string, string) (store.VoiceCreationOutcome, error)
	}); ok && requestID != "" {
		if row, err := reader.GetVoiceCreationOutcome(owner, requestID); err == nil {
			d.storedOutcome = row.Outcome
		} else if !errors.Is(err, store.ErrVoiceCreationOutcomeNotFound) {
			d.storedOutcome = ""
		}
	}
	var outcome, status, elapsed, closeClean any
	switch d.storedOutcome {
	case "pending", "unknown", "never_established", "established", "released":
		outcome = d.storedOutcome
	}
	if d.lastStatus >= 100 && d.lastStatus <= 599 {
		status = d.lastStatus
	}
	if !d.claimStarted.IsZero() {
		elapsed = time.Since(d.claimStarted).Milliseconds()
	}
	if d.relayCloseClean != nil {
		closeClean = *d.relayCloseClean
	}
	attrs := append(d.closeAttrs(),
		slog.Int("upstream_attempt_count", d.attempts), slog.Any("upstream_last_status", status),
		slog.Bool("relay_started", d.relayStarted), slog.Any("relay_close_clean", closeClean),
		slog.Any("stored_outcome", outcome), slog.Bool("outcome_write_ok", d.outcomeWriteOK), slog.Any("elapsed_ms", elapsed))
	if d.rejectionStatus >= 400 && d.rejectionStatus <= 599 {
		attrs = append(attrs, slog.Int("rejection_status", d.rejectionStatus))
	}
	logger.LogAttrs(context.Background(), slog.LevelWarn, "owned_call_creation_failed", attrs...)
}

// LogOwnedVoiceUnavailable covers a dial rejected before a voice service can be called.
func LogOwnedVoiceUnavailable() {
	d := creationDiagnostics{stage: stageAdmission, class: classInternal}
	d.log(slog.Default(), nil, "", "")
}

func creationHTTPClass(status int) creationErrorClass {
	switch {
	case status >= 400 && status <= 499:
		return classHTTP4xx
	case status >= 500 && status <= 599:
		return classHTTP5xx
	default:
		return classUnexpectedStatus
	}
}

func creationIOClass(err error, fallback creationErrorClass) creationErrorClass {
	var timeout net.Error
	var cert *tls.CertificateVerificationError
	var record tls.RecordHeaderError
	var alert tls.AlertError
	var unknownCA x509.UnknownAuthorityError
	var invalid x509.CertificateInvalidError
	var hostname x509.HostnameError
	var curl *httpclient.CurlRequestError
	switch {
	case errors.Is(err, context.Canceled):
		return classCanceled
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &timeout) && timeout.Timeout():
		return classTimeout
	case errors.Is(err, syscall.ECONNREFUSED):
		return classRefused
	case errors.As(err, &cert), errors.As(err, &record), errors.As(err, &alert), errors.As(err, &unknownCA), errors.As(err, &invalid), errors.As(err, &hostname):
		return classTLS
	case errors.As(err, &curl):
		// Curl's header reader can surface EOF for several distinct failures.
		switch curl.ExitCode {
		case 7:
			return classRefused
		case 28:
			return classTimeout
		case 35, 51, 58, 59, 60, 77, 80, 82, 83, 90, 91:
			return classTLS
		case 18, 52:
			return classEOF
		default:
			return fallback
		}
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return classEOF
	default:
		return fallback
	}
}
