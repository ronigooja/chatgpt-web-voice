package relay

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
)

type testPeerStats func() (peerStats, error)

func (f testPeerStats) collect() (peerStats, error) { return f() }

type peerLogSink chan string

func (s peerLogSink) Write(p []byte) (int, error) { s <- string(p); return len(p), nil }

func receiveDiag[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case x := <-ch:
		return x
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for diagnostics")
		var zero T
		return zero
	}
}

func decodeDiag(t *testing.T, line string) map[string]any {
	t.Helper()
	var log map[string]any
	if err := json.Unmarshal([]byte(line), &log); err != nil {
		t.Fatal(err)
	}
	return log
}

func assertDiag(t *testing.T, log map[string]any, key string, want any) {
	t.Helper()
	value := any(log)
	for part := range strings.SplitSeq(key, ".") {
		object, ok := value.(map[string]any)
		if !ok {
			t.Fatalf("%s: %s parent is not an object", key, part)
		}
		value = object[part]
	}
	if value != want {
		t.Errorf("%s = %v, want %v", key, value, want)
	}
}

func logDiagAttrs(t *testing.T, attrs []slog.Attr) map[string]any {
	t.Helper()
	sink := make(peerLogSink, 1)
	slog.New(slog.NewJSONHandler(sink, nil)).LogAttrs(t.Context(), slog.LevelInfo, "test", attrs...)
	return decodeDiag(t, receiveDiag(t, sink))
}

func TestPeerDiagnosticsActivity(t *testing.T) {
	now := time.Unix(1700000000, 0)
	d := newPeerDiagnostics(nil, func() time.Time { return now })
	b := d.addLeg("peer-b2")
	d.client.rtpRead.success(d.now())
	d.client.rtpRead.success(d.now())
	b.rtpWrite.success(d.now())
	d.client.dataRead.message(d.now(), 12)
	d.client.dataRead.message(d.now(), 4)
	now = now.Add(10 * time.Millisecond)
	d.client.rtpRead.failure(d.now(), classifyActivityError(io.EOF))
	now = now.Add(2 * time.Millisecond)
	d.client.rtpRead.failure(d.now(), classifyActivityError(errors.New("private error text")))
	b.rtpWrite.failure(d.now(), classifyActivityError(net.ErrClosed))
	d.client.rtpWrite.failure(d.now(), classifyActivityError(webrtc.ErrDataChannelNotOpen))
	now = now.Add(38 * time.Millisecond)
	log := logDiagAttrs(t, d.sessionAttrs())
	for key, want := range map[string]any{
		"session_age_ms": float64(50), "sampler_lateness_ms": diagUnavailable,
		"last_periodic_snapshot_age_ms":                diagUnavailable,
		"gateway_activity.peer_a.rtp.last_read_age_ms": float64(50), "gateway_activity.peer_a.rtp.last_read_packets": float64(2),
		"gateway_activity.peer_a.rtp.last_read_error_class": "error", "gateway_activity.peer_a.rtp.last_read_error_age_ms": float64(38),
		"gateway_activity.peer_a.rtp.last_write_error_class": "not_open", "gateway_activity.peer_a.rtp.last_write_age_ms": diagUnavailable,
		"gateway_activity.peer_a.datachannel.last_read_messages": float64(2), "gateway_activity.peer_a.datachannel.last_read_bytes": float64(16),
		"gateway_activity.peer_b2.rtp.last_write_packets": float64(1), "gateway_activity.peer_b2.rtp.last_write_age_ms": float64(50),
		"gateway_activity.peer_b2.rtp.last_write_error_class": "closed", "gateway_activity.peer_b2.rtp.last_write_error_age_ms": float64(38),
	} {
		assertDiag(t, log, key, want)
	}
}

func TestPeerDiagnosticsSnapshot(t *testing.T) {
	t.Run("normal and missing inbound", func(t *testing.T) {
		now := time.Unix(1700000000, 0)
		d := newPeerDiagnostics(nil, func() time.Time { return now })
		d.stats = testPeerStats(func() (peerStats, error) {
			s := peerStats{ice: "connected"}
			s.addReport(webrtc.StatsReport{"empty": webrtc.InboundRTPStreamStats{Kind: "audio"}}, webrtc.ICECandidatePairStats{}, false, webrtc.DataChannelStats{}, false)
			return s, nil
		})
		log := logDiagAttrs(t, d.attrs(d.snapshot(time.Second), nil, nil))
		assertDiag(t, log, "snapshot", "available")
		assertDiag(t, log, "ice_state", "connected")
		assertDiag(t, log, "dtls_state", diagUnavailable)
		assertDiag(t, log, "inbound_rtp", diagUnavailable)
		assertDiag(t, log, "selected_pair.present", false)
		assertDiag(t, log, "datachannel", diagUnavailable)
	})
	t.Run("timeout with abandoned collection", func(t *testing.T) {
		d := newPeerDiagnostics(nil, time.Now)
		var calls atomic.Int64
		release := make(chan struct{})
		t.Cleanup(func() { close(release) })
		started := make(chan struct{}, 1)
		d.stats = testPeerStats(func() (peerStats, error) {
			calls.Add(1)
			started <- struct{}{}
			<-release
			return peerStats{}, nil
		})
		result := make(chan peerSnapshot, 1)
		start := time.Now()
		go func() { result <- d.snapshot(100 * time.Millisecond) }()
		receiveDiag(t, started)
		sample := receiveDiag(t, result)
		elapsed := time.Since(start)
		if elapsed < 100*time.Millisecond || elapsed > 500*time.Millisecond || sample.reason != "timeout" {
			t.Fatalf("snapshot took %v, reason %q", elapsed, sample.reason)
		}
		if sample := d.snapshot(time.Millisecond); sample.reason != "timeout" || calls.Load() != 1 {
			t.Fatalf("overlapping snapshot: reason=%q calls=%d", sample.reason, calls.Load())
		}
		log := logDiagAttrs(t, d.attrs(sample, nil, nil))
		assertDiag(t, log, "snapshot", diagUnavailable)
		assertDiag(t, log, "snapshot_reason", "timeout")
		assertDiag(t, log, "gateway_activity.peer_a.rtp.last_read_packets", float64(0))
	})
	t.Run("closed", func(t *testing.T) {
		done := make(chan struct{})
		d := newPeerDiagnostics(done, time.Now)
		d.stats = testPeerStats(func() (peerStats, error) { panic("closed session called provider") })
		close(done)
		log := logDiagAttrs(t, d.attrs(d.snapshot(time.Second), nil, nil))
		assertDiag(t, log, "snapshot", diagUnavailable)
		assertDiag(t, log, "snapshot_reason", "closed")
	})
	t.Run("error class only", func(t *testing.T) {
		d := newPeerDiagnostics(nil, time.Now)
		d.stats = testPeerStats(func() (peerStats, error) { return peerStats{}, errors.New("private error text") })
		log := logDiagAttrs(t, d.attrs(d.snapshot(time.Second), nil, nil))
		assertDiag(t, log, "snapshot_reason", "error")
	})
}

func TestPeerDiagnosticWarningsAndFailureOrdering(t *testing.T) {
	for _, state := range []webrtc.PeerConnectionState{webrtc.PeerConnectionStateDisconnected, webrtc.PeerConnectionStateFailed} {
		t.Run(state.String(), func(t *testing.T) {
			sink := make(peerLogSink, 12)
			logger := slog.New(slog.NewJSONHandler(sink, nil)).With("media_session_id", "test-session")
			s := &pionSession{logger: logger, done: make(chan struct{})}
			const baseMS = 1700000000000
			s.diag = newPeerDiagnostics(s.done, func() time.Time { return time.UnixMilli(baseMS + 200) })
			s.diag.addLeg("peer-b7")
			started, release := make(chan struct{}), make(chan struct{})
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
			}()
			var captured atomic.Bool
			const candidateID, ip, url, secret = "private-candidate-id", "203.0.113.123", "https://private.invalid", "fake-credential"
			const port = 55201
			s.diag.stats = testPeerStats(func() (peerStats, error) {
				close(started)
				<-release
				sample := peerStats{ice: state.String(), dtls: "connected", sctp: "connected", dataState: "open"}
				sample.addReport(webrtc.StatsReport{
					candidateID: webrtc.ICECandidateStats{Protocol: "udp", IP: ip, Port: port, URL: url},
					"audio":     webrtc.InboundRTPStreamStats{Kind: "audio", PacketsReceived: 52, PacketsLost: -2, Jitter: .007, BytesReceived: 1400, LastPacketReceivedTimestamp: baseMS + 100},
				}, webrtc.ICECandidatePairStats{
					ID: candidateID, RemoteCandidateID: candidateID, State: webrtc.StatsICECandidatePairStateSucceeded,
					Nominated: true, PacketsSent: 53, PacketsReceived: 54, BytesSent: 200, BytesReceived: 204,
					LastPacketSentTimestamp: baseMS + 160, LastPacketReceivedTimestamp: baseMS + 90,
				}, true, webrtc.DataChannelStats{ID: secret, Label: secret, MessagesSent: 2, MessagesReceived: 3}, true)
				captured.Store(!s.diag.closed())
				return sample, nil
			})
			s.handlePeerAState(state) // Must return while the fake provider is blocked.
			receiveDiag(t, started)
			if s.diag.closed() {
				t.Fatal("session closed before the snapshot completed")
			}
			close(release)
			log := decodeDiag(t, receiveDiag(t, sink))
			delete(log, "time") // A wall-clock log timestamp can randomly contain the test port digits.
			line, err := json.Marshal(log)
			if err != nil {
				t.Fatal(err)
			}
			for _, private := range []string{candidateID, ip, "55201", url, secret} {
				if strings.Contains(string(line), private) {
					t.Errorf("diagnostics exposed %q: %s", private, line)
				}
			}
			for key, want := range map[string]any{
				"level": "WARN", "msg": "relay peer " + state.String(), "peer": "peer-a",
				"state": state.String(), "media_session_id": "test-session", "snapshot": "available",
				"dtls_state": "connected", "sctp_state": "connected", "datachannel_state": "open", "ice_state": state.String(),
				"selected_pair.present": true, "selected_pair.pair_state": "succeeded", "selected_pair.nominated": true,
				"selected_pair.transport_protocol": "udp", "selected_pair.address_family": "ipv4",
				"selected_pair.packets_sent": float64(53), "selected_pair.packets_received": float64(54),
				"selected_pair.bytes_sent": float64(200), "selected_pair.bytes_received": float64(204),
				"selected_pair.packets_sent_delta": diagUnavailable, "selected_pair.bytes_received_delta": diagUnavailable,
				"selected_pair.application_last_packet_sent_age_ms":     float64(40),
				"selected_pair.application_last_packet_received_age_ms": float64(110),
				"inbound_rtp.packets_received":                          float64(52), "inbound_rtp.packets_lost": float64(-2), "inbound_rtp.jitter_ms": float64(7),
				"inbound_rtp.last_packet_received_age_ms": float64(100),
				"datachannel.messages_sent":               float64(2), "datachannel.messages_received": float64(3),
				"datachannel.bytes_sent_delta": diagUnavailable, "datachannel.messages_received_delta": diagUnavailable,
				"gateway_activity.peer_a.rtp.last_read_age_ms":                diagUnavailable,
				"gateway_activity.peer_b7.datachannel.last_write_error_class": diagUnavailable,
				"last_periodic_snapshot_age_ms":                               diagUnavailable, "sampler_lateness_ms": diagUnavailable, "previous_sample_age_ms": diagUnavailable,
			} {
				assertDiag(t, log, key, want)
			}
			if _, ok := log["session_age_ms"].(float64); !ok || !captured.Load() {
				t.Fatal("missing session age or session was closed during collection")
			}
			if state == webrtc.PeerConnectionStateFailed {
				failure := decodeDiag(t, receiveDiag(t, sink))
				assertDiag(t, failure, "msg", "relay session failed")
				assertDiag(t, failure, "reason", "peer_a_failed")
				receiveDiag(t, s.done)
				assertDiag(t, decodeDiag(t, receiveDiag(t, sink)), "msg", "relay session closing")
				assertDiag(t, decodeDiag(t, receiveDiag(t, sink)), "msg", "relay session closed")
			}
		})
	}
}

func TestPeerSamplerStopsOnSessionClose(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	for range 12 {
		s := &pionSession{logger: logger, done: make(chan struct{})}
		s.diag = newPeerDiagnostics(s.done, time.Now)
		started, release, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
		s.diag.stats = testPeerStats(func() (peerStats, error) {
			close(started)
			<-release
			defer close(returned)
			return peerStats{}, nil
		})
		ticks, stopped := make(chan time.Time, 1), make(chan struct{})
		go func() {
			defer close(stopped)
			s.diag.sampleLoop(logger, ticks)
		}()
		ticks <- time.Now()
		receiveDiag(t, started)
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		receiveDiag(t, stopped) // The sampler exits even while the provider is blocked.
		close(release)
		receiveDiag(t, returned)
		select {
		case s.diag.gate <- struct{}{}: // Check that the worker finished with its slot.
			<-s.diag.gate
		case <-time.After(time.Second):
			t.Fatal("stats worker did not release its slot")
		}
	}
}

func TestPeerDiagnosticsPeriodicDeltas(t *testing.T) {
	const id = "pair-id"
	now := time.Unix(1700000000, 0)
	done := make(chan struct{})
	d := newPeerDiagnostics(done, func() time.Time { return now })
	initial := &peerSnapshot{at: now.Add(-10 * time.Second), stats: peerStats{
		pair:    &pairStats{id: id, counters: trafficCounters{2, 3, 10, 20}},
		channel: &channelStats{id: "events", counters: trafficCounters{4, 5, 30, 40}},
	}}
	d.last.Store(initial)
	stats := peerStats{
		pair:    &pairStats{id: id, counters: trafficCounters{4, 5, 40, 49}},
		channel: &channelStats{id: "events", counters: trafficCounters{1, 6, 2, 45}},
	}
	d.stats = testPeerStats(func() (peerStats, error) { return stats, nil })
	sink := make(peerLogSink, 1)
	logger := slog.New(slog.NewJSONHandler(sink, &slog.HandlerOptions{Level: slog.LevelDebug}))
	ticks, stopped := make(chan time.Time, 1), make(chan struct{})
	go func() {
		defer close(stopped)
		d.sampleLoop(logger, ticks)
	}()
	ticks <- now.Add(-25 * time.Millisecond)
	log := decodeDiag(t, receiveDiag(t, sink))
	close(done)
	receiveDiag(t, stopped)
	assertDiag(t, log, "level", "DEBUG")
	assertDiag(t, log, "previous_sample_age_ms", float64(10000))
	assertDiag(t, log, "last_periodic_snapshot_age_ms", float64(0))
	assertDiag(t, log, "sampler_lateness_ms", float64(25))
	assertDiag(t, log, "selected_pair.packets_sent_delta", float64(2))
	assertDiag(t, log, "selected_pair.bytes_received_delta", float64(29))
	assertDiag(t, log, "datachannel.messages_sent_delta", diagUnavailable)
	assertDiag(t, log, "datachannel.messages_received_delta", float64(1))
	assertDiag(t, log, "datachannel.bytes_sent_delta", diagUnavailable)
	previous := d.last.Load()
	if previous == initial {
		t.Fatal("periodic sample was not retained")
	}
	stats.pair = &pairStats{id: "new-pair", counters: trafficCounters{5, 9, 55, 56}}
	log = logDiagAttrs(t, d.attrs(peerSnapshot{at: now.Add(time.Second), stats: stats}, previous, previous))
	assertDiag(t, log, "selected_pair.packets_sent_delta", diagUnavailable)
	assertDiag(t, log, "datachannel.messages_received_delta", float64(0))
	log = logDiagAttrs(t, d.attrs(peerSnapshot{at: now, stats: stats}, &peerSnapshot{at: now, reason: "timeout"}, previous))
	assertDiag(t, log, "datachannel.messages_received_delta", diagUnavailable)
}
