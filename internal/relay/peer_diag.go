package relay

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/webrtc/v4"
)

const (
	peerSampleInterval = 10 * time.Second
	peerStatsDeadline  = time.Second
	diagUnavailable    = "unavailable"
)

type activityError uint8

const (
	activityClosed activityError = iota
	activityTimeout
	activityNotOpen
	activityOtherError
)

var activityErrorNames = [...]string{"closed", "timeout", "not_open", "error"}

func classifyActivityError(err error) activityError {
	var networkErr net.Error
	switch {
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrClosedPipe), errors.Is(err, net.ErrClosed), errors.Is(err, webrtc.ErrConnectionClosed):
		return activityClosed
	case errors.Is(err, webrtc.ErrDataChannelNotOpen):
		return activityNotOpen
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, os.ErrDeadlineExceeded), errors.As(err, &networkErr) && networkErr.Timeout():
		return activityTimeout
	default:
		return activityOtherError
	}
}

type trafficActivity struct {
	last  atomic.Int64
	count atomic.Uint64
	bytes atomic.Uint64
	// A timestamp per class keeps the age and class consistent without a lock.
	errors [len(activityErrorNames)]atomic.Int64
}

func (a *trafficActivity) success(now time.Time) {
	a.last.Store(now.UnixNano())
	a.count.Add(1)
}

func (a *trafficActivity) failure(now time.Time, class activityError) {
	a.errors[class].Store(now.UnixNano())
}

func (a *trafficActivity) message(now time.Time, size int) {
	a.bytes.Add(uint64(size))
	a.success(now)
}

func (a *trafficActivity) sent(err error, size int) {
	if a == nil {
		return
	}
	if err != nil {
		a.failure(time.Now(), classifyActivityError(err))
	} else {
		a.message(time.Now(), size)
	}
}

func (a *trafficActivity) attrs(now time.Time, direction, unit string) []slog.Attr {
	var errorAt int64
	errorClass := diagUnavailable
	for i := range a.errors {
		if at := a.errors[i].Load(); at > errorAt {
			errorAt, errorClass = at, activityErrorNames[i]
		}
	}
	attrs := []slog.Attr{
		slog.Any(direction+"_age_ms", activityAge(now, a.last.Load())),
		slog.Uint64(direction+"_"+unit, a.count.Load()),
		slog.String(direction+"_error_class", errorClass),
		slog.Any(direction+"_error_age_ms", activityAge(now, errorAt)),
	}
	if unit == "messages" {
		attrs = append(attrs, slog.Uint64(direction+"_bytes", a.bytes.Load()))
	}
	return attrs
}

type peerActivity struct {
	rtpRead, rtpWrite   trafficActivity
	dataRead, dataWrite trafficActivity
}

func (a *peerActivity) attrs(now time.Time, name string) slog.Attr {
	return slog.GroupAttrs(name,
		slog.GroupAttrs("rtp", append(a.rtpRead.attrs(now, "last_read", "packets"), a.rtpWrite.attrs(now, "last_write", "packets")...)...),
		slog.GroupAttrs("datachannel", append(a.dataRead.attrs(now, "last_read", "messages"), a.dataWrite.attrs(now, "last_write", "messages")...)...),
	)
}

func activityAge(now time.Time, nanos int64) any {
	if nanos <= 0 {
		return diagUnavailable
	}
	return elapsedMS(now, time.Unix(0, nanos))
}

func elapsedMS(now, then time.Time) int64 {
	return max(0, now.Sub(then).Milliseconds())
}

type trafficCounters struct {
	sent, received, bytesSent, bytesReceived uint64
}

func (c trafficCounters) attrs(unit string, before *trafficCounters) []slog.Attr {
	keys := [...]string{unit + "_sent", unit + "_received", "bytes_sent", "bytes_received"}
	totals := [...]uint64{c.sent, c.received, c.bytesSent, c.bytesReceived}
	var previous [len(totals)]uint64
	if before != nil {
		previous = [len(totals)]uint64{before.sent, before.received, before.bytesSent, before.bytesReceived}
	}
	attrs := make([]slog.Attr, 0, 2*len(totals))
	for i, total := range totals {
		var delta any = diagUnavailable
		if before != nil && total >= previous[i] {
			delta = total - previous[i]
		}
		attrs = append(attrs, slog.Uint64(keys[i], total), slog.Any(keys[i]+"_delta", delta))
	}
	return attrs
}

type pairStats struct {
	id, state, protocol, family string
	nominated                   bool
	counters                    trafficCounters
	lastSent, lastReceived      int64
}

type inboundRTPStats struct {
	packetsReceived, bytesReceived uint64
	packetsLost                    int64
	jitterMS, lastReceived         int64
}

type channelStats struct {
	id       string
	counters trafficCounters
}

// peerStats contains only diagnostic values. IDs identify counter series in
// memory; neither IDs nor the original library report are logged.
type peerStats struct {
	ice, dtls, sctp, dataState string
	pair                       *pairStats
	inbound                    *inboundRTPStats
	channel                    *channelStats
}

type peerStatsProvider interface {
	collect() (peerStats, error)
}

type pionPeerStats struct {
	pc     *webrtc.PeerConnection
	bridge *dataChannelBridge
}

func (p pionPeerStats) collect() (peerStats, error) {
	if p.pc.ConnectionState() == webrtc.PeerConnectionStateClosed {
		return peerStats{}, net.ErrClosed
	}
	stats := peerStats{ice: p.pc.ICEConnectionState().String()}
	var selected webrtc.ICECandidatePairStats
	var hasPair bool
	if sctp := p.pc.SCTP(); sctp != nil {
		stats.sctp = sctp.State().String()
		if dtls := sctp.Transport(); dtls != nil {
			stats.dtls = dtls.State().String()
			if transport := dtls.ICETransport(); transport != nil {
				selected, hasPair = transport.GetSelectedCandidatePairStats()
			}
		}
	}
	p.bridge.downstreamMu.Lock()
	channel := p.bridge.downstream
	p.bridge.downstreamMu.Unlock()
	report := p.pc.GetStats()
	if p.pc.ConnectionState() == webrtc.PeerConnectionStateClosed {
		return peerStats{}, net.ErrClosed
	}
	if report == nil {
		return peerStats{}, errors.New("peer stats missing")
	}
	var dc webrtc.DataChannelStats
	var hasChannel bool
	if channel != nil {
		stats.dataState = channel.ReadyState().String()
		dc, hasChannel = report.GetDataChannelStats(channel)
	}
	stats.addReport(report, selected, hasPair, dc, hasChannel)
	return stats, nil
}

func (s *peerStats) addReport(report webrtc.StatsReport, selected webrtc.ICECandidatePairStats, hasPair bool, dc webrtc.DataChannelStats, hasChannel bool) {
	if hasPair {
		pair := &pairStats{
			id: selected.ID, state: string(selected.State), nominated: selected.Nominated,
			counters: trafficCounters{uint64(selected.PacketsSent), uint64(selected.PacketsReceived), selected.BytesSent, selected.BytesReceived},
			lastSent: selected.LastPacketSentTimestamp.Time().UnixNano(), lastReceived: selected.LastPacketReceivedTimestamp.Time().UnixNano(),
		}
		if candidate, ok := report[selected.RemoteCandidateID].(webrtc.ICECandidateStats); ok {
			switch candidate.Protocol {
			case "udp", "tcp":
				pair.protocol = candidate.Protocol
			}
			if ip := net.ParseIP(candidate.IP); ip != nil {
				if ip.To4() != nil {
					pair.family = "ipv4"
				} else {
					pair.family = "ipv6"
				}
			}
		}
		s.pair = pair
	}
	if hasChannel {
		s.channel = &channelStats{id: dc.ID, counters: trafficCounters{uint64(dc.MessagesSent), uint64(dc.MessagesReceived), dc.BytesSent, dc.BytesReceived}}
	}
	var inbound inboundRTPStats
	for _, entry := range report {
		rtp, ok := entry.(webrtc.InboundRTPStreamStats)
		if !ok || rtp.Kind != string(webrtc.MediaKindAudio) {
			continue
		}
		// The receiver can publish an empty row before its counters exist.
		if rtp.LastPacketReceivedTimestamp <= 0 && rtp.PacketsReceived == 0 && rtp.PacketsLost == 0 && rtp.BytesReceived == 0 {
			continue
		}
		s.inbound = &inbound
		inbound.packetsReceived += uint64(rtp.PacketsReceived)
		inbound.packetsLost += int64(rtp.PacketsLost)
		inbound.bytesReceived += rtp.BytesReceived
		inbound.jitterMS = max(inbound.jitterMS, int64(rtp.Jitter*1000))
		inbound.lastReceived = max(inbound.lastReceived, rtp.LastPacketReceivedTimestamp.Time().UnixNano())
	}
}

type peerSnapshot struct {
	at       time.Time
	reason   string
	lateness int64
	stats    peerStats
}

type namedPeerActivity struct {
	name     string
	activity *peerActivity
}

type peerDiagnostics struct {
	start time.Time
	now   func() time.Time
	done  <-chan struct{}
	stats peerStatsProvider
	gate  chan struct{}
	last  atomic.Pointer[peerSnapshot]
	once  sync.Once

	client peerActivity
	legsMu sync.Mutex
	legs   []namedPeerActivity
}

func newPeerDiagnostics(done <-chan struct{}, now func() time.Time) *peerDiagnostics {
	return &peerDiagnostics{start: now(), now: now, done: done, gate: make(chan struct{}, 1)}
}

func (d *peerDiagnostics) addLeg(name string) *peerActivity {
	a := &peerActivity{}
	d.legsMu.Lock()
	d.legs = append(d.legs, namedPeerActivity{name: strings.ReplaceAll(name, "-", "_"), activity: a})
	d.legsMu.Unlock()
	return a
}

func (d *peerDiagnostics) closed() bool {
	select {
	case <-d.done:
		return true
	default:
		return false
	}
}

func (d *peerDiagnostics) snapshot(deadline time.Duration) (sample peerSnapshot) {
	defer func() { sample.at = d.now() }()
	if d.closed() {
		return peerSnapshot{reason: "closed"}
	}
	if d.stats == nil {
		return peerSnapshot{reason: "error"}
	}
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	// Share the deadline between waiting for an earlier collection and starting
	// a fresh one. An abandoned, uncancellable library call keeps the slot until
	// it returns, so we never pile up blocked workers for a session.
	select {
	case d.gate <- struct{}{}:
	case <-ctx.Done():
		return peerSnapshot{reason: "timeout"}
	case <-d.done:
		return peerSnapshot{reason: "closed"}
	}
	if d.closed() || ctx.Err() != nil {
		<-d.gate
		if d.closed() {
			return peerSnapshot{reason: "closed"}
		}
		return peerSnapshot{reason: "timeout"}
	}
	result := make(chan peerSnapshot, 1)
	provider, gate := d.stats, d.gate
	go func() {
		stats, err := provider.collect()
		<-gate
		switch {
		case errors.Is(err, net.ErrClosed):
			result <- peerSnapshot{reason: "closed"}
		case err != nil:
			result <- peerSnapshot{reason: "error"}
		default:
			result <- peerSnapshot{stats: stats}
		}
	}()
	select {
	case sample = <-result:
		if d.closed() {
			return peerSnapshot{reason: "closed"}
		}
		if ctx.Err() != nil {
			return peerSnapshot{reason: "timeout"}
		}
		return sample
	case <-ctx.Done():
		return peerSnapshot{reason: "timeout"}
	case <-d.done:
		return peerSnapshot{reason: "closed"}
	}
}

func (d *peerDiagnostics) startSampler(logger *slog.Logger) {
	d.once.Do(func() {
		go func() {
			ticker := time.NewTicker(peerSampleInterval)
			defer ticker.Stop()
			d.sampleLoop(logger, ticker.C)
		}()
	})
}

func (d *peerDiagnostics) sampleLoop(logger *slog.Logger, ticks <-chan time.Time) {
	for {
		select {
		case <-d.done:
			return
		case tick := <-ticks:
			lateness := elapsedMS(d.now(), tick)
			previous := d.last.Load()
			sample := d.snapshot(peerStatsDeadline)
			if d.closed() {
				return
			}
			sample.lateness = lateness
			d.last.Store(&sample)
			if logger.Enabled(context.Background(), slog.LevelDebug) {
				logger.LogAttrs(context.Background(), slog.LevelDebug, "relay peer sample", d.attrs(sample, previous, &sample)...)
			}
		}
	}
}

func (d *peerDiagnostics) logDrop(logger *slog.Logger, message string, after func()) {
	previous := d.last.Load()
	sample := d.snapshot(peerStatsDeadline)
	logger.LogAttrs(context.Background(), slog.LevelWarn, message, d.attrs(sample, previous, d.last.Load())...)
	if after != nil {
		after()
	}
}

func (d *peerDiagnostics) gatewayAttrs(now time.Time, last *peerSnapshot) []slog.Attr {
	var age, late any = diagUnavailable, diagUnavailable
	if last != nil {
		age, late = elapsedMS(now, last.at), last.lateness
	}
	peers := []slog.Attr{d.client.attrs(now, "peer_a")}
	d.legsMu.Lock()
	legs := append([]namedPeerActivity(nil), d.legs...)
	d.legsMu.Unlock()
	for _, leg := range legs {
		peers = append(peers, leg.activity.attrs(now, leg.name))
	}
	return []slog.Attr{
		slog.Int64("session_age_ms", elapsedMS(now, d.start)),
		slog.Any("last_periodic_snapshot_age_ms", age),
		slog.Any("sampler_lateness_ms", late),
		slog.GroupAttrs("gateway_activity", peers...),
	}
}

func (d *peerDiagnostics) sessionAttrs(attrs ...slog.Attr) []slog.Attr {
	if d == nil {
		return attrs
	}
	return append(attrs, d.gatewayAttrs(d.now(), d.last.Load())...)
}

func diagValue(s string) string {
	if s == "" {
		return diagUnavailable
	}
	return s
}

func (d *peerDiagnostics) attrs(sample peerSnapshot, previous, last *peerSnapshot) []slog.Attr {
	attrs := d.gatewayAttrs(sample.at, last)
	if sample.reason != "" {
		return append(attrs, slog.String("snapshot", diagUnavailable), slog.String("snapshot_reason", sample.reason))
	}
	stats := sample.stats
	var previousAge any = diagUnavailable
	if previous != nil {
		previousAge = elapsedMS(sample.at, previous.at)
	}
	attrs = append(attrs, slog.String("snapshot", "available"),
		slog.Any("previous_sample_age_ms", previousAge),
		slog.String("ice_state", diagValue(stats.ice)), slog.String("dtls_state", diagValue(stats.dtls)),
		slog.String("sctp_state", diagValue(stats.sctp)), slog.String("datachannel_state", diagValue(stats.dataState)),
	)
	pair := []slog.Attr{slog.Bool("present", stats.pair != nil)}
	if p := stats.pair; p != nil {
		var before *trafficCounters
		if previous != nil && previous.stats.pair != nil && p.id != "" && previous.stats.pair.id == p.id {
			before = &previous.stats.pair.counters
		}
		pair = append(pair, slog.String("pair_state", diagValue(p.state)), slog.Bool("nominated", p.nominated),
			slog.String("transport_protocol", diagValue(p.protocol)), slog.String("address_family", diagValue(p.family)),
			slog.Any("application_last_packet_sent_age_ms", activityAge(sample.at, p.lastSent)),
			slog.Any("application_last_packet_received_age_ms", activityAge(sample.at, p.lastReceived)),
		)
		pair = append(pair, p.counters.attrs("packets", before)...)
	}
	attrs = append(attrs, slog.GroupAttrs("selected_pair", pair...))
	if rtp := stats.inbound; rtp != nil {
		attrs = append(attrs, slog.GroupAttrs("inbound_rtp", slog.Uint64("packets_received", rtp.packetsReceived),
			slog.Int64("packets_lost", rtp.packetsLost), slog.Uint64("bytes_received", rtp.bytesReceived), slog.Int64("jitter_ms", rtp.jitterMS),
			slog.Any("last_packet_received_age_ms", activityAge(sample.at, rtp.lastReceived))))
	} else {
		attrs = append(attrs, slog.String("inbound_rtp", diagUnavailable))
	}
	if dc := stats.channel; dc != nil {
		var before *trafficCounters
		if previous != nil && previous.stats.channel != nil && dc.id != "" && previous.stats.channel.id == dc.id {
			before = &previous.stats.channel.counters
		}
		attrs = append(attrs, slog.GroupAttrs("datachannel", dc.counters.attrs("messages", before)...))
	} else {
		attrs = append(attrs, slog.String("datachannel", diagUnavailable))
	}
	return attrs
}
