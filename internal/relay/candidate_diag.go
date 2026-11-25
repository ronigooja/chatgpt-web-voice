package relay

import (
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"net"

	"github.com/pion/webrtc/v4"
)

func candidateIndex(id string) string {
	if id == "" {
		return diagUnavailable
	}
	digest := sha256.Sum256([]byte(id))
	return hex.EncodeToString(digest[:8])
}

func candidateNetwork(c webrtc.ICECandidateStats) (protocol, family string) {
	switch c.Protocol {
	case "udp", "tcp":
		protocol = c.Protocol
	}
	if ip := net.ParseIP(c.IP); ip != nil {
		if ip.To4() != nil {
			family = "ipv4"
		} else {
			family = "ipv6"
		}
	}
	return protocol, family
}

type remoteCandidateStats struct {
	id, kind, protocol, family string
}

func newRemoteCandidateStats(c webrtc.ICECandidateStats) remoteCandidateStats {
	r := remoteCandidateStats{id: c.ID}
	r.protocol, r.family = candidateNetwork(c)
	switch c.CandidateType {
	case webrtc.ICECandidateTypeHost, webrtc.ICECandidateTypeSrflx, webrtc.ICECandidateTypePrflx, webrtc.ICECandidateTypeRelay:
		r.kind = c.CandidateType.String()
	}
	return r
}

func newPairStats(p webrtc.ICECandidatePairStats) pairStats {
	s := pairStats{
		id: p.ID, localID: p.LocalCandidateID, remoteID: p.RemoteCandidateID, nominated: p.Nominated,
		counters: trafficCounters{uint64(p.PacketsSent), uint64(p.PacketsReceived), p.BytesSent, p.BytesReceived},
		lastSent: p.LastPacketSentTimestamp.Time().UnixNano(), lastReceived: p.LastPacketReceivedTimestamp.Time().UnixNano(),
	}
	switch p.State {
	case webrtc.StatsICECandidatePairStateFrozen, webrtc.StatsICECandidatePairStateWaiting, webrtc.StatsICECandidatePairStateInProgress, webrtc.StatsICECandidatePairStateFailed, webrtc.StatsICECandidatePairStateSucceeded:
		s.state = string(p.State)
	}
	return s
}

func pairRows(pairs, baseline []pairStats, deltas bool) []map[string]any {
	before := make(map[string]*trafficCounters, len(baseline))
	for i := range baseline {
		if baseline[i].id != "" {
			before[baseline[i].id] = &baseline[i].counters
		}
	}
	rows := make([]map[string]any, 0, len(pairs))
	for _, p := range pairs {
		row := map[string]any{
			"local_candidate_id": candidateIndex(p.localID), "remote_candidate_id": candidateIndex(p.remoteID),
			"pair_state": diagValue(p.state), "nominated": p.nominated,
			"packets_sent": p.counters.sent, "packets_received": p.counters.received,
			"bytes_sent": p.counters.bytesSent, "bytes_received": p.counters.bytesReceived,
		}
		if deltas {
			for _, attr := range p.counters.attrs("packets", before[p.id]) {
				row[attr.Key] = attr.Value.Any()
			}
		}
		rows = append(rows, row)
	}
	return rows
}

func (s peerSnapshot) pairBaselineAttrs(at time.Time) []slog.Attr {
	var age any = diagUnavailable
	source := diagUnavailable
	if s.pairBaseline != nil {
		source = "last_sample"
		if !s.pairBaselineAt.IsZero() {
			age = elapsedMS(at, s.pairBaselineAt)
		}
	}
	return []slog.Attr{slog.String("previous_candidate_pairs_source", source), slog.Any("previous_candidate_pairs_age_ms", age),
		slog.Any("previous_candidate_pairs", pairRows(s.pairBaseline, nil, false))}
}

func (d *peerDiagnostics) selectedPairChanged(pair *webrtc.ICECandidatePair) {
	if pair == nil {
		return
	}
	d.pairChangeMu.Lock()
	d.pairChangeCount++
	d.pairChangeAt = d.now().UnixNano()
	d.pairChangeMu.Unlock()
}

func (d *peerDiagnostics) candidateAttrs(sample peerSnapshot) []slog.Attr {
	d.pairChangeMu.Lock()
	count, at := d.pairChangeCount, d.pairChangeAt
	d.pairChangeMu.Unlock()
	attrs := []slog.Attr{
		slog.Uint64("selected_pair_change_count", count),
		slog.Any("last_selected_pair_change_age_ms", activityAge(sample.at, at)),
	}
	source := "live"
	selected := &sample
	var age any = diagUnavailable
	if sample.allowCandidateFallback && sample.stats.pair == nil {
		last := d.lastSelectedSample.Load()
		if last == nil && sample.reason != "" {
			last = d.last.Load()
		}
		if last != nil && last.stats.candidateReport {
			selected, source = last, "last_sample"
			age = elapsedMS(sample.at, last.at)
		}
	}
	if !selected.stats.candidateReport {
		attrs = append(attrs, slog.String("candidates_source", diagUnavailable),
			slog.String("candidate_pairs_source", diagUnavailable), slog.String("selected_candidate_ids_source", diagUnavailable))
		return append(attrs, sample.pairBaselineAttrs(sample.at)...)
	}
	var localID, remoteID string
	selectedSource := diagUnavailable
	if p := selected.stats.pair; p != nil {
		localID, remoteID, selectedSource = p.localID, p.remoteID, source
	}
	candidates := make([]map[string]any, 0, len(selected.stats.candidates))
	for _, c := range selected.stats.candidates {
		candidates = append(candidates, map[string]any{
			"candidate_id": candidateIndex(c.id), "candidate_type": diagValue(c.kind),
			"protocol": diagValue(c.protocol), "address_family": diagValue(c.family),
			"selected": remoteID != "" && c.id == remoteID,
		})
	}
	attrs = append(attrs, slog.String("candidates_source", source), slog.String("candidate_pairs_source", source),
		slog.String("selected_candidate_ids_source", selectedSource), slog.Any("candidate_snapshot_age_ms", age),
		slog.String("selected_local_candidate_id", candidateIndex(localID)),
		slog.String("selected_remote_candidate_id", candidateIndex(remoteID)),
		slog.Int("remote_candidate_count", len(candidates)), slog.Any("remote_candidates", candidates),
		slog.Int("candidate_pair_count", len(selected.stats.pairs)),
		slog.Any("candidate_pairs", pairRows(selected.stats.pairs, selected.pairBaseline, true)))
	return append(attrs, selected.pairBaselineAttrs(sample.at)...)
}
