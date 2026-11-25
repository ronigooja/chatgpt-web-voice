package relay

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
)

func TestCandidateDiagnosticsDeltasAndFallback(t *testing.T) {
	now := time.Unix(1700000000, 0)
	d := newPeerDiagnostics(nil, func() time.Time { return now })
	privateIP, privateLocal, privateURL := "203.0.113.42", "[2001:db8::42]:53291", "wss://private.example/sensitive-credential"
	remoteID := privateIP + ":53291"
	makeSample := func(at time.Time, a, b uint32) peerSnapshot {
		selected := webrtc.ICECandidatePairStats{ID: "pair1", LocalCandidateID: privateLocal, RemoteCandidateID: remoteID,
			State: webrtc.StatsICECandidatePairStateSucceeded, Nominated: true, PacketsReceived: a, BytesReceived: uint64(a) * 2}
		other := webrtc.ICECandidatePairStats{ID: "pair2", LocalCandidateID: privateLocal, RemoteCandidateID: "other",
			State: webrtc.StatsICECandidatePairStateWaiting, PacketsReceived: b}
		report := webrtc.StatsReport{}
		report[remoteID] = webrtc.ICECandidateStats{ID: remoteID, Type: webrtc.StatsTypeRemoteCandidate, IP: privateIP, Port: 53291,
			URL: privateURL, CandidateType: webrtc.ICECandidateTypeHost, Protocol: "udp"}
		report["other"] = webrtc.ICECandidateStats{ID: "other", Type: webrtc.StatsTypeRemoteCandidate, IP: "2001:db8::42",
			CandidateType: webrtc.ICECandidateTypePrflx, Protocol: "tcp"}
		report[selected.ID], report[other.ID] = selected, other
		sample := peerSnapshot{at: at}
		sample.stats.addReport(report, selected, true, webrtc.DataChannelStats{}, false)
		return sample
	}
	d.selectedPairChanged(&webrtc.ICECandidatePair{})
	now = now.Add(5 * time.Second)
	d.selectedPairChanged(&webrtc.ICECandidatePair{})
	baseline := makeSample(now, 100, 20)
	now = now.Add(10 * time.Second)
	last := makeSample(now, 120, 25)
	last.pairBaseline = baseline.stats.pairs
	d.lastSelectedSample.Store(&last)
	now = now.Add(time.Second)
	live := makeSample(now, 150, 25)
	live.pairBaseline = last.stats.pairs
	failed := peerSnapshot{at: now, allowCandidateFallback: true, pairBaseline: last.stats.pairs}
	failed.stats.addReport(webrtc.StatsReport{}, webrtc.ICECandidatePairStats{}, false, webrtc.DataChannelStats{}, false)
	for _, tc := range []struct {
		name, source string
		sample       peerSnapshot
		delta, total float64
	}{{"disconnected", "live", live, 30, 120}, {"failed", "last_sample", failed, 20, 100}} {
		t.Run(tc.name, func(t *testing.T) {
			log := logDiagAttrs(t, d.attrs(tc.sample, &last, &last))
			for _, key := range []string{"candidates_source", "candidate_pairs_source", "selected_candidate_ids_source"} {
				assertDiag(t, log, key, tc.source)
			}
			if tc.name == "failed" {
				assertDiag(t, log, "candidate_snapshot_age_ms", float64(1000))
				assertDiag(t, log, "selected_pair.present", false)
			}
			assertDiag(t, log, "selected_pair_change_count", float64(2))
			assertDiag(t, log, "last_selected_pair_change_age_ms", float64(11000))
			assertDiag(t, log, "remote_candidate_count", float64(2))
			assertDiag(t, log, "candidate_pair_count", float64(2))
			assertDiag(t, log, "selected_local_candidate_id", candidateIndex(privateLocal))
			assertDiag(t, log, "selected_remote_candidate_id", candidateIndex(remoteID))
			candidate := log["remote_candidates"].([]any)[0].(map[string]any)
			assertDiag(t, candidate, "candidate_id", candidateIndex(remoteID))
			assertDiag(t, candidate, "selected", true)
			assertDiag(t, candidate, "candidate_type", "host")
			assertDiag(t, candidate, "address_family", "ipv4")
			assertDiag(t, candidate, "protocol", "udp")
			pair := log["candidate_pairs"].([]any)[0].(map[string]any)
			assertDiag(t, pair, "pair_state", "succeeded")
			assertDiag(t, pair, "nominated", true)
			assertDiag(t, pair, "packets_received_delta", tc.delta)
			assertDiag(t, pair, "bytes_received_delta", 2*tc.delta)
			previous := log["previous_candidate_pairs"].([]any)[0].(map[string]any)
			assertDiag(t, previous, "packets_received", tc.total)
			delete(log, "time")
			line, err := json.Marshal(log)
			if err != nil {
				t.Fatal(err)
			}
			for _, private := range []string{privateIP, "2001:db8::42", "53291", "private.example", "sensitive-credential"} {
				if strings.Contains(string(line), private) {
					t.Errorf("diagnostics leaked %q", private)
				}
			}
		})
	}
}
