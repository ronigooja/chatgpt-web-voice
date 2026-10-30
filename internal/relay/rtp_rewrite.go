package relay

import (
	"sync"

	"github.com/pion/rtp"
)

// opusSamplesPerPacket is the RTP timestamp step of one 20 ms Opus packet at 48 kHz.
const opusSamplesPerPacket = 960

// rtpRewriter keeps an outgoing RTP stream continuous when its source changes.
// Sequence numbers and timestamps are re-based onto the last values it emitted,
// so a receiver sees one uninterrupted stream. SSRC is set by the sending track.
type rtpRewriter struct {
	mu sync.Mutex

	started bool
	// last emitted values
	lastSeq uint16
	lastTS  uint32
	// offsets applied to the current source
	seqOffset uint16
	tsOffset  uint32
	// last values seen from the current source
	srcSeq  uint16
	srcTS   uint32
	srcSeen bool
	// source identifies the current upstream leg.
	source int
}

// rewrite re-bases packet for the given source. A change of source starts a new
// segment that continues one packet after the last emitted packet.
func (r *rtpRewriter) rewrite(source int, p *rtp.Packet) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.started {
		r.started = true
		r.source = source
		r.srcSeen = false
	}
	if source != r.source || !r.srcSeen {
		if r.srcSeen || source != r.source {
			// New source: continue from the last emitted position.
			r.seqOffset = r.lastSeq + 1 - p.SequenceNumber
			r.tsOffset = r.lastTS + opusSamplesPerPacket - p.Timestamp
		} else {
			// First packet ever: keep the source numbering.
			r.seqOffset = 0
			r.tsOffset = 0
		}
		r.source = source
		r.srcSeen = true
	}
	p.SequenceNumber += r.seqOffset
	p.Timestamp += r.tsOffset
	r.lastSeq = p.SequenceNumber
	r.lastTS = p.Timestamp
	r.srcSeq = p.SequenceNumber
	r.srcTS = p.Timestamp
}
