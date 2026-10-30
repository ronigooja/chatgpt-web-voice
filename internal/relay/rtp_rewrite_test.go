package relay

import (
	"testing"

	"github.com/pion/rtp"
)

func pkt(seq uint16, ts uint32) *rtp.Packet {
	return &rtp.Packet{Header: rtp.Header{SequenceNumber: seq, Timestamp: ts}}
}

func TestRewriterKeepsFirstSourceUntouched(t *testing.T) {
	var r rtpRewriter
	for i := uint16(0); i < 3; i++ {
		p := pkt(100+i, 5000+uint32(i)*960)
		r.rewrite(1, p)
		if p.SequenceNumber != 100+i || p.Timestamp != 5000+uint32(i)*960 {
			t.Fatalf("first source must pass through, got %d/%d", p.SequenceNumber, p.Timestamp)
		}
	}
}

func TestRewriterContinuesAcrossSourceChange(t *testing.T) {
	var r rtpRewriter
	var last *rtp.Packet
	for i := uint16(0); i < 3; i++ {
		last = pkt(100+i, 5000+uint32(i)*960)
		r.rewrite(1, last)
	}
	// New source with unrelated numbering.
	for i := uint16(0); i < 3; i++ {
		p := pkt(40000+i, 7_000_000+uint32(i)*960)
		r.rewrite(2, p)
		wantSeq := last.SequenceNumber + 1 + i
		wantTS := last.Timestamp + 960*(1+uint32(i))
		if p.SequenceNumber != wantSeq || p.Timestamp != wantTS {
			t.Fatalf("packet %d: got %d/%d want %d/%d", i, p.SequenceNumber, p.Timestamp, wantSeq, wantTS)
		}
	}
}

func TestRewriterHandlesSequenceWraparound(t *testing.T) {
	var r rtpRewriter
	r.rewrite(1, pkt(65534, 4294966000))
	p := pkt(65535, 4294966000+960)
	r.rewrite(1, p)
	next := pkt(7, 100)
	r.rewrite(2, next)
	if next.SequenceNumber != 0 {
		t.Fatalf("sequence must wrap to 0, got %d", next.SequenceNumber)
	}
	if next.Timestamp != p.Timestamp+960 {
		t.Fatalf("timestamp must continue across wrap: %d vs %d", next.Timestamp, p.Timestamp+960)
	}
}

func TestRewriterSameSourceDoesNotShift(t *testing.T) {
	var r rtpRewriter
	r.rewrite(1, pkt(1, 960))
	r.rewrite(2, pkt(500, 100000)) // switch
	a := pkt(501, 100960)
	r.rewrite(2, a)
	b := pkt(502, 101920)
	r.rewrite(2, b)
	if b.SequenceNumber-a.SequenceNumber != 1 || b.Timestamp-a.Timestamp != 960 {
		t.Fatalf("steady source must keep its spacing, got %d/%d", b.SequenceNumber-a.SequenceNumber, b.Timestamp-a.Timestamp)
	}
}
