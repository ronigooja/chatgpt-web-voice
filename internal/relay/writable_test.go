package relay

import "testing"

func TestWritableAllowsLargeMessageOnEmptyBuffer(t *testing.T) {
	for _, tc := range []struct{ amount uint64; size int; wait bool }{
		{0, mediaDataBufferedMaxSize + 1, false},
		{1, mediaDataBufferedMaxSize + 1, true},
		{1, mediaDataBufferedMaxSize - 1, false},
		{2, mediaDataBufferedMaxSize - 1, true},
	} {
		if got := mustWaitWritable(tc.amount, tc.size); got != tc.wait {
			t.Fatalf("amount=%d size=%d: wait=%t", tc.amount, tc.size, got)
		}
	}
}
