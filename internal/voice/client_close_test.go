package voice

import "testing"

func TestOwnedEndReasonClientDataChannelClose(t *testing.T) {
	if got := ownedEndReason("client_data_channel_closed"); got != "media_lost" {
		t.Fatalf("owned end reason = %q, want media_lost", got)
	}
}
