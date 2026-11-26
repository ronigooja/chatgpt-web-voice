package voice

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/callsessions"
)

func TestWebWMSessionInOwnedMode(t *testing.T) {
	const answer = "v=0\r\nweb-answer\r\n"
	f, relay := newRelayFixture(t, []string{fixtureTokenA, fixtureTokenB}, 1, func(w http.ResponseWriter, _ *http.Request, _ wmCapture) {
		_, _ = io.WriteString(w, answer)
	})
	svc := f.svc
	svc.cfg.VoiceUpstreamRoute = "avas"
	svc.cfg.ZoeGatewayAPIKeyID = 42
	svc.avasURL = "http://[invalid"
	svc.WithCallSessions(callsessions.NewStore(f.pool.DB()))
	fixtureSQL(t, f.pool.DB(), `INSERT INTO voice_media_sessions
		(voice_session_id, media_generation, owner, account_id, lease_id, lease_generation, state, created_at, updated_at, zoe_generation)
		VALUES ('owned',1,'api_key:42',1,'lease',1,'active',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,1)`)
	offer := readTestdata(t, "wm_offer.sdp")
	admin := CreateSessionRequest{Owner: "admin:fixture", OfferSDP: offer}
	for _, req := range []CreateSessionRequest{
		admin,
		{Owner: "api_key:42", OfferSDP: offer},
		(CreateSessionRequest{Owner: "api_key:42", OfferSDP: offer}).ForAdminWeb(),
	} {
		_, err := svc.CreateSession(req)
		var se *ServiceError
		if !errors.As(err, &se) || se.StatusCode != 503 || se.Message != "media admission is gateway-owned" {
			t.Fatalf("unreserved admission: %v", err)
		}
	}
	pinned := admin
	pinned.AccountID = 1
	_, err := svc.CreateSession(pinned.ForAdminWeb())
	var se *ServiceError
	if !errors.As(err, &se) || se.StatusCode != 503 {
		t.Fatalf("reserved sticky account: %v", err)
	}
	res, err := svc.CreateSession(admin.ForAdminWeb())
	if err != nil {
		t.Fatal(err)
	}
	if res.AccountID != 2 || res.AnswerSDP != answer || res.VoiceSessionGeneration != 0 || len(relay.sessions) != 0 {
		t.Fatal("expected alternate account and direct upstream answer without relay")
	}
	capture := f.only(t)
	wantOffer, err := normalizeSDP(offer)
	if err != nil || capture.Path != "/realtime/wm" || capture.Fields["sdp"] != wantOffer {
		t.Fatal("expected one WM request with browser offer")
	}
	id := res.VoiceSessionID
	if _, err := svc.UpdateSessionContext(admin.Owner, id, UpstreamContext{Text: "fixture", ConversationID: "active"}); err != nil {
		t.Fatalf("live WM context update: %v", err)
	}
	if !svc.ReleaseSession(admin.Owner, id) {
		t.Fatal("web session not released")
	}
	row, err := svc.records.Get(admin.Owner, id)
	if err != nil || row.Status != callsessions.StatusReleased || row.UpstreamRoute != "wm" {
		t.Fatal("expected stored WM route on released session")
	}
	updated, err := svc.UpdateSessionContext(admin.Owner, id, UpstreamContext{ConversationID: "late"})
	if err != nil || updated.ConversationID != "late" {
		t.Fatalf("durable WM context update: %v", err)
	}
}

func TestZoeOwnedDialStillPostsAVAS(t *testing.T) {
	svc, db, relay, _ := newOfflineOwnedCreation(t, 1)
	svc.WithAVASURL("https://fixture.invalid/avas")
	setOwnedCurl(t, svc, `for url; do :; done
[ "$url" = 'https://fixture.invalid/avas' ] || exit 1
cat >/dev/null
`+ownedRejection)
	_, err := svc.DialClient(context.Background(), ownedTestKey, ClientDialRequest{RequestID: "fixture", OfferSDP: readTestdata(t, "wm_offer.sdp")})
	if err != unavailableCall || len(relay.sessions) != 1 {
		t.Fatalf("owned AVAS rejection: %v", err)
	}
	assertOwnedClaim(t, db, "never_established")
}
