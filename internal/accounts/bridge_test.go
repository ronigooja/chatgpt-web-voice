package accounts

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/secretbox"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/store"
)

func bridgePool(t *testing.T, path string) *Pool {
	t.Helper()
	db, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	box, err := secretbox.New([]byte("01234567890123456789012345678901"))
	if err != nil {
		t.Fatal(err)
	}
	return NewPoolFromDB(db).WithBox(box)
}
func bridgeFixture() BridgePayload {
	return BridgePayload{Identity: "synthetic@example.test", CredentialType: "chatgpt_web", Credential: BridgeCredential{AccessToken: "synthetic-token<&>测试\u2028\u2029", DeviceID: "测试<&>\u2028\u2029"}, Route: BridgeRoute{Type: "proxy", URL: "socks5h://user:secret@127.0.0.1:9876"}}
}
func TestBridgeCanonicalCredentialDigest(t *testing.T) {
	digest, err := BridgeDigest(BridgeCredential{AccessToken: "synthetic", DeviceID: "测试<&>\u2028\u2029"})
	if err != nil || digest != "5fcc9d4ab814aedce4447a6dd1243d8f67353cf8270f4d23eeaef6aa6286502d" {
		t.Fatalf("digest=%s err=%v", digest, err)
	}
	raw, err := CanonicalJSON(map[string]string{"device_id": `literal\u2028`})
	if err != nil || string(raw) != `{"device_id":"literal\\u2028"}` {
		t.Fatalf("literal separator changed: %q %v", raw, err)
	}
}
func TestBridgePersistenceConflictAndNativeMutation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "synthetic.db")
	p := bridgePool(t, path)
	b := bridgeFixture()
	v, err := p.BridgePut("bridge-synthetic", "synthetic-key", "digest-one", b)
	if err != nil {
		t.Fatal(err)
	}
	if v.Route != b.Route {
		t.Fatalf("route=%+v", v.Route)
	}
	list, err := p.List()
	if err != nil || len(list) != 1 || list[0].DeviceID != b.Credential.DeviceID || list[0].AccessToken != b.Credential.AccessToken {
		t.Fatalf("list=%+v err=%v", list, err)
	}
	var token, proxy, device string
	if err := p.db.Conn().QueryRow(`SELECT access_token,proxy,device_id FROM accounts a JOIN bridge_accounts b ON a.id=b.account_id`).Scan(&token, &proxy, &device); err != nil {
		t.Fatal(err)
	}
	for _, v := range []string{token, proxy, device} {
		if !secretbox.IsSealed(v) {
			t.Fatal("plaintext secret persisted")
		}
	}
	p2 := bridgePool(t, path)
	read, err := p2.BridgeGet("bridge-synthetic")
	if err != nil || read != v {
		t.Fatalf("restart read=%+v err=%v", read, err)
	}
	b.Identity = "different@example.test"
	if _, err = p.BridgePut("bridge-synthetic", "other", "other", b); !errors.Is(err, ErrConflict) {
		t.Fatalf("identity conflict=%v", err)
	}
	b = bridgeFixture()
	if _, err = p.BridgePut("bridge-other", "other", "other", b); !errors.Is(err, ErrConflict) {
		t.Fatalf("token conflict=%v", err)
	}
	if _, err = p.BridgePut("bridge-synthetic", "synthetic-key", "different", b); !errors.Is(err, ErrConflict) {
		t.Fatalf("idempotency conflict=%v", err)
	}
	replacement := "rotated-synthetic-token"
	direct := "direct"
	if _, err = p.Update(list[0].ID, AccountUpdate{Email: b.Identity, AccessToken: &replacement, Proxy: &direct}); err != nil {
		t.Fatal(err)
	}
	replay, err := p.BridgePut("bridge-synthetic", "synthetic-key", "digest-one", b)
	if err != nil {
		t.Fatal(err)
	}
	if replay.CredentialDigest == v.CredentialDigest || replay.Route.Type != "direct" {
		t.Fatal("replay overwrote newer native state")
	}
	current, _ := p.Get(list[0].ID)
	if current.AccessToken != replacement {
		t.Fatal("rotated token overwritten")
	}
	if _, err = p.BridgeGet("absent"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing=%v", err)
	}
}
func TestBridgePreservesUnrelatedNativeAccounts(t *testing.T) {
	p := bridgePool(t, filepath.Join(t.TempDir(), "synthetic.db"))
	b := bridgeFixture()
	b.Credential.AccessToken = "synthetic-shared-native-token"
	a, err := p.Create(Account{Email: "native@example.test", AccessToken: b.Credential.AccessToken})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.BridgePut("bridge-new", "key", "digest", b); !errors.Is(err, ErrConflict) {
		t.Fatalf("adopted unrelated account: %v", err)
	}
	got, _ := p.Get(a.ID)
	if got.Email != a.Email || got.Proxy != "" {
		t.Fatal("native account changed")
	}
	for _, route := range []BridgeRoute{{Type: ""}, {Type: "proxy", URL: "http://secret@host:99999"}, {Type: "direct", URL: "http://proxy"}} {
		b.Route = route
		if b.Validate() == nil {
			t.Fatalf("accepted invalid route %+v", route)
		}
	}
	if strings.Contains(got.Proxy, "secret") {
		t.Fatal("unexpected proxy")
	}
}

func TestBridgeTransactionRollbackAndDeletedMappingReplay(t *testing.T) {
	p := bridgePool(t, filepath.Join(t.TempDir(), "synthetic.db"))
	b := bridgeFixture()
	if _, err := p.db.Conn().Exec(`CREATE TRIGGER synthetic_bridge_failure BEFORE INSERT ON bridge_accounts BEGIN SELECT RAISE(ABORT,'synthetic'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := p.BridgePut("bridge-rollback", "key", "digest", b); err == nil {
		t.Fatal("injected failure ignored")
	}
	list, err := p.List()
	if err != nil || len(list) != 0 {
		t.Fatal("partial native write survived rollback")
	}
	if _, err = p.db.Conn().Exec(`DROP TRIGGER synthetic_bridge_failure`); err != nil {
		t.Fatal(err)
	}
	if _, err = p.BridgePut("bridge-rollback", "key", "digest", b); err != nil {
		t.Fatal(err)
	}
	list, _ = p.List()
	if err = p.Delete(list[0].ID); err != nil {
		t.Fatal(err)
	}
	if _, err = p.BridgePut("bridge-rollback", "key", "digest", b); !errors.Is(err, ErrConflict) {
		t.Fatalf("deleted key replay recreated account: %v", err)
	}
	list, _ = p.List()
	if len(list) != 0 {
		t.Fatal("deleted account recreated")
	}
}
func TestBridgeMigratesLegacyPlaintextProxy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "synthetic.db")
	p := bridgePool(t, path)
	_, err := p.db.Conn().Exec(`INSERT INTO accounts(email,access_token,proxy) VALUES(?,?,?)`, "legacy@example.test", "synthetic-legacy-token", "http://user:synthetic-secret@127.0.0.1:9876")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.SealStoredTokens(); err != nil {
		t.Fatal(err)
	}
	var stored string
	if err = p.db.Conn().QueryRow(`SELECT proxy FROM accounts`).Scan(&stored); err != nil || !secretbox.IsSealed(stored) {
		t.Fatal("legacy proxy was not sealed")
	}
	p2 := bridgePool(t, path)
	list, err := p2.List()
	if err != nil || len(list) != 1 || list[0].Proxy != "http://user:synthetic-secret@127.0.0.1:9876" {
		t.Fatalf("legacy proxy changed after restart: %+v %v", list, err)
	}
	if count, err := p2.SealStoredTokens(); err != nil || count != 0 {
		t.Fatalf("non-idempotent migration: %d %v", count, err)
	}
}

func TestBridgeMappingIdentityCannotBeReassignedByNativeEdit(t *testing.T) {
	p := bridgePool(t, filepath.Join(t.TempDir(), "synthetic.db"))
	b := bridgeFixture()
	if _, err := p.BridgePut("bridge-immutable", "key", "digest", b); err != nil {
		t.Fatal(err)
	}
	list, _ := p.List()
	changed := "other@example.test"
	if _, err := p.Update(list[0].ID, AccountUpdate{Email: changed}); err != nil {
		t.Fatal(err)
	}
	read, err := p.BridgeGet("bridge-immutable")
	if err != nil || read.Identity != changed {
		t.Fatal("readback must reflect native identity edit")
	}
	b.Identity = changed
	if _, err := p.BridgePut("bridge-immutable", "new-key", "new-digest", b); !errors.Is(err, ErrConflict) {
		t.Fatalf("mapping reassigned: %v", err)
	}
}
