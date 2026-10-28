package accounts

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"unicode"
)

// BridgeCredential is the complete web credential actually used by the gateway.
type BridgeCredential struct {
	AccessToken string `json:"access_token"`
	DeviceID    string `json:"device_id,omitempty"`
}
type BridgeRoute struct {
	Type string `json:"type"`
	URL  string `json:"url,omitempty"`
}
type BridgePayload struct {
	Identity       string           `json:"identity"`
	CredentialType string           `json:"credential_type"`
	Credential     BridgeCredential `json:"credential"`
	Route          BridgeRoute      `json:"route"`
	ExpiresAt      json.RawMessage  `json:"expires_at"`
}
type BridgeView struct {
	Identity         string      `json:"identity"`
	CredentialType   string      `json:"credential_type"`
	Route            BridgeRoute `json:"route"`
	CredentialDigest string      `json:"credential_digest"`
}

// CanonicalJSON matches Python sorted-key compact JSON for the bridge's string
// credentials. Disable HTML escapes and preserve literal Unicode separators.
func CanonicalJSON(value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var object any
	if err = dec.Decode(&object); err != nil {
		return nil, err
	}
	var out bytes.Buffer
	enc := json.NewEncoder(&out)
	enc.SetEscapeHTML(false)
	if err = enc.Encode(object); err != nil {
		return nil, err
	}
	result := bytes.TrimSuffix(out.Bytes(), []byte("\n"))
	var exact bytes.Buffer
	for i := 0; i < len(result); i++ {
		if result[i] == '\\' && i+1 < len(result) {
			if i+6 <= len(result) && (string(result[i:i+6]) == `\u2028` || string(result[i:i+6]) == `\u2029`) {
				if result[i+5] == '8' {
					exact.WriteRune('\u2028')
				} else {
					exact.WriteRune('\u2029')
				}
				i += 5
				continue
			}
			exact.WriteByte(result[i])
			i++
			exact.WriteByte(result[i])
			continue
		}
		exact.WriteByte(result[i])
	}
	return exact.Bytes(), nil
}
func BridgeDigest(value any) (string, error) {
	raw, err := CanonicalJSON(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
func ValidBridgeID(id string) bool {
	if id == "" || len(id) > 200 || strings.HasPrefix(id, ".") || strings.Contains(id, "..") {
		return false
	}
	for _, c := range id {
		if !strings.ContainsRune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.@+", c) {
			return false
		}
	}
	return true
}
func (b BridgePayload) Validate() error {
	invalid := &Error{Message: "invalid bridge account payload"}
	if strings.TrimSpace(b.Identity) == "" || b.Identity != strings.TrimSpace(b.Identity) || b.CredentialType != "chatgpt_web" || b.Credential.AccessToken == "" || strings.TrimSpace(b.Credential.AccessToken) == "" {
		return invalid
	}
	if b.Credential.DeviceID != "" && strings.TrimSpace(b.Credential.DeviceID) == "" {
		return invalid
	}
	for _, text := range []string{b.Identity, b.Credential.AccessToken, b.Credential.DeviceID} {
		if strings.IndexFunc(text, unicode.IsControl) >= 0 {
			return invalid
		}
	}
	if len(b.ExpiresAt) > 0 && string(b.ExpiresAt) != "null" {
		var expiry float64
		if json.Unmarshal(b.ExpiresAt, &expiry) != nil {
			return invalid
		}
	}
	switch b.Route.Type {
	case "direct":
		if b.Route.URL != "" {
			return invalid
		}
	case "proxy":
		u, err := url.Parse(b.Route.URL)
		if err != nil || u.Hostname() == "" || u.Opaque != "" || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" || strings.IndexFunc(b.Route.URL, unicode.IsSpace) >= 0 {
			return invalid
		}
		switch u.Scheme {
		case "http", "https", "socks5", "socks5h":
		default:
			return invalid
		}
		if port := u.Port(); port != "" {
			var n int
			for _, c := range port {
				if c < '0' || c > '9' {
					return invalid
				}
				n = n*10 + int(c-'0')
				if n > 65535 {
					return invalid
				}
			}
			if n == 0 {
				return invalid
			}
		}
	default:
		return invalid
	}
	return nil
}

// BridgeGet computes readback from the current native row, never a request digest.
func (p *Pool) BridgeGet(remoteID string) (BridgeView, error) {
	p.db.Lock()
	defer p.db.Unlock()
	return p.bridgeGetUnlocked(remoteID)
}
func (p *Pool) bridgeGetUnlocked(remoteID string) (BridgeView, error) {
	var id int64
	if err := p.db.Conn().QueryRow(`SELECT account_id FROM bridge_accounts WHERE remote_id=?`, remoteID).Scan(&id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return BridgeView{}, ErrNotFound
		}
		return BridgeView{}, err
	}
	account, err := p.getUnlocked(id)
	if err != nil {
		return BridgeView{}, err
	}
	route := BridgeRoute{Type: "proxy", URL: account.Proxy}
	if account.Proxy == "direct" {
		route = BridgeRoute{Type: "direct"}
	} else if account.Proxy == "" {
		return BridgeView{}, ErrConflict
	}
	digest, err := BridgeDigest(BridgeCredential{AccessToken: account.AccessToken, DeviceID: account.DeviceID})
	if err != nil {
		return BridgeView{}, err
	}
	return BridgeView{Identity: account.Email, CredentialType: "chatgpt_web", Route: route, CredentialDigest: digest}, nil
}

// BridgePut atomically binds one caller-owned stable ID to a native account.
// Existing unrelated accounts are never adopted, even if their token matches.
func (p *Pool) BridgePut(remoteID, key, payloadDigest string, b BridgePayload) (BridgeView, error) {
	if !ValidBridgeID(remoteID) || key == "" || len(key) > 512 {
		return BridgeView{}, &Error{Message: "invalid bridge account request"}
	}
	if err := b.Validate(); err != nil {
		return BridgeView{}, err
	}
	stored, hash, err := p.sealExactToken(b.Credential.AccessToken)
	if err != nil {
		return BridgeView{}, err
	}
	proxy := b.Route.URL
	if b.Route.Type == "direct" {
		proxy = "direct"
	}
	sealedProxy, err := p.sealOptional(proxy)
	if err != nil {
		return BridgeView{}, err
	}
	device, err := p.sealOptional(b.Credential.DeviceID)
	if err != nil {
		return BridgeView{}, err
	}
	p.db.Lock()
	defer p.db.Unlock()
	tx, err := p.db.Conn().Begin()
	if err != nil {
		return BridgeView{}, err
	}
	defer tx.Rollback()
	var oldID, oldDigest string
	err = tx.QueryRow(`SELECT remote_id,payload_digest FROM bridge_idempotency WHERE key=?`, key).Scan(&oldID, &oldDigest)
	if err == nil && (oldID != remoteID || oldDigest != payloadDigest) {
		return BridgeView{}, ErrConflict
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return BridgeView{}, err
	}
	replay := err == nil
	var id int64
	var identity, nativeIdentity string
	err = tx.QueryRow(`SELECT a.id,b.identity,a.email FROM bridge_accounts b JOIN accounts a ON a.id=b.account_id WHERE b.remote_id=?`, remoteID).Scan(&id, &identity, &nativeIdentity)
	exists := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return BridgeView{}, err
	}
	if exists && (!strings.EqualFold(identity, b.Identity) || !strings.EqualFold(nativeIdentity, b.Identity)) {
		return BridgeView{}, ErrConflict
	}
	if replay {
		// Never replay an old write over newer native credentials or routing.
		if !exists {
			return BridgeView{}, ErrConflict
		}
		if err = tx.Commit(); err != nil {
			return BridgeView{}, err
		}
		return p.bridgeGetUnlocked(remoteID)
	}
	var duplicate int
	if err = tx.QueryRow(`SELECT COUNT(*) FROM accounts WHERE token_hash=? AND id<>?`, hash, id).Scan(&duplicate); err != nil {
		return BridgeView{}, err
	}
	if duplicate > 0 {
		return BridgeView{}, ErrConflict
	}
	if exists {
		_, err = tx.Exec(`UPDATE accounts SET email=?,access_token=?,token_hash=?,proxy=?,updated_at=CURRENT_TIMESTAMP WHERE id=?`, b.Identity, stored, hash, sealedProxy, id)
	} else {
		var result sql.Result
		result, err = tx.Exec(`INSERT INTO accounts(email,access_token,token_hash,proxy) VALUES(?,?,?,?)`, b.Identity, stored, hash, sealedProxy)
		if err == nil {
			id, err = result.LastInsertId()
		}
	}
	if err != nil {
		return BridgeView{}, err
	}
	expiry := string(b.ExpiresAt)
	if expiry == "" {
		expiry = "null"
	}
	_, err = tx.Exec(`INSERT INTO bridge_accounts(remote_id,identity,account_id,device_id,expires_at) VALUES(?,?,?,?,?) ON CONFLICT(remote_id) DO UPDATE SET device_id=excluded.device_id,expires_at=excluded.expires_at`, remoteID, b.Identity, id, device, expiry)
	if err != nil {
		return BridgeView{}, err
	}
	_, err = tx.Exec(`INSERT INTO bridge_idempotency(key,remote_id,payload_digest) VALUES(?,?,?)`, key, remoteID, payloadDigest)
	if err != nil {
		return BridgeView{}, err
	}
	if err = tx.Commit(); err != nil {
		return BridgeView{}, err
	}
	return p.bridgeGetUnlocked(remoteID)
}
