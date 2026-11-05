package config

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
)

// ZoeTranscriptToken validates the optional transcript destination and reads its
// credential at startup. Either empty setting disables forwarding.
func (c Config) ZoeTranscriptToken() (string, error) {
	if strings.TrimSpace(c.ZoeURL) == "" || strings.TrimSpace(c.ZoeTokenFile) == "" {
		return "", nil
	}
	u, err := url.Parse(c.ZoeURL)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("invalid VOICE_ZOE_URL")
	}
	local := strings.EqualFold(u.Hostname(), "localhost")
	if ip := net.ParseIP(u.Hostname()); ip != nil {
		local = ip.IsLoopback()
	}
	if u.Scheme != "https" && !(u.Scheme == "http" && local) {
		return "", fmt.Errorf("VOICE_ZOE_URL requires HTTPS (HTTP allowed only for localhost)")
	}
	b, err := os.ReadFile(c.ZoeTokenFile)
	if err != nil {
		return "", fmt.Errorf("cannot read VOICE_ZOE_TOKEN_FILE")
	}
	token := strings.TrimSpace(string(b))
	if strings.ContainsAny(token, "\r\n") {
		return "", fmt.Errorf("invalid VOICE_ZOE_TOKEN_FILE contents")
	}
	return token, nil
}
