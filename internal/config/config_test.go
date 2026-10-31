package config

import (
	"strings"
	"testing"
)

func TestValidateRequiresUsernameAndPassword(t *testing.T) {
	cfg := Config{
		Environment:         EnvironmentDevelopment,
		DatabaseFile:        "voice.db",
		AuthSessionTTL:      60,
		LoginMaxFailures:    8,
		LoginWindowSeconds:  900,
		LoginLockoutSeconds: 900,
		UpstreamTransport:   TransportTLSClient,
		TLSProfile:          DefaultTLSProfile,
		DeviceID:            "device-test",
		SessionID:           "session-test",
		LogFormat:           "json",
		LogLevel:            "info",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected missing username to fail validation")
	}
	cfg.AuthUsername = "admin"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected missing password to fail validation")
	}
	cfg.AuthPassword = "secret"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected missing token encryption key to fail validation")
	}
	cfg.TokenEncryptionKey = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid auth configuration: %v", err)
	}
}

func TestLoadDefaultsToDevelopmentEnvironment(t *testing.T) {
	t.Setenv("VOICE_ENV", "")
	cfg := Load()
	if cfg.Environment != EnvironmentDevelopment {
		t.Fatalf("unexpected default environment: %q", cfg.Environment)
	}
}

func TestLoadDefaultsTo8090(t *testing.T) {
	t.Setenv("VOICE_LISTEN_ADDR", "")
	cfg := Load()
	if cfg.ListenAddr != "0.0.0.0:8090" {
		t.Fatalf("unexpected default listen address: %q", cfg.ListenAddr)
	}
}

func TestLoadNormalizesEnvironment(t *testing.T) {
	t.Setenv("VOICE_ENV", " PRODUCTION ")
	cfg := Load()
	if cfg.Environment != EnvironmentProduction {
		t.Fatalf("unexpected normalized environment: %q", cfg.Environment)
	}
}

func TestLoadForcesTLSVerificationInProduction(t *testing.T) {
	t.Setenv("VOICE_ENV", "production")
	t.Setenv("VOICE_SKIP_SSL_VERIFY", "true")
	cfg := Load()
	if cfg.SkipSSLVerify {
		t.Fatal("production must not allow upstream TLS verification to be disabled")
	}
}

func TestLoadAllowsSkipTLSVerificationInDevelopment(t *testing.T) {
	t.Setenv("VOICE_ENV", "development")
	t.Setenv("VOICE_SKIP_SSL_VERIFY", "true")
	cfg := Load()
	if !cfg.SkipSSLVerify {
		t.Fatal("development skip-verify setting was not loaded")
	}
}

func TestLoadVerifiesTLSByDefaultInDevelopment(t *testing.T) {
	t.Setenv("VOICE_ENV", "development")
	t.Setenv("VOICE_SKIP_SSL_VERIFY", "")
	cfg := Load()
	if cfg.SkipSSLVerify {
		t.Fatal("upstream TLS verification must be enabled by default")
	}
}

func TestValidateRejectsUnknownEnvironment(t *testing.T) {
	cfg := Config{
		Environment:         "staging",
		AuthUsername:        "admin",
		AuthPassword:        "secret",
		TokenEncryptionKey:  "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		DatabaseFile:        "voice.db",
		AuthSessionTTL:      60,
		LoginMaxFailures:    8,
		LoginWindowSeconds:  900,
		LoginLockoutSeconds: 900,
		UpstreamTransport:   TransportTLSClient,
		TLSProfile:          DefaultTLSProfile,
		DeviceID:            "device-test",
		SessionID:           "session-test",
		LogFormat:           "json",
		LogLevel:            "info",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected unknown environment to fail validation")
	}
}

func TestLoadGeneratesGlobalFingerprint(t *testing.T) {
	t.Setenv("VOICE_DEVICE_ID", "")
	t.Setenv("VOICE_SESSION_ID", "")
	cfg := Load()
	if cfg.DeviceID == "" || cfg.SessionID == "" {
		t.Fatalf("expected auto-generated fingerprint, got device=%q session=%q", cfg.DeviceID, cfg.SessionID)
	}
	if cfg.UpstreamTransport != TransportTLSClient {
		t.Fatalf("default transport=%q", cfg.UpstreamTransport)
	}
	if cfg.TLSProfile != DefaultTLSProfile {
		t.Fatalf("default TLS profile=%q", cfg.TLSProfile)
	}
	if cfg.Impersonate != "edge_101" {
		t.Fatalf("default impersonate=%q", cfg.Impersonate)
	}
	if !strings.Contains(cfg.DefaultUA, "Edg/143") {
		t.Fatalf("expected Edge 143 UA, got %q", cfg.DefaultUA)
	}
	if !strings.Contains(cfg.SecCHUA, "Microsoft Edge") {
		t.Fatalf("expected Edge sec-ch-ua, got %q", cfg.SecCHUA)
	}
	if cfg.ClientVersion != "prod-be885abbfcfe7b1f511e88b3003d9ee44757fbad" {
		t.Fatalf("client version=%q", cfg.ClientVersion)
	}
	if cfg.ClientBuildNumber != "5955942" {
		t.Fatalf("client build=%q", cfg.ClientBuildNumber)
	}
}

func TestValidateRejectsUnknownTransport(t *testing.T) {
	cfg := Config{
		Environment:         EnvironmentDevelopment,
		AuthUsername:        "admin",
		AuthPassword:        "secret",
		TokenEncryptionKey:  "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		DatabaseFile:        "voice.db",
		AuthSessionTTL:      60,
		LoginMaxFailures:    8,
		LoginWindowSeconds:  900,
		LoginLockoutSeconds: 900,
		UpstreamTransport:   "browser",
		TLSProfile:          DefaultTLSProfile,
		DeviceID:            "d",
		SessionID:           "s",
		LogFormat:           "json",
		LogLevel:            "info",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected unknown transport to fail")
	}
}

func TestLoadPreservesPasswordWhitespace(t *testing.T) {
	t.Setenv("VOICE_AUTH_USERNAME", " admin ")
	t.Setenv("VOICE_AUTH_PASSWORD", " password with spaces ")
	t.Setenv("VOICE_DATABASE_FILE", "./test.db")
	t.Setenv("VOICE_LOG_FORMAT", "json")
	t.Setenv("VOICE_LOG_LEVEL", "info")
	cfg := Load()
	if cfg.AuthUsername != "admin" {
		t.Fatalf("unexpected username: %q", cfg.AuthUsername)
	}
	if cfg.AuthPassword != " password with spaces " {
		t.Fatalf("password whitespace was changed: %q", cfg.AuthPassword)
	}
}

func TestValidateProductionForbidsSkipSSLVerify(t *testing.T) {
	cfg := Config{
		Environment:         EnvironmentProduction,
		UpstreamTransport:   TransportTLSClient,
		TLSProfile:          DefaultTLSProfile,
		DeviceID:            "device-test",
		SessionID:           "session-test",
		AuthUsername:        "admin",
		AuthPassword:        "secret",
		TokenEncryptionKey:  "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		DatabaseFile:        "voice.db",
		AuthSessionTTL:      60,
		LoginMaxFailures:    8,
		LoginWindowSeconds:  900,
		LoginLockoutSeconds: 900,
		SkipSSLVerify:       true,
		LogFormat:           "json",
		LogLevel:            "info",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected production skip ssl verify to fail")
	}
	cfg.SkipSSLVerify = false
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected production config without skip-verify to pass: %v", err)
	}
}

func TestLoadAccountHealthDefaultsAndOverrides(t *testing.T) {
	for _, name := range []string{
		"VOICE_ACCOUNT_EXPIRY_GUARD_SECONDS", "VOICE_ACCOUNT_BACKOFF_BASE_SECONDS",
		"VOICE_ACCOUNT_BACKOFF_MAX_SECONDS", "VOICE_ACCOUNT_TRANSIENT_COOLDOWN_SECONDS",
		"VOICE_ACCOUNT_UNAUTHORIZED_COOLDOWN_SECONDS",
	} {
		t.Setenv(name, "")
	}
	cfg := Load()
	if cfg.AccountExpiryGuardSeconds != 300 || cfg.AccountBackoffBaseSeconds != 30 ||
		cfg.AccountBackoffMaxSeconds != 1800 || cfg.AccountTransientCooldownSeconds != 15 ||
		cfg.AccountUnauthorizedCooldownSeconds != 60 {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	t.Setenv("VOICE_ACCOUNT_EXPIRY_GUARD_SECONDS", "900")
	t.Setenv("VOICE_ACCOUNT_BACKOFF_BASE_SECONDS", "bad")
	if cfg := Load(); cfg.AccountExpiryGuardSeconds != 900 || cfg.AccountBackoffBaseSeconds != 30 {
		t.Fatalf("override/fallback failed: %+v", cfg)
	}
}

func TestValidateRejectsBadAccountHealthSettings(t *testing.T) {
	t.Setenv("VOICE_AUTH_USERNAME", "admin")
	t.Setenv("VOICE_AUTH_PASSWORD", "secret")
	t.Setenv("VOICE_TOKEN_ENCRYPTION_KEY", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	cfg := Load()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("baseline config invalid: %v", err)
	}
	bad := cfg
	bad.AccountTransientCooldownSeconds = -1
	if bad.Validate() == nil {
		t.Fatal("negative cooldown must fail")
	}
	bad = cfg
	bad.AccountBackoffBaseSeconds, bad.AccountBackoffMaxSeconds = 100, 10
	if bad.Validate() == nil {
		t.Fatal("base above max must fail")
	}
}

func TestRelayConfigDefaultsAndOverrides(t *testing.T) {
	cfg := Load()
	if !cfg.RelayEnabled || cfg.RelayMediaRoute != RelayMediaRouteAccount || cfg.RelayMaxSessions != 0 {
		t.Fatalf("unexpected relay defaults: %+v", cfg)
	}
	t.Setenv("VOICE_RELAY_ENABLED", "false")
	if cfg := Load(); cfg.RelayEnabled {
		t.Fatal("VOICE_RELAY_ENABLED=false must disable the relay")
	}
	t.Setenv("VOICE_RELAY_ENABLED", "true")
	t.Setenv("VOICE_RELAY_MEDIA_ROUTE", "DIRECT")
	t.Setenv("VOICE_RELAY_MAX_SESSIONS", "4")
	t.Setenv("VOICE_RELAY_UDP_PORT_MIN", "40000")
	t.Setenv("VOICE_RELAY_UDP_PORT_MAX", "40100")
	t.Setenv("VOICE_RELAY_PUBLIC_IP", "203.0.113.5")
	cfg = Load()
	if !cfg.RelayEnabled || cfg.RelayMediaRoute != RelayMediaRouteDirect || cfg.RelayMaxSessions != 4 ||
		cfg.RelayUDPPortMin != 40000 || cfg.RelayUDPPortMax != 40100 || cfg.RelayPublicIP != "203.0.113.5" {
		t.Fatalf("relay overrides not applied: %+v", cfg)
	}
}

func TestValidateRejectsBadRelaySettings(t *testing.T) {
	t.Setenv("VOICE_AUTH_USERNAME", "admin")
	t.Setenv("VOICE_AUTH_PASSWORD", "secret")
	t.Setenv("VOICE_TOKEN_ENCRYPTION_KEY", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	cfg := Load()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("baseline config invalid: %v", err)
	}
	for name, mutate := range map[string]func(*Config){
		"unknown route":      func(c *Config) { c.RelayMediaRoute = "proxy" },
		"negative sessions":  func(c *Config) { c.RelayMaxSessions = -1 },
		"port min only":      func(c *Config) { c.RelayUDPPortMin = 40000 },
		"port min above max": func(c *Config) { c.RelayUDPPortMin, c.RelayUDPPortMax = 40100, 40000 },
		"port out of range":  func(c *Config) { c.RelayUDPPortMin, c.RelayUDPPortMax = 40000, 70000 },
	} {
		bad := cfg
		mutate(&bad)
		if bad.Validate() == nil {
			t.Fatalf("%s must fail validation", name)
		}
	}
}

func TestRelayRebuildAndHandoffDefaultsAndValidation(t *testing.T) {
	cfg := Load()
	if cfg.RelayRebuildMinutes != 45 || cfg.RelayRebuildReadySeconds != 30 || !cfg.RelayHandoffEnabled || cfg.RelayHandoffMaxChars != 6000 {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	t.Setenv("VOICE_RELAY_REBUILD_MINUTES", "60")
	t.Setenv("VOICE_RELAY_HANDOFF", "false")
	t.Setenv("VOICE_RELAY_HANDOFF_MAX_CHARS", "9000")
	cfg = Load()
	if cfg.RelayRebuildMinutes != 60 || cfg.RelayHandoffEnabled || cfg.RelayHandoffMaxChars != 9000 {
		t.Fatalf("overrides not applied: %+v", cfg)
	}

	t.Setenv("VOICE_AUTH_USERNAME", "admin")
	t.Setenv("VOICE_AUTH_PASSWORD", "secret")
	t.Setenv("VOICE_TOKEN_ENCRYPTION_KEY", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	base := Load()
	if err := base.Validate(); err != nil {
		t.Fatalf("baseline invalid: %v", err)
	}
	for name, mutate := range map[string]func(*Config){
		"negative rebuild":      func(c *Config) { c.RelayRebuildMinutes = -1 },
		"rebuild too frequent":  func(c *Config) { c.RelayRebuildMinutes = 2 },
		"ready timeout too low": func(c *Config) { c.RelayRebuildReadySeconds = 1 },
		"handoff too small":     func(c *Config) { c.RelayHandoffEnabled, c.RelayHandoffMaxChars = true, 100 },
		"handoff too large":     func(c *Config) { c.RelayHandoffEnabled, c.RelayHandoffMaxChars = true, 100000 },
	} {
		bad := base
		mutate(&bad)
		if bad.Validate() == nil {
			t.Fatalf("%s must fail validation", name)
		}
	}
	off := base
	off.RelayRebuildMinutes, off.RelayRebuildReadySeconds = 0, 0
	if err := off.Validate(); err != nil {
		t.Fatalf("rebuild off must not require a ready timeout: %v", err)
	}
	off.RelayHandoffEnabled, off.RelayHandoffMaxChars = false, 0
	if err := off.Validate(); err != nil {
		t.Fatalf("handoff off must not constrain its size: %v", err)
	}
}
