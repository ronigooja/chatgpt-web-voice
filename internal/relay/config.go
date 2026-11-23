package relay

import (
	"errors"
	"fmt"
	"net"
	"strings"
)

// DefaultMaxSessions is used when Config.MaxSessions is zero.
const DefaultMaxSessions = 16

// Config configures a Relay. The zero value is valid and uses defaults.
type Config struct {
	// OversizeCaptureDir enables private oversize message captures when nonempty.
	OversizeCaptureDir string
	// OversizeCaptureThresholdBytes captures messages above this size; nonpositive values use 256 KiB.
	OversizeCaptureThresholdBytes int
	// MaxSessions bounds concurrent relay sessions. Zero selects DefaultMaxSessions.
	MaxSessions int
	// UDPPortMin and UDPPortMax restrict the ephemeral UDP port range. Both zero means unrestricted.
	UDPPortMin uint16
	UDPPortMax uint16
	// PublicIP is advertised as the host candidate address (NAT 1:1) when set.
	PublicIP string
	// DisablePrivateRemoteIPs drops client-facing candidates from private, loopback, and link-local addresses.
	DisablePrivateRemoteIPs     bool
	EventShapeSampling          bool
	EventShapeSamplingMaxFrames int
	EventShapeRoute             string

	includeLoopback bool // tests only
}

// Validate reports configuration errors.
func (c Config) Validate() error {
	if c.MaxSessions < 0 {
		return errors.New("relay: MaxSessions must not be negative")
	}
	if (c.UDPPortMin == 0) != (c.UDPPortMax == 0) {
		return errors.New("relay: UDPPortMin and UDPPortMax must be set together")
	}
	if c.UDPPortMin > c.UDPPortMax {
		return errors.New("relay: UDPPortMin must not exceed UDPPortMax")
	}
	if ip := strings.TrimSpace(c.PublicIP); ip != "" && net.ParseIP(ip) == nil {
		return fmt.Errorf("relay: PublicIP %q is not an IP address", ip)
	}
	return nil
}

// EffectiveMaxSessions returns the session limit after applying defaults.
func (c Config) EffectiveMaxSessions() int {
	if c.MaxSessions == 0 {
		return DefaultMaxSessions
	}
	return c.MaxSessions
}
