package relay

import (
	"context"
	"time"
)

// Route carries per-session network routing.
type Route struct {
	// ProxyURL, when it names a socks5/socks5h/http/https proxy, makes the
	// upstream peer reach the remote through TCP candidates tunnelled via that
	// proxy. Empty or "direct" means a direct connection.
	ProxyURL string
}

// Relay creates media sessions. Each session owns two peers: peer A faces the
// client and peer B faces the upstream service.
type Relay interface {
	// NewSession accepts the client's SDP offer and returns the SDP offer that
	// must be sent to the upstream service.
	NewSession(ctx context.Context, clientOffer string, route Route) (Session, string, error)
}

// Session is one bridged call.
type Session interface {
	// ID is the relay-local media session identifier.
	ID() string
	// AcceptUpstreamAnswer applies the upstream answer and returns the SDP
	// answer for the client.
	AcceptUpstreamAnswer(ctx context.Context, upstreamAnswer string) (clientAnswer string, err error)
	// SetCloseHandler registers a handler that fires at most once with the
	// close reason. If the session is already closed it fires immediately.
	SetCloseHandler(func(reason string))
	// Close is CloseWithReason("closed").
	Close() error
	// CloseWithReason is idempotent. The first call (or the first failure)
	// determines the reason and the returned error.
	CloseWithReason(reason string) error
	// Termination reports per-component close results. ok is false until the
	// session has been closed.
	Termination() (t Termination, ok bool)
	// Ready is closed once peer B is connected and its oai-events DataChannel is open.
	Ready() <-chan struct{}
}

// ComponentResult is the outcome of closing one component.
type ComponentResult struct {
	Closed bool
	Err    error
}

// Termination is a gateway-local observation of how a session closed. It does
// not prove the upstream service released anything.
type Termination struct {
	SessionID    string
	Reason       string
	ClosedAt     time.Time
	PeerA        ComponentResult
	PeerB        ComponentResult
	DataChannels ComponentResult
	Tunnels      ComponentResult
}

// Clean reports whether every component closed without error.
func (t Termination) Clean() bool {
	return t.PeerA.Closed && t.PeerB.Closed && t.DataChannels.Closed && t.Tunnels.Closed
}
