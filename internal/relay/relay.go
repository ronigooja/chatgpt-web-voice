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
	// SetEventObserver registers a callback that receives a copy of every
	// text event the active upstream sends to the client. It must not block.
	// Events from a pending or retired leg are never delivered.
	SetEventObserver(func(data []byte))

	// BeginUpstreamSwap creates a replacement upstream leg (a new peer B) and
	// returns its SDP offer for the upstream service. The current leg keeps
	// carrying media and events until CommitUpstreamSwap. Peer A, the client,
	// and the relay slot are untouched. Only one swap may be pending.
	BeginUpstreamSwap(ctx context.Context, route Route) (upstreamOffer string, err error)
	// AcceptSwapAnswer applies the upstream answer to the pending leg.
	AcceptSwapAnswer(ctx context.Context, upstreamAnswer string) error
	// SwapReady is closed once the pending leg is connected and its oai-events
	// DataChannel is open. It is nil when no swap is pending.
	SwapReady() <-chan struct{}
	// SendPendingText sends one user text message on the pending leg's
	// oai-events channel, before it carries live traffic. Used for context handoff.
	SendPendingText(text string) error
	// CommitUpstreamSwap routes media and events to the pending leg, then closes
	// the old leg and returns how it closed. It fails, leaving the old leg
	// active, unless SwapReady has fired.
	CommitUpstreamSwap() (LegTermination, error)
	// AbortUpstreamSwap discards the pending leg. The active leg is untouched.
	AbortUpstreamSwap(reason string) error
}

// LegTermination is the close record of one upstream leg that was replaced
// while peer A stayed up. It carries no peer A result.
type LegTermination struct {
	Reason       string
	ClosedAt     time.Time
	PeerB        ComponentResult
	DataChannels ComponentResult
	Tunnels      ComponentResult
}

// Clean reports whether the replaced leg closed without error.
func (t LegTermination) Clean() bool {
	return t.PeerB.Closed && t.PeerB.Err == nil &&
		t.DataChannels.Closed && t.DataChannels.Err == nil &&
		t.Tunnels.Closed && t.Tunnels.Err == nil
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
