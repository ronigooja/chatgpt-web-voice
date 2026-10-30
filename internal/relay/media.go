// Adapted from CLIProxyAPI internal/client/codex/live/media.go
// (https://github.com/router-for-me/CLIProxyAPI). logrus replaced with log/slog,
// the CLIProxyAPI config replaced with a local Config, and a close/termination
// record and a ready signal added.
//
// MIT License
//
// Copyright (c) 2025-2005.9 Luis Pater
// Copyright (c) 2025.9-present Router-For.ME
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package relay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"golang.org/x/net/proxy"
)

const (
	realtimeDataChannelLabel = "oai-events"
	mediaDataQueueSize       = 64
	mediaDataMessageMaxSize  = 256 << 10
	mediaDataBufferedMaxSize = 1 << 20
)

var opusCodec = webrtc.RTPCodecCapability{
	MimeType:    webrtc.MimeTypeOpus,
	ClockRate:   48000,
	Channels:    2,
	SDPFmtpLine: "minptime=10;useinbandfec=1",
}

// PionRelay is the Pion based Relay implementation.
type PionRelay struct {
	downstreamAPI    *webrtc.API
	upstreamAPI      *webrtc.API
	proxyUpstreamAPI *webrtc.API
	configuration    webrtc.Configuration
	limiter          *sessionLimiter
	logger           *slog.Logger
}

var _ Relay = (*PionRelay)(nil)

type sessionLimiter struct {
	mu     sync.Mutex
	limit  int
	active int
}

func (l *sessionLimiter) acquire() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.limit <= 0 || l.active >= l.limit {
		return false
	}
	l.active++
	return true
}

func (l *sessionLimiter) release() {
	l.mu.Lock()
	if l.active > 0 {
		l.active--
	}
	l.mu.Unlock()
}

// leg is one upstream attachment: peer B with its audio track, TCP tunnels and
// readiness. A session has one active leg; a swap builds a second one first.
type leg struct {
	name        string
	pc          *webrtc.PeerConnection
	toUpstream  *webrtc.TrackLocalStaticRTP
	channel     *webrtc.DataChannel
	proxyDialer proxy.ContextDialer
	proxyScheme string
	localOffer  string

	tunnelsMu sync.Mutex
	tunnels   []*tcpCandidateTunnel

	readyMu   sync.Mutex
	ready     chan struct{}
	readyOnce sync.Once
	up        bool
	chanOpen  bool

	forwardingLogOnce sync.Once

	closeOnce sync.Once
	closed    chan struct{}
	closeRes  LegTermination
}

type pionSession struct {
	downstream *webrtc.PeerConnection
	bridge     *dataChannelBridge
	logger     *slog.Logger
	toClient   *webrtc.TrackLocalStaticRTP

	// relay-level builders, kept so a replacement leg can be created later.
	upstreamAPI      *webrtc.API
	proxyUpstreamAPI *webrtc.API
	configuration    webrtc.Configuration

	id          string
	done        chan struct{}
	closeOnce   sync.Once
	closeErr    error
	failureOnce sync.Once
	releaseSlot func()

	mu          sync.Mutex
	onClose     func(string)
	closed      bool
	handlerDone bool
	termination Termination

	// legMu guards active and pending.
	legMu   sync.Mutex
	active  *leg
	pending *leg
	legSeq  int
	// toClientRewrite keeps the client-facing RTP stream continuous across legs.
	toClientRewrite rtpRewriter

	observerMu sync.RWMutex
	observer   func([]byte)
}

// SetEventObserver implements Session.
func (s *pionSession) SetEventObserver(observer func([]byte)) {
	s.observerMu.Lock()
	s.observer = observer
	s.observerMu.Unlock()
}

func (s *pionSession) observeEvent(data []byte) {
	s.observerMu.RLock()
	observer := s.observer
	s.observerMu.RUnlock()
	if observer != nil {
		observer(append([]byte(nil), data...))
	}
}

type dataChannelMessage struct {
	data     []byte
	isString bool
}

type dataChannelPipe struct {
	name        string
	done        <-chan struct{}
	queue       chan dataChannelMessage
	ready       chan struct{}
	readyOnce   sync.Once
	writable    chan struct{}
	destination *webrtc.DataChannel
	mu          sync.RWMutex
	onError     func(error)
	onOpen      func()
	logger      *slog.Logger
}

type dataChannelBridge struct {
	done         <-chan struct{}
	logger       *slog.Logger
	downToUp     *dataChannelPipe
	upToDown     *dataChannelPipe
	closeOnce    sync.Once
	closeErr     error
	downstreamMu sync.Mutex
	downstream   *webrtc.DataChannel
	upstreamMu   sync.Mutex
	upstream     *webrtc.DataChannel

	observeMu sync.RWMutex
	observe   func([]byte)
}

// New creates a Pion based relay. A nil logger discards logs.
func New(cfg Config, logger *slog.Logger) (*PionRelay, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	downstreamAPI, err := newPionAPI(cfg, cfg.DisablePrivateRemoteIPs, false)
	if err != nil {
		return nil, err
	}
	upstreamAPI, err := newPionAPI(cfg, false, false)
	if err != nil {
		return nil, err
	}
	proxyUpstreamAPI, err := newPionAPI(cfg, false, true)
	if err != nil {
		return nil, err
	}
	iceServers := make([]webrtc.ICEServer, 0, len(cfg.ICEServers))
	for _, server := range cfg.ICEServers {
		urls := make([]string, 0, len(server.URLs))
		for _, raw := range server.URLs {
			urls = append(urls, strings.TrimSpace(raw))
		}
		iceServers = append(iceServers, webrtc.ICEServer{
			URLs:       urls,
			Username:   server.Username,
			Credential: server.Credential,
		})
	}
	return &PionRelay{
		downstreamAPI:    downstreamAPI,
		upstreamAPI:      upstreamAPI,
		proxyUpstreamAPI: proxyUpstreamAPI,
		configuration:    webrtc.Configuration{ICEServers: iceServers},
		limiter:          &sessionLimiter{limit: cfg.EffectiveMaxSessions()},
		logger:           logger,
	}, nil
}

func newPionAPI(cfg Config, filterPrivateRemoteIPs, loopbackOnly bool) (*webrtc.API, error) {
	mediaEngine := &webrtc.MediaEngine{}
	if err := mediaEngine.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: opusCodec,
		PayloadType:        111,
	}, webrtc.RTPCodecTypeAudio); err != nil {
		return nil, fmt.Errorf("register Opus codec: %w", err)
	}
	interceptorRegistry := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(mediaEngine, interceptorRegistry); err != nil {
		return nil, fmt.Errorf("register WebRTC interceptors: %w", err)
	}
	settingEngine := webrtc.SettingEngine{}
	if !loopbackOnly {
		if cfg.UDPPortMin != 0 {
			if err := settingEngine.SetEphemeralUDPPortRange(cfg.UDPPortMin, cfg.UDPPortMax); err != nil {
				return nil, fmt.Errorf("configure WebRTC UDP port range: %w", err)
			}
		}
		if publicIP := strings.TrimSpace(cfg.PublicIP); publicIP != "" {
			settingEngine.SetNAT1To1IPs([]string{publicIP}, webrtc.ICECandidateTypeHost)
		}
	}
	if cfg.includeLoopback {
		settingEngine.SetIncludeLoopbackCandidate(true)
		settingEngine.SetIPFilter(func(ip net.IP) bool { return ip != nil && ip.IsLoopback() })
	}
	if filterPrivateRemoteIPs {
		settingEngine.SetRemoteIPFilter(isPublicRemoteIP)
	}
	if loopbackOnly {
		settingEngine.SetNetworkTypes([]webrtc.NetworkType{
			webrtc.NetworkTypeUDP4,
			webrtc.NetworkTypeUDP6,
			webrtc.NetworkTypeTCP4,
			webrtc.NetworkTypeTCP6,
		})
		settingEngine.SetIncludeLoopbackCandidate(true)
		settingEngine.SetIPFilter(func(ip net.IP) bool {
			return ip != nil && ip.IsLoopback()
		})
	}
	return webrtc.NewAPI(
		webrtc.WithMediaEngine(mediaEngine),
		webrtc.WithInterceptorRegistry(interceptorRegistry),
		webrtc.WithSettingEngine(settingEngine),
	), nil
}

func isPublicRemoteIP(ip net.IP) bool {
	return ip != nil && !ip.IsUnspecified() && !ip.IsLoopback() && !ip.IsPrivate() &&
		!ip.IsLinkLocalUnicast() && !ip.IsLinkLocalMulticast() && !ip.IsMulticast()
}

// NewSession implements Relay.
func (r *PionRelay) NewSession(ctx context.Context, clientOffer string, route Route) (Session, string, error) {
	if r == nil || r.downstreamAPI == nil || r.upstreamAPI == nil || r.proxyUpstreamAPI == nil || r.limiter == nil {
		return nil, "", errors.New("relay unavailable")
	}
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if _, err := buildProxyDialer(route.ProxyURL); err != nil {
		return nil, "", fmt.Errorf("configure relay remote TCP proxy: %w", err)
	}
	if !r.limiter.acquire() {
		return nil, "", ErrCapacity
	}
	releaseSlot := r.limiter.release
	downstream, err := r.downstreamAPI.NewPeerConnection(r.configuration)
	if err != nil {
		releaseSlot()
		return nil, "", fmt.Errorf("create peer A: %w", err)
	}

	id := uuid.NewString()
	session := &pionSession{
		downstream:       downstream,
		done:             make(chan struct{}),
		id:               id,
		releaseSlot:      releaseSlot,
		upstreamAPI:      r.upstreamAPI,
		proxyUpstreamAPI: r.proxyUpstreamAPI,
		configuration:    r.configuration,
		logger:           r.logger.With("media_session_id", id),
	}
	session.bridge = newDataChannelBridge(session.done, session.logger, func(err error) {
		session.fail("data_channel_failed", err)
	}, session.markActiveChannelOpen)
	session.bridge.setObserver(session.observeEvent)
	session.installPeerAHandler()
	session.logger.Info("relay session created")

	fail := func(format string, err error) (Session, string, error) {
		_ = session.Close()
		return nil, "", fmt.Errorf(format+": %w", err)
	}

	if err := downstream.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: clientOffer}); err != nil {
		return fail("set client offer on peer A", err)
	}
	toClient, err := webrtc.NewTrackLocalStaticRTP(opusCodec, "audio", "relay")
	if err != nil {
		return fail("create peer A audio track", err)
	}
	session.toClient = toClient
	downstreamSender, err := downstream.AddTrack(toClient)
	if err != nil {
		return fail("add peer A audio track", err)
	}
	go session.drainRTCP("peer-a", downstreamSender)
	downstream.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		if !strings.EqualFold(track.Codec().MimeType, webrtc.MimeTypeOpus) {
			return
		}
		go session.relayClientRTP(track)
	})
	// The web client and /realtime/wm both use a pre-negotiated oai-events
	// channel (negotiated, id 0), so neither peer ever sees OnDataChannel for it.
	clientChannel, err := downstream.CreateDataChannel(realtimeDataChannelLabel, eventsChannelOptions())
	if err != nil {
		return fail("create peer A DataChannel", err)
	}
	session.bridge.attachDownstream(clientChannel)
	downstream.OnDataChannel(func(channel *webrtc.DataChannel) {
		if err := channel.Close(); err != nil {
			session.logger.Debug("relay: close unsupported peer A DataChannel", "error", err)
		}
	})

	first, offer, err := session.buildLeg(ctx, route)
	if err != nil {
		return fail("build upstream leg", err)
	}
	session.legMu.Lock()
	session.active = first
	session.legMu.Unlock()
	session.bridge.attachUpstream(first.channel)
	return session, offer, nil
}

func eventsChannelOptions() *webrtc.DataChannelInit {
	negotiated := true
	channelID := uint16(0)
	return &webrtc.DataChannelInit{Negotiated: &negotiated, ID: &channelID}
}

// buildLeg creates peer B, its audio track and DataChannel, and the offer for
// the upstream service. It does not attach the leg to the session.
func (s *pionSession) buildLeg(ctx context.Context, route Route) (*leg, string, error) {
	dialer, err := buildProxyDialer(route.ProxyURL)
	if err != nil {
		return nil, "", fmt.Errorf("configure relay remote TCP proxy: %w", err)
	}
	api, configuration := s.upstreamAPI, s.configuration
	if dialer != nil {
		api = s.proxyUpstreamAPI
		configuration.ICEServers = nil
	}
	pc, err := api.NewPeerConnection(configuration)
	if err != nil {
		return nil, "", fmt.Errorf("create peer B: %w", err)
	}
	s.legMu.Lock()
	s.legSeq++
	name := fmt.Sprintf("peer-b%d", s.legSeq)
	seq := s.legSeq
	s.legMu.Unlock()
	l := &leg{
		name:        name,
		pc:          pc,
		proxyDialer: dialer,
		proxyScheme: proxyScheme(route.ProxyURL),
		ready:       make(chan struct{}),
		closed:      make(chan struct{}),
	}
	failLeg := func(msg string, err error) (*leg, string, error) {
		_ = s.closeLegPeer(l, "setup_failed")
		return nil, "", fmt.Errorf("%s: %w", msg, err)
	}
	toUpstream, err := webrtc.NewTrackLocalStaticRTP(opusCodec, "audio", "relay")
	if err != nil {
		return failLeg("create peer B audio track", err)
	}
	l.toUpstream = toUpstream
	sender, err := pc.AddTrack(toUpstream)
	if err != nil {
		return failLeg("add peer B audio track", err)
	}
	go s.drainRTCP(name, sender)
	pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		if !strings.EqualFold(track.Codec().MimeType, webrtc.MimeTypeOpus) {
			return
		}
		go s.relayUpstreamRTP(l, seq, track)
	})
	pc.OnDataChannel(func(channel *webrtc.DataChannel) {
		if err := channel.Close(); err != nil {
			s.logger.Debug("relay: close unsupported peer B DataChannel", "leg", name, "error", err)
		}
	})
	channel, err := pc.CreateDataChannel(realtimeDataChannelLabel, eventsChannelOptions())
	if err != nil {
		return failLeg("create peer B DataChannel", err)
	}
	l.channel = channel
	s.installLegHandlers(l)

	gatherComplete := webrtc.GatheringCompletePromise(pc)
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return failLeg("create upstream offer", err)
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		return failLeg("set upstream offer", err)
	}
	select {
	case <-gatherComplete:
	case <-ctx.Done():
		return failLeg("gather upstream candidates", ctx.Err())
	}
	local := pc.LocalDescription()
	if local == nil || strings.TrimSpace(local.SDP) == "" {
		return failLeg("upstream offer", errors.New("empty"))
	}
	l.localOffer = local.SDP
	return l, local.SDP, nil
}

// ErrCapacity is returned by NewSession when MaxSessions sessions are active.
var ErrCapacity = errors.New("relay capacity exhausted")

func (s *pionSession) ID() string { return s.id }

func (s *pionSession) activeLeg() *leg {
	s.legMu.Lock()
	defer s.legMu.Unlock()
	return s.active
}

// Ready is closed once the first leg is connected and its oai-events channel is open.
func (s *pionSession) Ready() <-chan struct{} {
	if l := s.activeLeg(); l != nil {
		return l.ready
	}
	return nil
}

func (s *pionSession) AcceptUpstreamAnswer(ctx context.Context, upstreamAnswer string) (string, error) {
	if s == nil || s.downstream == nil {
		return "", errors.New("relay session unavailable")
	}
	l := s.activeLeg()
	if l == nil {
		return "", errors.New("relay session has no upstream leg")
	}
	if err := s.applyLegAnswer(l, upstreamAnswer); err != nil {
		return "", err
	}
	gatherComplete := webrtc.GatheringCompletePromise(s.downstream)
	answer, err := s.downstream.CreateAnswer(nil)
	if err != nil {
		return "", fmt.Errorf("create client answer: %w", err)
	}
	if err := s.downstream.SetLocalDescription(answer); err != nil {
		return "", fmt.Errorf("set client answer on peer A: %w", err)
	}
	select {
	case <-gatherComplete:
	case <-ctx.Done():
		return "", fmt.Errorf("gather peer A candidates: %w", ctx.Err())
	}
	localDescription := s.downstream.LocalDescription()
	if localDescription == nil || strings.TrimSpace(localDescription.SDP) == "" {
		return "", errors.New("client answer is empty")
	}
	return localDescription.SDP, nil
}

// applyLegAnswer rewrites the answer for a proxied leg and sets it on peer B.
func (s *pionSession) applyLegAnswer(l *leg, upstreamAnswer string) error {
	answerToApply := upstreamAnswer
	if l.proxyDialer != nil {
		rewritten, tunnels, err := prepareProxiedUpstreamAnswer(upstreamAnswer, l.localOffer, l.proxyDialer, s.logger)
		if err != nil {
			return err
		}
		for _, tunnel := range tunnels {
			tunnel.setForwardingStartedHandler(func() { s.logForwardingStarted(l) })
		}
		if !s.installCandidateTunnels(l, tunnels) {
			errClosed := errors.New("relay leg closed while configuring TCP proxy")
			if errClose := closeCandidateTunnels(tunnels); errClose != nil {
				return errors.Join(errClosed, fmt.Errorf("close TCP candidate tunnels: %w", errClose))
			}
			return errClosed
		}
		answerToApply = rewritten
	}
	if err := l.pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: answerToApply}); err != nil {
		errSet := fmt.Errorf("set upstream answer on %s: %w", l.name, err)
		if errClose := closeLegTunnels(l); errClose != nil {
			return errors.Join(errSet, fmt.Errorf("close TCP candidate tunnels: %w", errClose))
		}
		return errSet
	}
	return nil
}

func (s *pionSession) installCandidateTunnels(l *leg, tunnels []*tcpCandidateTunnel) bool {
	l.tunnelsMu.Lock()
	defer l.tunnelsMu.Unlock()
	select {
	case <-s.done:
		return false
	case <-l.closed:
		return false
	default:
	}
	l.tunnels = tunnels
	return true
}

func closeLegTunnels(l *leg) error {
	l.tunnelsMu.Lock()
	tunnels := l.tunnels
	l.tunnels = nil
	l.tunnelsMu.Unlock()
	return closeCandidateTunnels(tunnels)
}

func (s *pionSession) logForwardingStarted(l *leg) {
	l.forwardingLogOnce.Do(func() {
		s.logger.Info("relay remote media forwarding started", "leg", l.name, "connection", "via "+l.proxyScheme+" proxy")
	})
}

// markActiveChannelOpen is called when the bridge's upstream destination opens.
func (s *pionSession) markActiveChannelOpen() {
	if l := s.activeLeg(); l != nil {
		s.markChannelOpen(l)
	}
}

func (s *pionSession) markChannelOpen(l *leg) {
	l.readyMu.Lock()
	l.chanOpen = true
	l.readyMu.Unlock()
	s.checkLegReady(l)
}

func (s *pionSession) markLegConnected(l *leg) {
	l.readyMu.Lock()
	l.up = true
	l.readyMu.Unlock()
	s.checkLegReady(l)
}

func (s *pionSession) checkLegReady(l *leg) {
	l.readyMu.Lock()
	ok := l.up && l.chanOpen
	l.readyMu.Unlock()
	if !ok {
		return
	}
	select {
	case <-s.done:
		return
	case <-l.closed:
		return
	default:
	}
	l.readyOnce.Do(func() { close(l.ready) })
}

// SetCloseHandler registers the close handler. It fires at most once; if the
// session has already closed it fires immediately.
func (s *pionSession) SetCloseHandler(handler func(string)) {
	s.mu.Lock()
	s.onClose = handler
	call := handler != nil && s.closed && !s.handlerDone
	reason := s.termination.Reason
	if call {
		s.handlerDone = true
	}
	s.mu.Unlock()
	if call {
		handler(reason)
	}
}

func (s *pionSession) Close() error { return s.CloseWithReason("closed") }

func (s *pionSession) CloseWithReason(reason string) error {
	first := false
	s.closeOnce.Do(func() {
		first = true
		s.logger.Info("relay session closing", "reason", reason)
		close(s.done)

		s.legMu.Lock()
		active, pending := s.active, s.pending
		s.pending = nil
		s.legMu.Unlock()

		result := Termination{SessionID: s.id, Reason: reason}
		result.DataChannels = componentResult(s.bridge.close())
		// A pending swap leg is part of this session: any error closing it
		// must keep the session's termination unclean.
		var tunnelErrs, peerBErrs []error
		for _, l := range []*leg{pending, active} {
			if l == nil {
				continue
			}
			res := s.closeLeg(l, reason)
			if res.Tunnels.Err != nil {
				tunnelErrs = append(tunnelErrs, res.Tunnels.Err)
			}
			if res.PeerB.Err != nil {
				peerBErrs = append(peerBErrs, res.PeerB.Err)
			}
			if res.DataChannels.Err != nil {
				result.DataChannels = ComponentResult{Err: errors.Join(result.DataChannels.Err, res.DataChannels.Err)}
			}
		}
		result.Tunnels = componentResult(errors.Join(tunnelErrs...))
		result.PeerA = componentResult(s.closePeerConnection("peer-a", s.downstream))
		result.PeerB = componentResult(errors.Join(peerBErrs...))
		result.ClosedAt = time.Now()
		if s.releaseSlot != nil {
			s.releaseSlot()
		}
		var errs []error
		for _, c := range []struct {
			name string
			r    ComponentResult
		}{{"peer A", result.PeerA}, {"peer B", result.PeerB}, {"data channels", result.DataChannels}, {"tcp tunnels", result.Tunnels}} {
			if c.r.Err != nil {
				errs = append(errs, fmt.Errorf("close %s: %w", c.name, c.r.Err))
			}
		}
		s.closeErr = errors.Join(errs...)
		if s.closeErr != nil {
			s.logger.Warn("relay session closed with errors", "reason", reason, "error", s.closeErr)
		} else {
			s.logger.Info("relay session closed", "reason", reason)
		}
		s.mu.Lock()
		s.termination = result
		s.closed = true
		s.mu.Unlock()
	})
	if first {
		s.mu.Lock()
		handler := s.onClose
		call := handler != nil && !s.handlerDone
		if call {
			s.handlerDone = true
		}
		s.mu.Unlock()
		if call {
			handler(reason)
		}
	}
	return s.closeErr
}

// closeLeg closes one upstream leg exactly once and records how it closed. Its
// DataChannel is closed here only when it is not the bridge's active channel;
// the bridge closes its own channels.
func (s *pionSession) closeLeg(l *leg, reason string) LegTermination {
	l.closeOnce.Do(func() {
		close(l.closed)
		res := LegTermination{Reason: reason}
		res.Tunnels = componentResult(closeLegTunnels(l))
		var chErr error
		if l.channel != nil && !s.bridge.owns(l.channel) {
			chErr = l.channel.Close()
		}
		res.DataChannels = componentResult(chErr)
		res.PeerB = componentResult(s.closePeerConnection(l.name, l.pc))
		res.ClosedAt = time.Now()
		l.closeRes = res
	})
	<-l.closed
	return l.closeRes
}

// closeLegPeer closes a leg that failed during setup.
func (s *pionSession) closeLegPeer(l *leg, reason string) error {
	return s.closeLeg(l, reason).PeerB.Err
}

func componentResult(err error) ComponentResult {
	return ComponentResult{Closed: err == nil, Err: err}
}

// Termination returns the close record once the session is closed.
func (s *pionSession) Termination() (Termination, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.termination, s.closed
}

func (s *pionSession) closePeerConnection(peer string, connection *webrtc.PeerConnection) error {
	if connection == nil {
		return nil
	}
	before := connection.ConnectionState().String()
	err := connection.Close()
	after := connection.ConnectionState().String()
	if err != nil {
		s.logger.Warn("relay peer close failed", "peer", peer, "state_before", before, "state_after", after, "error", err)
		return err
	}
	s.logger.Info("relay peer closed", "peer", peer, "state_before", before, "state_after", after)
	return nil
}

func (s *pionSession) installPeerAHandler() {
	s.downstream.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		logger := s.logger.With("peer", "peer-a", "state", state.String())
		switch state {
		case webrtc.PeerConnectionStateConnecting:
			logger.Info("relay peer connecting")
		case webrtc.PeerConnectionStateConnected:
			logger.Info("relay peer connected")
		case webrtc.PeerConnectionStateDisconnected:
			logger.Warn("relay peer disconnected")
		case webrtc.PeerConnectionStateFailed:
			logger.Warn("relay peer failed")
			go s.fail("peer_a_failed", errors.New("peer-a failed"))
		case webrtc.PeerConnectionStateClosed:
			select {
			case <-s.done:
				return
			default:
				logger.Info("relay peer closed by remote")
				go s.fail("peer_a_closed", errors.New("peer-a closed"))
			}
		default:
			logger.Debug("relay peer state changed")
		}
	})
}

// installLegHandlers wires peer B state changes for one leg. A failure of the
// active leg fails the session; a failure of a pending leg only aborts the swap.
func (s *pionSession) installLegHandlers(l *leg) {
	l.pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		logger := s.logger.With("peer", l.name, "state", state.String())
		switch state {
		case webrtc.PeerConnectionStateConnecting:
			logger.Info("relay peer connecting")
		case webrtc.PeerConnectionStateConnected:
			logger.Info("relay peer connected")
			s.logForwardingStarted(l)
			s.markLegConnected(l)
		case webrtc.PeerConnectionStateDisconnected:
			logger.Warn("relay peer disconnected")
		case webrtc.PeerConnectionStateFailed:
			logger.Warn("relay peer failed")
			go s.legEnded(l, "peer_b_failed")
		case webrtc.PeerConnectionStateClosed:
			select {
			case <-s.done:
				return
			case <-l.closed:
				return
			default:
				logger.Info("relay peer closed by remote")
				go s.legEnded(l, "peer_b_closed")
			}
		default:
			logger.Debug("relay peer state changed")
		}
	})
}

// legEnded handles an unexpected end of a leg.
func (s *pionSession) legEnded(l *leg, reason string) {
	s.legMu.Lock()
	isActive := s.active == l
	isPending := s.pending == l
	s.legMu.Unlock()
	switch {
	case isActive:
		s.fail(reason, fmt.Errorf("%s ended", l.name))
	case isPending:
		_ = s.AbortUpstreamSwap("pending_" + reason)
	}
}

func (s *pionSession) fail(reason string, err error) {
	s.failureOnce.Do(func() {
		if err != nil {
			s.logger.Warn("relay session failed", "reason", reason, "error", err)
		}
		if errClose := s.CloseWithReason(reason); errClose != nil {
			s.logger.Debug("relay: close failed session", "error", errClose)
		}
	})
}

// relayClientRTP forwards the client's audio to whichever leg is active now.
func (s *pionSession) relayClientRTP(source *webrtc.TrackRemote) {
	for {
		packet, _, err := source.ReadRTP()
		if err != nil {
			if !isClosedMediaError(err, s.done) {
				s.logger.Debug("relay: RTP read stopped", "direction", "a-to-b", "error", err)
			}
			return
		}
		normalizeRTPPacket(packet)
		l := s.activeLeg()
		if l == nil || l.toUpstream == nil {
			continue
		}
		if err := l.toUpstream.WriteRTP(packet); err != nil && !isClosedMediaError(err, s.done) {
			s.logger.Debug("relay: RTP write failed", "direction", "a-to-b", "leg", l.name, "error", err)
		}
	}
}

// relayUpstreamRTP forwards one leg's audio to the client, but only while that
// leg is the active one. Audio from a pending or retired leg is dropped, and the
// client-facing stream is re-based so a leg change does not break it.
func (s *pionSession) relayUpstreamRTP(l *leg, source int, track *webrtc.TrackRemote) {
	for {
		packet, _, err := track.ReadRTP()
		if err != nil {
			if !isClosedMediaError(err, s.done) {
				s.logger.Debug("relay: RTP read stopped", "direction", "b-to-a", "leg", l.name, "error", err)
			}
			return
		}
		if s.activeLeg() != l {
			continue
		}
		normalizeRTPPacket(packet)
		s.toClientRewrite.rewrite(source, packet)
		if err := s.toClient.WriteRTP(packet); err != nil {
			if !isClosedMediaError(err, s.done) {
				s.logger.Debug("relay: RTP write stopped", "direction", "b-to-a", "leg", l.name, "error", err)
			}
			return
		}
	}
}

func normalizeRTPPacket(packet *rtp.Packet) {
	if packet == nil {
		return
	}
	packet.Extension = false
	packet.ExtensionProfile = 0
	packet.Extensions = nil
}

func (s *pionSession) drainRTCP(name string, sender *webrtc.RTPSender) {
	for {
		if _, _, err := sender.ReadRTCP(); err != nil {
			if !isClosedMediaError(err, s.done) {
				s.logger.Debug("relay: RTCP reader stopped", "peer", name, "error", err)
			}
			return
		}
	}
}

func isClosedMediaError(err error, done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	default:
	}
	return errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed)
}

func newDataChannelBridge(done <-chan struct{}, logger *slog.Logger, onError func(error), onUpstreamOpen func()) *dataChannelBridge {
	bridge := &dataChannelBridge{done: done, logger: logger}
	bridge.downToUp = newDataChannelPipe("a-to-b", done, logger, onError, onUpstreamOpen)
	bridge.upToDown = newDataChannelPipe("b-to-a", done, logger, onError, nil)
	return bridge
}

func newDataChannelPipe(name string, done <-chan struct{}, logger *slog.Logger, onError func(error), onOpen func()) *dataChannelPipe {
	pipe := &dataChannelPipe{
		name:     name,
		done:     done,
		queue:    make(chan dataChannelMessage, mediaDataQueueSize),
		ready:    make(chan struct{}),
		writable: make(chan struct{}, 1),
		onError:  onError,
		onOpen:   onOpen,
		logger:   logger,
	}
	go pipe.run()
	return pipe
}

func (b *dataChannelBridge) attachDownstream(channel *webrtc.DataChannel) {
	b.downstreamMu.Lock()
	if b.downstream != nil {
		b.downstreamMu.Unlock()
		if err := channel.Close(); err != nil {
			b.logger.Debug("relay: close duplicate peer A DataChannel", "error", err)
		}
		return
	}
	b.downstream = channel
	b.downstreamMu.Unlock()
	b.upToDown.setDestination(channel)
	b.bindSource(channel, b.downToUp)
}

func (b *dataChannelBridge) attachUpstream(channel *webrtc.DataChannel) {
	b.upstreamMu.Lock()
	if b.upstream != nil {
		b.upstreamMu.Unlock()
		if err := channel.Close(); err != nil {
			b.logger.Debug("relay: close duplicate peer B DataChannel", "error", err)
		}
		return
	}
	b.upstream = channel
	b.upstreamMu.Unlock()
	b.downToUp.setDestination(channel)
	b.bindUpstreamSource(channel)
}

// setObserver installs the upstream-event observer used by bindUpstreamSource.
func (b *dataChannelBridge) setObserver(observe func([]byte)) {
	b.observeMu.Lock()
	b.observe = observe
	b.observeMu.Unlock()
}

// owns reports whether channel is the bridge's current upstream channel.
func (b *dataChannelBridge) owns(channel *webrtc.DataChannel) bool {
	b.upstreamMu.Lock()
	defer b.upstreamMu.Unlock()
	return b.upstream == channel
}

// current returns the bridge's current upstream channel.
func (b *dataChannelBridge) current() *webrtc.DataChannel {
	b.upstreamMu.Lock()
	defer b.upstreamMu.Unlock()
	return b.upstream
}

// bindUpstreamSource forwards events from channel to the client, but only while
// channel is still the bridge's current upstream channel. After a swap the
// retired channel is ignored, and its closing is not an error.
func (b *dataChannelBridge) bindUpstreamSource(channel *webrtc.DataChannel) {
	destination := b.upToDown
	channel.OnMessage(func(message webrtc.DataChannelMessage) {
		if !b.owns(channel) {
			return
		}
		if len(message.Data) > mediaDataMessageMaxSize {
			destination.reportError(fmt.Errorf("%s DataChannel message exceeds %d bytes", destination.name, mediaDataMessageMaxSize))
			return
		}
		payload := append([]byte(nil), message.Data...)
		if message.IsString {
			b.observeMu.RLock()
			observe := b.observe
			b.observeMu.RUnlock()
			if observe != nil {
				observe(payload)
			}
		}
		select {
		case destination.queue <- dataChannelMessage{data: payload, isString: message.IsString}:
		case <-b.done:
		}
	})
	channel.OnError(func(err error) {
		if !b.owns(channel) {
			return
		}
		destination.reportError(fmt.Errorf("%s DataChannel error: %w", destination.name, err))
	})
	channel.OnClose(func() {
		select {
		case <-b.done:
			return
		default:
		}
		if !b.owns(channel) {
			return
		}
		destination.reportError(fmt.Errorf("%s DataChannel closed", destination.name))
	})
}

// swapUpstream makes next the bridge's upstream channel and returns the retired
// one. Events already queued for the old channel are dropped on the floor by the
// pipe's destination switch; new client events go to next.
func (b *dataChannelBridge) swapUpstream(next *webrtc.DataChannel) *webrtc.DataChannel {
	b.upstreamMu.Lock()
	old := b.upstream
	b.upstream = next
	b.upstreamMu.Unlock()
	b.downToUp.replaceDestination(next)
	b.bindUpstreamSource(next)
	return old
}

func (b *dataChannelBridge) bindSource(channel *webrtc.DataChannel, destination *dataChannelPipe) {
	channel.OnMessage(func(message webrtc.DataChannelMessage) {
		if len(message.Data) > mediaDataMessageMaxSize {
			destination.reportError(fmt.Errorf("%s DataChannel message exceeds %d bytes", destination.name, mediaDataMessageMaxSize))
			return
		}
		payload := append([]byte(nil), message.Data...)
		select {
		case destination.queue <- dataChannelMessage{data: payload, isString: message.IsString}:
		case <-b.done:
		}
	})
	channel.OnError(func(err error) {
		destination.reportError(fmt.Errorf("%s DataChannel error: %w", destination.name, err))
	})
	channel.OnClose(func() {
		select {
		case <-b.done:
			return
		default:
			destination.reportError(fmt.Errorf("%s DataChannel closed", destination.name))
		}
	})
}

// close closes both bridged channels and returns any close errors. It is idempotent.
func (b *dataChannelBridge) close() error {
	if b == nil {
		return nil
	}
	b.closeOnce.Do(func() {
		var errs []error
		b.downstreamMu.Lock()
		downstream := b.downstream
		b.downstreamMu.Unlock()
		if downstream != nil {
			if err := downstream.Close(); err != nil {
				errs = append(errs, fmt.Errorf("peer A DataChannel: %w", err))
			}
		}
		b.upstreamMu.Lock()
		upstream := b.upstream
		b.upstreamMu.Unlock()
		if upstream != nil {
			if err := upstream.Close(); err != nil {
				errs = append(errs, fmt.Errorf("peer B DataChannel: %w", err))
			}
		}
		b.closeErr = errors.Join(errs...)
	})
	return b.closeErr
}

// replaceDestination points the pipe at a new, already-open channel.
func (p *dataChannelPipe) replaceDestination(channel *webrtc.DataChannel) {
	p.mu.Lock()
	p.destination = channel
	p.mu.Unlock()
	channel.SetBufferedAmountLowThreshold(mediaDataBufferedMaxSize / 2)
	channel.OnBufferedAmountLow(func() {
		select {
		case p.writable <- struct{}{}:
		default:
		}
	})
}

func (p *dataChannelPipe) setDestination(channel *webrtc.DataChannel) {
	p.mu.Lock()
	p.destination = channel
	p.mu.Unlock()
	markReady := func() {
		p.readyOnce.Do(func() { close(p.ready) })
		if p.onOpen != nil {
			p.onOpen()
		}
	}
	channel.SetBufferedAmountLowThreshold(mediaDataBufferedMaxSize / 2)
	channel.OnBufferedAmountLow(func() {
		select {
		case p.writable <- struct{}{}:
		default:
		}
	})
	channel.OnOpen(markReady)
	if channel.ReadyState() == webrtc.DataChannelStateOpen {
		markReady()
	}
}

func (p *dataChannelPipe) run() {
	select {
	case <-p.ready:
	case <-p.done:
		return
	}
	for {
		select {
		case message := <-p.queue:
			p.mu.RLock()
			destination := p.destination
			p.mu.RUnlock()
			if destination == nil {
				p.reportError(fmt.Errorf("%s DataChannel destination unavailable", p.name))
				return
			}
			if !p.waitWritable(destination, len(message.data)) {
				return
			}
			var err error
			if message.isString {
				err = destination.SendText(string(message.data))
			} else {
				err = destination.Send(message.data)
			}
			if err != nil {
				p.reportError(fmt.Errorf("send %s DataChannel message: %w", p.name, err))
				return
			}
		case <-p.done:
			return
		}
	}
}

func (p *dataChannelPipe) waitWritable(destination *webrtc.DataChannel, messageSize int) bool {
	for destination.BufferedAmount()+uint64(messageSize) > mediaDataBufferedMaxSize {
		select {
		case <-p.writable:
		case <-p.done:
			return false
		}
	}
	return true
}

func (p *dataChannelPipe) reportError(err error) {
	if p.onError != nil {
		p.onError(err)
	}
}
