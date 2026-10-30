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

type pionSession struct {
	downstream *webrtc.PeerConnection
	upstream   *webrtc.PeerConnection
	bridge     *dataChannelBridge
	logger     *slog.Logger

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

	readyOnce        sync.Once
	ready            chan struct{}
	readyMu          sync.Mutex
	upstreamUp       bool
	upstreamChanOpen bool

	proxyDialer       proxy.ContextDialer
	proxyScheme       string
	forwardingLogOnce sync.Once
	localOffer        string
	tunnelsMu         sync.Mutex
	tunnels           []*tcpCandidateTunnel
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
	proxyDialer, err := buildProxyDialer(route.ProxyURL)
	if err != nil {
		return nil, "", fmt.Errorf("configure relay remote TCP proxy: %w", err)
	}
	proxied := proxyDialer != nil
	if !r.limiter.acquire() {
		return nil, "", ErrCapacity
	}
	releaseSlot := r.limiter.release
	downstream, err := r.downstreamAPI.NewPeerConnection(r.configuration)
	if err != nil {
		releaseSlot()
		return nil, "", fmt.Errorf("create peer A: %w", err)
	}
	upstreamAPI := r.upstreamAPI
	upstreamConfiguration := r.configuration
	if proxied {
		upstreamAPI = r.proxyUpstreamAPI
		upstreamConfiguration.ICEServers = nil
	}
	upstream, err := upstreamAPI.NewPeerConnection(upstreamConfiguration)
	if err != nil {
		releaseSlot()
		if errClose := downstream.Close(); errClose != nil {
			r.logger.Debug("relay: close peer A after setup error", "error", errClose)
		}
		return nil, "", fmt.Errorf("create peer B: %w", err)
	}

	id := uuid.NewString()
	session := &pionSession{
		downstream:  downstream,
		upstream:    upstream,
		done:        make(chan struct{}),
		ready:       make(chan struct{}),
		id:          id,
		releaseSlot: releaseSlot,
		proxyDialer: proxyDialer,
		proxyScheme: proxyScheme(route.ProxyURL),
		logger:      r.logger.With("media_session_id", id),
	}
	session.bridge = newDataChannelBridge(session.done, session.logger, func(err error) {
		session.fail("data_channel_failed", err)
	}, session.markUpstreamChannelOpen)
	session.installStateHandlers()
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
	downstreamSender, err := downstream.AddTrack(toClient)
	if err != nil {
		return fail("add peer A audio track", err)
	}
	go session.drainRTCP("peer-a", downstreamSender)

	toUpstream, err := webrtc.NewTrackLocalStaticRTP(opusCodec, "audio", "relay")
	if err != nil {
		return fail("create peer B audio track", err)
	}
	upstreamSender, err := upstream.AddTrack(toUpstream)
	if err != nil {
		return fail("add peer B audio track", err)
	}
	go session.drainRTCP("peer-b", upstreamSender)

	downstream.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		if !strings.EqualFold(track.Codec().MimeType, webrtc.MimeTypeOpus) {
			return
		}
		go session.relayRTP("a-to-b", track, toUpstream)
	})
	upstream.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		if !strings.EqualFold(track.Codec().MimeType, webrtc.MimeTypeOpus) {
			return
		}
		go session.relayRTP("b-to-a", track, toClient)
	})
	downstream.OnDataChannel(func(channel *webrtc.DataChannel) {
		if channel.Label() != realtimeDataChannelLabel {
			if err := channel.Close(); err != nil {
				session.logger.Debug("relay: close unsupported peer A DataChannel", "error", err)
			}
			return
		}
		session.bridge.attachDownstream(channel)
	})
	upstream.OnDataChannel(func(channel *webrtc.DataChannel) {
		if err := channel.Close(); err != nil {
			session.logger.Debug("relay: close unsupported peer B DataChannel", "error", err)
		}
	})
	upstreamChannel, err := upstream.CreateDataChannel(realtimeDataChannelLabel, nil)
	if err != nil {
		return fail("create peer B DataChannel", err)
	}
	session.bridge.attachUpstream(upstreamChannel)

	gatherComplete := webrtc.GatheringCompletePromise(upstream)
	offer, err := upstream.CreateOffer(nil)
	if err != nil {
		return fail("create upstream offer", err)
	}
	if err := upstream.SetLocalDescription(offer); err != nil {
		return fail("set upstream offer", err)
	}
	select {
	case <-gatherComplete:
	case <-ctx.Done():
		return fail("gather upstream candidates", ctx.Err())
	}
	localDescription := upstream.LocalDescription()
	if localDescription == nil || strings.TrimSpace(localDescription.SDP) == "" {
		return fail("upstream offer", errors.New("empty"))
	}
	session.localOffer = localDescription.SDP
	return session, localDescription.SDP, nil
}

// ErrCapacity is returned by NewSession when MaxSessions sessions are active.
var ErrCapacity = errors.New("relay capacity exhausted")

func (s *pionSession) ID() string { return s.id }

func (s *pionSession) Ready() <-chan struct{} { return s.ready }

func (s *pionSession) AcceptUpstreamAnswer(ctx context.Context, upstreamAnswer string) (string, error) {
	if s == nil || s.upstream == nil || s.downstream == nil {
		return "", errors.New("relay session unavailable")
	}
	answerToApply := upstreamAnswer
	if s.proxyDialer != nil {
		rewritten, tunnels, err := prepareProxiedUpstreamAnswer(upstreamAnswer, s.localOffer, s.proxyDialer, s.logger)
		if err != nil {
			return "", err
		}
		for _, tunnel := range tunnels {
			tunnel.setForwardingStartedHandler(s.logForwardingStarted)
		}
		if !s.installCandidateTunnels(tunnels) {
			errClosed := errors.New("relay session closed while configuring TCP proxy")
			if errClose := closeCandidateTunnels(tunnels); errClose != nil {
				return "", errors.Join(errClosed, fmt.Errorf("close TCP candidate tunnels: %w", errClose))
			}
			return "", errClosed
		}
		answerToApply = rewritten
	}
	if err := s.upstream.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: answerToApply}); err != nil {
		errSet := fmt.Errorf("set upstream answer on peer B: %w", err)
		if errClose := s.closeCandidateTunnels(); errClose != nil {
			return "", errors.Join(errSet, fmt.Errorf("close TCP candidate tunnels: %w", errClose))
		}
		return "", errSet
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

func (s *pionSession) installCandidateTunnels(tunnels []*tcpCandidateTunnel) bool {
	s.tunnelsMu.Lock()
	defer s.tunnelsMu.Unlock()
	select {
	case <-s.done:
		return false
	default:
	}
	s.tunnels = tunnels
	return true
}

func (s *pionSession) closeCandidateTunnels() error {
	s.tunnelsMu.Lock()
	tunnels := s.tunnels
	s.tunnels = nil
	s.tunnelsMu.Unlock()
	return closeCandidateTunnels(tunnels)
}

func (s *pionSession) logForwardingStarted() {
	s.forwardingLogOnce.Do(func() {
		s.logger.Info("relay remote media forwarding started", "connection", "via "+s.proxyScheme+" proxy")
	})
}

func (s *pionSession) markUpstreamChannelOpen() {
	s.readyMu.Lock()
	s.upstreamChanOpen = true
	s.readyMu.Unlock()
	s.checkReady()
}

func (s *pionSession) markUpstreamConnected() {
	s.readyMu.Lock()
	s.upstreamUp = true
	s.readyMu.Unlock()
	s.checkReady()
}

func (s *pionSession) checkReady() {
	s.readyMu.Lock()
	ok := s.upstreamUp && s.upstreamChanOpen
	s.readyMu.Unlock()
	if !ok {
		return
	}
	select {
	case <-s.done:
		return
	default:
	}
	s.readyOnce.Do(func() { close(s.ready) })
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

		result := Termination{SessionID: s.id, Reason: reason}
		result.DataChannels = componentResult(s.bridge.close())
		result.Tunnels = componentResult(s.closeCandidateTunnels())
		result.PeerA = componentResult(s.closePeerConnection("peer-a", s.downstream))
		result.PeerB = componentResult(s.closePeerConnection("peer-b", s.upstream))
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

func (s *pionSession) installStateHandlers() {
	handle := func(peer, reasonPrefix string) func(webrtc.PeerConnectionState) {
		return func(state webrtc.PeerConnectionState) {
			logger := s.logger.With("peer", peer, "state", state.String())
			switch state {
			case webrtc.PeerConnectionStateConnecting:
				logger.Info("relay peer connecting")
			case webrtc.PeerConnectionStateConnected:
				logger.Info("relay peer connected")
				if peer == "peer-b" {
					s.logForwardingStarted()
					s.markUpstreamConnected()
				}
			case webrtc.PeerConnectionStateDisconnected:
				logger.Warn("relay peer disconnected")
			case webrtc.PeerConnectionStateFailed:
				logger.Warn("relay peer failed")
				go s.fail(reasonPrefix+"_failed", fmt.Errorf("%s failed", peer))
			case webrtc.PeerConnectionStateClosed:
				select {
				case <-s.done:
					return
				default:
					logger.Info("relay peer closed by remote")
					go s.fail(reasonPrefix+"_closed", fmt.Errorf("%s closed", peer))
				}
			default:
				logger.Debug("relay peer state changed")
			}
		}
	}
	s.downstream.OnConnectionStateChange(handle("peer-a", "peer_a"))
	s.upstream.OnConnectionStateChange(handle("peer-b", "peer_b"))
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

func (s *pionSession) relayRTP(name string, source *webrtc.TrackRemote, destination *webrtc.TrackLocalStaticRTP) {
	for {
		packet, _, err := source.ReadRTP()
		if err != nil {
			if !isClosedMediaError(err, s.done) {
				s.logger.Debug("relay: RTP read stopped", "direction", name, "error", err)
			}
			return
		}
		normalizeRTPPacket(packet)
		if err := destination.WriteRTP(packet); err != nil {
			if !isClosedMediaError(err, s.done) {
				s.logger.Debug("relay: RTP write stopped", "direction", name, "error", err)
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
	b.bindSource(channel, b.upToDown)
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
