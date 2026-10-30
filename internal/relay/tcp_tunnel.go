// Adapted from CLIProxyAPI internal/client/codex/live/tcp_proxy.go
// (https://github.com/router-for-me/CLIProxyAPI). logrus replaced with log/slog.
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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"sync"

	"github.com/pion/ice/v4"
	"github.com/pion/sdp/v3"
	"github.com/pion/stun/v3"
	"golang.org/x/net/proxy"
)

const (
	maxUpstreamICECandidates   = 64
	maxProxiedTCPCandidates    = 16
	maxUnauthenticatedTCPConns = 4
	maxInitialSTUNFrameSize    = 4096
	stunMessageHeaderSize      = 20
)

var nonRoutableProxyTargetPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("169.254.0.0/16"),
	netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("224.0.0.0/4"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("::/96"),
	netip.MustParsePrefix("::ffff:0:0:0/96"),
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("64:ff9b:1::/48"),
	netip.MustParsePrefix("100::/64"),
	netip.MustParsePrefix("2001::/23"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("3fff::/20"),
	netip.MustParsePrefix("5f00::/16"),
	netip.MustParsePrefix("fc00::/7"),
	netip.MustParsePrefix("fe80::/10"),
	netip.MustParsePrefix("fec0::/10"),
	netip.MustParsePrefix("ff00::/8"),
}

type iceCredentials struct {
	ufrag    string
	password string
}

type tcpCandidateTunnel struct {
	listener       net.Listener
	target         netip.AddrPort
	dialer         proxy.ContextDialer
	expectedUser   string
	remotePassword string
	logger         *slog.Logger

	mu                  sync.Mutex
	closed              bool
	claimed             bool
	connections         map[net.Conn]struct{}
	validationSlots     chan struct{}
	onForwardingStarted func()
	ctx                 context.Context
	cancel              context.CancelFunc
}

type tcpCandidatePlan struct {
	mediaIndex     int
	attributeIndex int
	fields         []string
	target         netip.AddrPort
}

func prepareProxiedUpstreamAnswer(answer, localOffer string, dialer proxy.ContextDialer, logger *slog.Logger) (string, []*tcpCandidateTunnel, error) {
	if dialer == nil {
		return "", nil, errors.New("relay TCP proxy dialer is unavailable")
	}
	var remoteDescription sdp.SessionDescription
	if err := remoteDescription.UnmarshalString(answer); err != nil {
		return "", nil, fmt.Errorf("parse upstream WebRTC answer for TCP proxy: %w", err)
	}
	var localDescription sdp.SessionDescription
	if err := localDescription.UnmarshalString(localOffer); err != nil {
		return "", nil, fmt.Errorf("parse upstream WebRTC offer for TCP proxy: %w", err)
	}
	remoteCredentials, err := bundledICECredentials(&remoteDescription)
	if err != nil {
		return "", nil, fmt.Errorf("read upstream WebRTC answer ICE credentials: %w", err)
	}
	localCredentials, err := bundledICECredentials(&localDescription)
	if err != nil {
		return "", nil, fmt.Errorf("read upstream WebRTC offer ICE credentials: %w", err)
	}

	plans := make([]tcpCandidatePlan, 0, 4)
	candidateCount := 0
	for mediaIndex, media := range remoteDescription.MediaDescriptions {
		if media == nil {
			continue
		}
		filtered := make([]sdp.Attribute, 0, len(media.Attributes))
		for attributeIndex := range media.Attributes {
			attribute := media.Attributes[attributeIndex]
			if !attribute.IsICECandidate() {
				filtered = append(filtered, attribute)
				continue
			}
			candidateCount++
			if candidateCount > maxUpstreamICECandidates {
				return "", nil, fmt.Errorf("upstream WebRTC answer exceeds the %d candidate limit", maxUpstreamICECandidates)
			}
			plan, keep, err := proxiedTCPCandidatePlan(attribute.Value)
			if err != nil {
				return "", nil, err
			}
			if !keep {
				continue
			}
			if len(plans) >= maxProxiedTCPCandidates {
				return "", nil, fmt.Errorf("upstream WebRTC answer exceeds the %d TCP candidate proxy limit", maxProxiedTCPCandidates)
			}
			plan.mediaIndex = mediaIndex
			plan.attributeIndex = len(filtered)
			filtered = append(filtered, attribute)
			plans = append(plans, plan)
		}
		media.Attributes = filtered
	}
	if len(plans) == 0 {
		return "", nil, errors.New("upstream WebRTC answer has no supported public TCP passive candidate on port 443")
	}

	expectedUser := remoteCredentials.ufrag + ":" + localCredentials.ufrag
	tunnels := make([]*tcpCandidateTunnel, 0, len(plans))
	closeTunnels := func() {
		for _, tunnel := range tunnels {
			if err := tunnel.Close(); err != nil {
				logger.Debug("relay tcp tunnel: close after setup error", "error", err)
			}
		}
	}
	for _, plan := range plans {
		tunnel, err := newTCPCandidateTunnel(plan.target, dialer, expectedUser, remoteCredentials.password, logger)
		if err != nil {
			closeTunnels()
			return "", nil, err
		}
		tunnels = append(tunnels, tunnel)
		listenerAddress, ok := tunnel.listener.Addr().(*net.TCPAddr)
		if !ok || listenerAddress.IP == nil {
			closeTunnels()
			return "", nil, errors.New("relay TCP proxy listener returned an invalid address")
		}
		fields := append([]string(nil), plan.fields...)
		fields[4] = listenerAddress.IP.String()
		fields[5] = strconv.Itoa(listenerAddress.Port)
		remoteDescription.MediaDescriptions[plan.mediaIndex].Attributes[plan.attributeIndex].Value = strings.Join(fields, " ")
	}

	rewritten, err := remoteDescription.Marshal()
	if err != nil {
		closeTunnels()
		return "", nil, fmt.Errorf("marshal proxied upstream WebRTC answer: %w", err)
	}
	return string(rewritten), tunnels, nil
}

func proxiedTCPCandidatePlan(rawCandidate string) (tcpCandidatePlan, bool, error) {
	trimmed := strings.TrimSpace(rawCandidate)
	candidate, err := ice.UnmarshalCandidate(trimmed)
	if err != nil {
		return tcpCandidatePlan{}, false, fmt.Errorf("parse upstream WebRTC candidate: %w", err)
	}
	if candidate.NetworkType() != ice.NetworkTypeTCP4 && candidate.NetworkType() != ice.NetworkTypeTCP6 {
		return tcpCandidatePlan{}, false, nil
	}
	if candidate.TCPType() != ice.TCPTypePassive {
		return tcpCandidatePlan{}, false, nil
	}
	if candidate.Component() != uint16(ice.ComponentRTP) || candidate.Type() != ice.CandidateTypeHost {
		return tcpCandidatePlan{}, false, nil
	}
	if candidate.Port() != 443 {
		return tcpCandidatePlan{}, false, fmt.Errorf("upstream WebRTC TCP proxy candidate uses disallowed port %d", candidate.Port())
	}
	address, err := netip.ParseAddr(candidate.Address())
	if err != nil {
		return tcpCandidatePlan{}, false, errors.New("upstream WebRTC TCP proxy candidate address must be an IP")
	}
	address = address.Unmap()
	if !isPublicProxyTarget(address) {
		return tcpCandidatePlan{}, false, errors.New("upstream WebRTC TCP proxy candidate address must be globally routable")
	}
	fields := strings.Fields(trimmed)
	if len(fields) < 8 {
		return tcpCandidatePlan{}, false, errors.New("upstream WebRTC TCP proxy candidate is malformed")
	}
	return tcpCandidatePlan{
		fields: fields,
		target: netip.AddrPortFrom(address, uint16(candidate.Port())),
	}, true, nil
}

func isPublicProxyTarget(address netip.Addr) bool {
	if !address.IsValid() || !address.IsGlobalUnicast() || address.IsUnspecified() || address.IsLoopback() ||
		address.IsPrivate() || address.IsLinkLocalUnicast() || address.IsLinkLocalMulticast() || address.IsMulticast() {
		return false
	}
	for _, prefix := range nonRoutableProxyTargetPrefixes {
		if prefix.Contains(address) {
			return false
		}
	}
	return true
}

func bundledICECredentials(description *sdp.SessionDescription) (iceCredentials, error) {
	if description == nil {
		return iceCredentials{}, errors.New("SDP is unavailable")
	}
	sessionUfrag, _ := description.Attribute("ice-ufrag")
	sessionPassword, _ := description.Attribute("ice-pwd")
	var selected iceCredentials
	for _, media := range description.MediaDescriptions {
		if media == nil {
			continue
		}
		ufrag := sessionUfrag
		if mediaUfrag, ok := media.Attribute("ice-ufrag"); ok {
			ufrag = mediaUfrag
		}
		password := sessionPassword
		if mediaPassword, ok := media.Attribute("ice-pwd"); ok {
			password = mediaPassword
		}
		ufrag = strings.TrimSpace(ufrag)
		password = strings.TrimSpace(password)
		if ufrag == "" && password == "" {
			continue
		}
		if ufrag == "" || password == "" {
			return iceCredentials{}, errors.New("SDP contains incomplete ICE credentials")
		}
		current := iceCredentials{ufrag: ufrag, password: password}
		if selected.ufrag == "" {
			selected = current
			continue
		}
		if selected != current {
			return iceCredentials{}, errors.New("SDP contains inconsistent bundled ICE credentials")
		}
	}
	if selected.ufrag == "" {
		selected = iceCredentials{ufrag: strings.TrimSpace(sessionUfrag), password: strings.TrimSpace(sessionPassword)}
	}
	if selected.ufrag == "" || selected.password == "" {
		return iceCredentials{}, errors.New("SDP is missing ICE credentials")
	}
	return selected, nil
}

func closeCandidateTunnels(tunnels []*tcpCandidateTunnel) error {
	var closeErrors []error
	for _, tunnel := range tunnels {
		if err := tunnel.Close(); err != nil {
			closeErrors = append(closeErrors, err)
		}
	}
	return errors.Join(closeErrors...)
}

func newTCPCandidateTunnel(target netip.AddrPort, dialer proxy.ContextDialer, expectedUser, remotePassword string, logger *slog.Logger) (*tcpCandidateTunnel, error) {
	if !isPublicProxyTarget(target.Addr()) || target.Port() != 443 {
		return nil, errors.New("relay TCP proxy target is not allowed")
	}
	if dialer == nil || strings.TrimSpace(expectedUser) == "" || strings.TrimSpace(remotePassword) == "" {
		return nil, errors.New("relay TCP proxy tunnel configuration is incomplete")
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	network := "tcp4"
	listenAddress := "127.0.0.1:0"
	if target.Addr().Is6() {
		network = "tcp6"
		listenAddress = "[::1]:0"
	}
	listener, err := net.Listen(network, listenAddress)
	if err != nil {
		return nil, fmt.Errorf("listen for relay TCP proxy candidate: %w", err)
	}
	tunnelContext, cancelTunnel := context.WithCancel(context.Background())
	tunnel := &tcpCandidateTunnel{
		listener:        listener,
		target:          target,
		dialer:          dialer,
		expectedUser:    expectedUser,
		remotePassword:  remotePassword,
		logger:          logger,
		connections:     make(map[net.Conn]struct{}),
		validationSlots: make(chan struct{}, maxUnauthenticatedTCPConns),
		ctx:             tunnelContext,
		cancel:          cancelTunnel,
	}
	go tunnel.accept()
	return tunnel, nil
}

func (t *tcpCandidateTunnel) accept() {
	for {
		connection, err := t.listener.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				t.logger.Warn("relay tcp tunnel: accept candidate connection failed", "error", err)
			}
			return
		}
		if !t.trackConnection(connection) {
			if err := connection.Close(); err != nil {
				t.logger.Debug("relay tcp tunnel: close connection after shutdown", "error", err)
			}
			return
		}
		select {
		case t.validationSlots <- struct{}{}:
			go func() {
				defer func() { <-t.validationSlots }()
				t.handleConnection(connection)
			}()
		default:
			t.untrackAndClose(connection)
			t.logger.Warn("relay tcp tunnel: rejected excess unauthenticated candidate connection")
		}
	}
}

func (t *tcpCandidateTunnel) handleConnection(client net.Conn) {
	firstFrame, err := readValidatedICEBindingFrame(client, t.expectedUser, t.remotePassword)
	if err != nil {
		t.untrackAndClose(client)
		t.logger.Warn("relay tcp tunnel: rejected unauthenticated candidate connection", "error", err)
		return
	}
	if !t.claim() {
		t.untrackAndClose(client)
		return
	}
	if err := t.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.logger.Debug("relay tcp tunnel: close claimed candidate listener", "error", err)
	}
	upstream, err := t.dialer.DialContext(t.ctx, "tcp", t.target.String())
	if err != nil {
		t.untrackAndClose(client)
		t.logger.Warn("relay tcp tunnel: connect fixed upstream candidate failed", "error", err)
		return
	}
	if !t.trackConnection(upstream) {
		if err := upstream.Close(); err != nil {
			t.logger.Debug("relay tcp tunnel: close upstream after shutdown", "error", err)
		}
		t.untrackAndClose(client)
		return
	}
	if err := writeAll(upstream, firstFrame); err != nil {
		t.untrackAndClose(upstream)
		t.untrackAndClose(client)
		t.logger.Warn("relay tcp tunnel: forward authenticated ICE frame failed", "error", err)
		return
	}
	t.notifyForwardingStarted()

	copyDone := make(chan struct{}, 2)
	copyConnection := func(destination, source net.Conn) {
		_, _ = io.Copy(destination, source)
		copyDone <- struct{}{}
	}
	go copyConnection(upstream, client)
	go copyConnection(client, upstream)
	<-copyDone
	t.untrackAndClose(upstream)
	t.untrackAndClose(client)
	<-copyDone
}

func (t *tcpCandidateTunnel) setForwardingStartedHandler(handler func()) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.onForwardingStarted = handler
	t.mu.Unlock()
}

func (t *tcpCandidateTunnel) notifyForwardingStarted() {
	if t == nil {
		return
	}
	t.mu.Lock()
	handler := t.onForwardingStarted
	t.mu.Unlock()
	if handler != nil {
		handler()
	}
}

func (t *tcpCandidateTunnel) trackConnection(connection net.Conn) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return false
	}
	t.connections[connection] = struct{}{}
	return true
}

func (t *tcpCandidateTunnel) untrackAndClose(connection net.Conn) {
	if connection == nil {
		return
	}
	t.mu.Lock()
	delete(t.connections, connection)
	t.mu.Unlock()
	if err := connection.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.logger.Debug("relay tcp tunnel: close tunnel connection", "error", err)
	}
}

func (t *tcpCandidateTunnel) claim() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed || t.claimed {
		return false
	}
	t.claimed = true
	return true
}

func (t *tcpCandidateTunnel) Close() error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	cancel := t.cancel
	connections := make([]net.Conn, 0, len(t.connections))
	for connection := range t.connections {
		connections = append(connections, connection)
	}
	t.connections = make(map[net.Conn]struct{})
	t.mu.Unlock()
	if cancel != nil {
		cancel()
	}

	var closeErrors []error
	if t.listener != nil {
		if err := t.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			closeErrors = append(closeErrors, err)
		}
	}
	for _, connection := range connections {
		if err := connection.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			closeErrors = append(closeErrors, err)
		}
	}
	return errors.Join(closeErrors...)
}

func readValidatedICEBindingFrame(connection io.Reader, expectedUser, remotePassword string) ([]byte, error) {
	var header [2]byte
	if _, err := io.ReadFull(connection, header[:]); err != nil {
		return nil, fmt.Errorf("read ICE-TCP frame header: %w", err)
	}
	frameSize := int(binary.BigEndian.Uint16(header[:]))
	if frameSize < stunMessageHeaderSize || frameSize > maxInitialSTUNFrameSize {
		return nil, fmt.Errorf("invalid initial ICE-TCP STUN frame size %d", frameSize)
	}
	payload := make([]byte, frameSize)
	if _, err := io.ReadFull(connection, payload); err != nil {
		return nil, fmt.Errorf("read ICE-TCP STUN frame: %w", err)
	}
	message := stun.NewWithOptions(stun.WithStrict(true))
	if err := stun.Decode(payload, message); err != nil {
		return nil, fmt.Errorf("decode initial ICE-TCP STUN message: %w", err)
	}
	if len(payload) != stunMessageHeaderSize+int(message.Length) {
		return nil, errors.New("initial ICE-TCP STUN message contains trailing data")
	}
	if message.Type != stun.BindingRequest {
		return nil, fmt.Errorf("initial ICE-TCP STUN message has unexpected type %s", message.Type)
	}
	var username stun.Username
	if err := username.GetFrom(message); err != nil {
		return nil, fmt.Errorf("read initial ICE-TCP STUN username: %w", err)
	}
	if string(username) != expectedUser {
		return nil, errors.New("initial ICE-TCP STUN username does not match the media session")
	}
	if err := stun.NewShortTermIntegrity(remotePassword).Check(message); err != nil {
		return nil, fmt.Errorf("verify initial ICE-TCP STUN integrity: %w", err)
	}
	if err := stun.Fingerprint.Check(message); err != nil {
		return nil, fmt.Errorf("verify initial ICE-TCP STUN fingerprint: %w", err)
	}
	frame := make([]byte, len(header)+len(payload))
	copy(frame, header[:])
	copy(frame[len(header):], payload)
	return frame, nil
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written <= 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}
