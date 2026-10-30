package relay

import "log/slog"

// NewForTest returns a relay that also gathers loopback candidates so tests in
// other packages can run both peers on one host. Do not use it in production.
func NewForTest(logger *slog.Logger) (*PionRelay, error) {
	return New(Config{includeLoopback: true}, logger)
}
