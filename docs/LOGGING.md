# Logging

When adding or changing gateway logs, follow the full level standard in
`ronigooja/Zoe` at `docs/operations/logging.md`: ERROR for failures, WARN for
degraded or rejected operations, INFO for metadata only, and DEBUG as the only
level permitted to carry content text. Never log tokens, API keys, credentials,
or Authorization headers at any level.

Legacy diagnostics gated by `VOICE_ZOE_FINAL_TURN_DEBUG_TEXT` (default `true`)
may carry content at DEBUG only. Gateway-owned calls do not use this content
switch. Client voice access logs omit caller-supplied IDs, paths, user agents,
and panic contents. Lifecycle and transcript delivery log status/metadata, not
response bodies, network error strings, or credentials. Event-shape sampling
and create instructions retain their no-text logging contracts.
