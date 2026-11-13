# Logging

When adding or changing gateway logs, follow the full level standard in
`ronigooja/Zoe` at `docs/operations/logging.md`: ERROR for failures, WARN for
degraded or rejected operations, INFO for metadata only, and DEBUG as the only
level permitted to carry content text. Never log tokens, API keys, credentials,
or Authorization headers at any level.

When final-turn mapping is enabled, `VOICE_ZOE_FINAL_TURN_DEBUG_TEXT` defaults
to `true`; its deliberately gated content logs contain full, untruncated text
at DEBUG only. `VOICE_LOG_LEVEL` defaults to `debug`; explicit switch `false`
disables content logs, while `info` filters them out. Event-shape sampling,
transcript forwarding, create instructions, and relay handoff retain their
existing no-text logging contracts; this switch does not relax those contracts.

When updating an existing deployment, inspect its explicit environment overrides:
`VOICE_LOG_LEVEL=info` must change to `debug` for DEBUG records to appear, and
an explicit debug-text `false` must be removed or changed to `true`. The deployed
host's `/opt/llm-auth-bridge/compose.yaml` is outside this repository and is not
modified here.
