# Logging

Gateway logs carry metadata only, never conversation content, request or response
bodies, credentials, or tokens at any level. Conversation content is available
in the in-memory `/viewer` event viewer. Event-shape sampling records types,
safe structural key names, lengths and counts, not payload values. Audio shape
samples also retain safe key names and charset classes (`hexchars`, `b64chars`,
`ascii`, `utf8`, or `mixed`), never values. Charset is a character-class hint,
not proof of an encoding. Client
voice access logs omit caller-supplied IDs, paths and user agents.

Client-facing peer (`peer-a`) disconnect and failure warnings carry a bounded
transport snapshot and gateway RTP/DataChannel activity per leg. Periodic DEBUG
samples run every 10 seconds while the session is open after peer-a first
connects. Deltas use the previous periodic sample; missing samples, replaced
pairs, and counter resets do not count as zero. Selected-pair packet ages refer
to application packets. `sampler_lateness_ms` measures how late the last tick
was handled, not transport delay. A snapshot can be unavailable even when
gateway activity is known; a failure waits at most one second for stats before
teardown. Gateway writes count successful local API calls, not remote delivery
acknowledgments.

Candidate and pair rows use opaque candidate indexes. Remote rows identify type,
protocol, address family and selection. Pair rows carry application counters and
deltas; Pion credits received traffic to the selected pair, not necessarily to
the pair matching the packet source. Per-candidate receive age is not available.
At a drop, the last periodic sample with a selected pair can replace missing live
candidate evidence; source and sample-age fields distinguish it from live stats.

| Message | Level | When |
| --- | --- | --- |
| `relay peer disconnected`, `relay peer failed`, `relay session failed` | WARN | Existing transport or relay failure |
| `relay peer sample` | DEBUG | Periodic peer-a snapshot |
| `relay session created`, `relay session closing`, `relay session closed` | INFO | Normal lifecycle; also the completion of an unexpected close |
| `relay session closing` | WARN | Unexpected sideband or client DataChannel loss, unless peer-a failure already supplied the warning |
| `relay sideband closed` | DEBUG | Local teardown or sideband close without a tracked session drop |
| `relay sideband upstream error event` | WARN | An upstream error report; safe machine-code class and message-length class only |

Unexpected sideband closes attach cause, last-read age and frame counts to both
lifecycle lines. A final upstream error event or a failed last write is evidence
associated with the close, not proof of causality; the terminal read class and
WebSocket close code are included when available. Neither observation initiates
teardown on its own.

Audio facts remain in generation metadata. Optional audio file capture is disabled by default; see the README environment table. Captures contain sensitive call audio. WAV files use mono 16-bit PCM at 24 kHz, inferred from observed frame sizes and duration. No silence is inserted. A crash can leave a header with zero data size. Files are stored under `<dir>/<session>/gen<N>/`, for example `20261125T001500.123Z_session_gen1_in.wav`; file names use UTC and a sanitized session identifier. Oldest finished files are removed when the configured byte limit is exceeded; open files are never removed.

Frames dropped when the capture queue is full are not replaced by silence, so a file may be shorter than the wall-clock span between its timestamp and the next file’s timestamp. The 24000 Hz sample rate is inferred from observed audio frames.
