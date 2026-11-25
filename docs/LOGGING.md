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
was handled, not transport delay. Candidate details are limited to protocol and
address family. A snapshot can be unavailable even when gateway activity is
known; a failure waits at most one second for stats before teardown. Gateway
writes count successful local API calls, not remote delivery acknowledgments.

Audio facts remain in generation metadata. Optional audio file capture is disabled by default; see the README environment table. Captures contain sensitive call audio. WAV files use mono 16-bit PCM at 24 kHz, inferred from observed frame sizes and duration. No silence is inserted. A crash can leave a header with zero data size. Files are stored under `<dir>/<session>/gen<N>/`, for example `20261125T001500.123Z_session_gen1_in.wav`; file names use UTC and a sanitized session identifier. Oldest finished files are removed when the configured byte limit is exceeded; open files are never removed.

Frames dropped when the capture queue is full are not replaced by silence, so a file may be shorter than the wall-clock span between its timestamp and the next file’s timestamp. The 24000 Hz sample rate is inferred from observed audio frames.
