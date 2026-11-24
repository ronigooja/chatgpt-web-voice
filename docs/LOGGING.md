# Logging

Gateway logs carry metadata only, never conversation content, request or response
bodies, credentials, or tokens at any level. Conversation content is available
in the in-memory `/viewer` event viewer. Event-shape sampling records types,
safe structural key names, lengths and counts, not payload values. Audio shape
samples also retain safe key names and charset classes (`hexchars`, `b64chars`,
`ascii`, `utf8`, or `mixed`), never values. Charset is a character-class hint,
not proof of an encoding. Client
voice access logs omit caller-supplied IDs, paths and user agents.

The viewer retains sensitive raw data-channel frames in memory only. Separate
conversation and audio rings retain at most 8 MiB each per generation; the audio
ring may contain voice data. Each frame is capped at 1 MiB and the last three
oversize frames are also retained separately, subject to the viewer's global
retention cap and TTL. Audio facts remain in generation metadata. Optional audio file capture is disabled by default; see the README environment table. Captures contain sensitive call audio. WAV files use mono 16-bit PCM at 24 kHz, inferred from observed frame sizes and duration. No silence is inserted. A crash can leave a header with zero data size. Files are stored under `<dir>/<session>/gen<N>/`, for example `20261125T001500.123Z_session_gen1_in.wav`; file names use UTC and a sanitized session identifier. Oldest finished files are removed when the configured byte limit is exceeded; open files are never removed.
