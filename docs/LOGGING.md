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
retention cap and TTL. `VOICE_FRAME_RING_SIZE` (default 200) and
`VOICE_AUDIO_RING_SIZE` (default 300) control frame counts; zero disables the
respective ring. Login or the viewer read key is required to read frames via
`GET /api/call-sessions/{id}/generations/{gen}/frames`.
