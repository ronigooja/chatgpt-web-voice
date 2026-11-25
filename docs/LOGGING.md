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

## Owned call creation

`owned_call_creation_failed` is WARN, once per failed dial returning 503.
`creation_stage` and `error_class` also annotate relay closing/closed lines when
the failed create initiates the close. A close error on those lines uses
`close_error_class`; it does not overwrite the create error. The terminal dial
warning can differ from an earlier relay close if account selection or an
outcome write fails afterward.

| `creation_stage` | 503 path | `error_class` | `stored_outcome` with no subsequent write failure |
| --- | --- | --- | --- |
| `admission` | Owned dial unavailable, unready or stopping | `internal` | unavailable |
| `zoe_start` | Zoe start transport, rejected status, quarantine or invalid reply | IO classes, `http_4xx`, `http_5xx`, `unexpected_status`, `pool_quarantined`, `malformed_response` | unavailable |
| `creation_claim` | Claim insert failure, or replay with no reusable answer | `db_write_failed`, `claim_conflict` | unavailable or existing claim |
| `request_validation` | Invalid owned start manifest or offer | `validation` | `never_established` before allocation; otherwise no stronger outcome |
| `account_reservation` | No eligible account, all occupied, failed account operation or media reservation | `pool_no_account`, `pool_quarantined`, `db_write_failed`, `evidence_failed` | `never_established` with no possible admission or only explicit rejections with verified teardown; otherwise `unknown` |
| `relay_construct` | Allocator failure or capacity refusal | IO classes, `capacity` | `never_established` for proven clean pre-POST refusal; otherwise `unknown` |
| `relay_apply_offer` | Relay rejects client offer | `validation` | `never_established` for proven clean pre-POST refusal; otherwise `unknown` |
| `upstream_post` | Request construction, send or transport with no usable response | IO classes | `unknown` |
| `upstream_response` | Response read failure, malformed SDP, unexpected status or explicit rejection at the attempt limit | IO classes, `malformed_response`, `unexpected_status`, `http_4xx`, `http_5xx` | `never_established` only for explicit rejections with verified teardown; otherwise `unknown` |
| `upstream_sideband` | Missing call ID, handshake failure or rejected upgrade | IO classes, `malformed_response`, `unexpected_status`, `http_4xx`, `http_5xx` | `unknown` |
| `answer_negotiation` | Relay cannot apply the upstream answer or generate the client answer | IO classes | `unknown` |
| `relay_attach` | Cannot attach the relay to its reserved media identity | `evidence_failed` | `unknown` |
| `outcome_write` | Cannot persist an outcome | `db_write_failed` | `pending` or `unknown`, according to what was committed |
| `binding_ack` | Zoe did not acknowledge binding, or media closed before the answer could be returned | IO classes, `malformed_response`, `unexpected_status`, `http_4xx`, `http_5xx` | `established` (raw claim, not an assertion that media is still open) |

IO classes are `transport_timeout`, `transport_refused`, `transport_eof`, `tls`,
`context_canceled`, and `internal`. `upstream_attempt_count` counts AVAS POSTs
handed to the HTTP client. `upstream_last_status` is the last observed AVAS POST
status, not the Zoe or sideband status. `relay_started` indicates observed
allocation; `relay_close_clean` requires durable matching closure for all
inspected failed relay allocations. `stored_outcome` is the raw claim state,
not the media-derived public outcome. `outcome_write_ok` is true only when this
dial wrote the claim and no outcome write failed. `elapsed_ms` starts at
successful claim acquisition. Unavailable status, close, claim and timing
fields are null (or omitted by the log handler).
`rejection_status` is present for an explicit Zoe or sideband 4xx/5xx response;
AVAS POST status is already recorded in `upstream_last_status`.

Request-fence access logs use a static route and a server-generated correlation
ID; they omit caller request IDs, user agents, remote addresses, and bodies.
`voice_request_fence_failed` records only the `fence_unavailable` error class.
`busy` is a normal retryable response, not an upstream error or proof of release.

Audio facts remain in generation metadata. Optional audio file capture is disabled by default; see the README environment table. Captures contain sensitive call audio. WAV files use mono 16-bit PCM at 24 kHz, inferred from observed frame sizes and duration. No silence is inserted. A crash can leave a header with zero data size. Files are stored under `<dir>/<session>/gen<N>/`, for example `20261125T001500.123Z_session_gen1_in.wav`; file names use UTC and a sanitized session identifier. Oldest finished files are removed when the configured byte limit is exceeded; open files are never removed.

Frames dropped when the capture queue is full are not replaced by silence, so a file may be shorter than the wall-clock span between its timestamp and the next file’s timestamp. The 24000 Hz sample rate is inferred from observed audio frames.
