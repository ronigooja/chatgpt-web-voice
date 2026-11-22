# Gateway media relay and account rotation plan

Status: media relay, termination evidence, and account admission are implemented.
Long-running provider behavior remains unverified. This document applies to work
in this fork that changes how voice media is terminated, how account leases are
released, and how pool accounts are selected for continuous voice.

## Goal

Zoe needs voice that can run continuously for 24 hours and can change ChatGPT
accounts without operator action. Today the pool is fenced after every hangup,
so continuous voice and automatic account change both fail.

## Why the pool is fenced today

- Media was client-direct before the gateway relay was enabled by default. The
  client opened WebRTC to the upstream service; the gateway only proxied SDP
  (`internal/voice/service.go`, `CreateSession`) and had no PeerConnection.
- `DELETE /v1/voice/sessions/{id}` drops the gateway binding and marks the call
  session released (`ReleaseSession`). After a release or clean relay
  termination, an owner-scoped repeat is acknowledged as already released;
  unknown and cross-key ids remain 404.
- `ReleaseAccountLease` returns "cannot obtain upstream WebRTC termination
  evidence". (An earlier draft of this plan also said a `Capabilities()` function
  advertised support; no such function exists in this repository, and
  `/bridge/v1/capabilities` makes no termination claim.)
- A consumer that requires trusted termination evidence (Zoe) therefore blocks
  the whole account pool after each session.

## Decision

Terminate media in the gateway. The gateway owns two PeerConnections per call:

```text
client  <-WebRTC->  PeerConnection A  <-bridge->  PeerConnection B  <-WebRTC->  upstream
                                                   (SDP over /realtime/wm)
```

- Put the relay in a new package, `internal/relay`. Do not create a separate
  repository: termination evidence depends on the account lease and session
  records in this process, and a second service would add a trust boundary and a
  network hop.
- Expose a small interface from `internal/relay` (create session, close session,
  return a termination observation). Keep Pion types out of `internal/accounts`
  and the HTTP contract, so the relay can be split out later.
- Copy the media bridge from CLIProxyAPI
  (`internal/client/codex/live/media.go`, MIT licensed, Pion based). Keep its
  copyright notice in the copied files. Do not copy its handler, auth scheduler,
  or Gin code. No third-party project was found that could replace this code.
- Rewrite a small cooldown and rotation policy in `internal/accounts`, following
  the rules in CLIProxyAPI `sdk/cliproxy/auth` (cooldown queue, `Retry-After`,
  exponential backoff, stale-result protection).
- Container memory and CPU limits are an operator concern and are outside this
  plan.

## What `/realtime/wm` is

`https://chatgpt.com/realtime/wm?dcid=0` is the ChatGPT web voice signaling
endpoint this gateway already calls. The request is `multipart/form-data` with
fields `sdp` (offer) and `session` (JSON), authenticated with the account web
`access_token` and a browser-like header set. A successful response is the answer
SDP as plain text. It is not the Codex endpoint that CLIProxyAPI relays
(`/backend-api/codex/realtime/calls`), and the two must not be assumed
equivalent.

## Negotiation probe (2026-10-30)

A one-off probe ran inside the production gateway container on the existing
image (`ba8b3093`), using a copy of the account database and the container's own
encryption key. It sent one request with one pool account and printed only
structural facts. No token, account identifier, address, or SDP body was output.
The probe binary and database copy were removed afterwards.

Observed:

| Check | Result |
|---|---|
| Pion-generated offer accepted by `/realtime/wm` | Yes. HTTP 201, plain SDP answer |
| Answer media sections | `m=audio ... UDP/TLS/RTP/SAVPF`, `m=application ... UDP/DTLS/SCTP` |
| Answer ICE candidates | UDP host and TCP host only; no relay/srflx candidates |
| Pion `SetRemoteDescription` with the answer | Accepted |
| ICE and peer connection state | `checking`, then `connected`; closed cleanly by us |
| Negotiated `oai-events` channel (id 0) created by the gateway side | Opened |
| Events received on the channel within the window | 2: one `data_message`, one `error` |
| Inbound audio RTP packets | 0 within the 25 second window |
| Peer connection close | Returned no error |

What this establishes: a gateway-side Pion peer can complete the full
negotiation and connect to the upstream. The relay architecture is feasible at
the signaling and transport level.

What this does not establish:

- The `error` event was not decoded, so its cause is unknown. Because it arrived
  with no audio, the session may have been rejected after connecting. The probe
  sent no microphone audio and used a minimal `session` payload; the cause could
  be either. This must be resolved in phase 0 before any relay code depends on
  it.
- It did not exercise sustained audio in either direction, barge-in, or any
  conversation event the gateway reads today (`UpdateSessionContext`). A relayed
  call also learns both ids from its own events.
- It did not show that closing peer B ends the upstream session or releases the
  account. Local close success is not remote termination.
- It ran from one host and one account. TCP candidate handling (the answer
  contains TCP host candidates) was not tested through an account proxy.
- It does not predict stability; the endpoint is an internal web interface and
  can change.

## Live probe (2026-10-30, phase 0 follow-up)

A second one-off probe ran in the production gateway container (account database
copy, container's own key, one pool account, synthetic speech audio). Output
was structural only. The probe binary and database copy were removed afterwards.

| Check | Result |
|---|---|
| Cause of the earlier `error` event | Gateway bug, not upstream rejection. `/realtime/wm` and the web client use a pre-negotiated `oai-events` channel (`negotiated`, id 0). A normally negotiated channel produced an SCTP PPID error. Fixed in `internal/relay`. |
| Direct media, about 40 s of audio | `/realtime/wm` 201; peer B ready in about 0.5 s; about 2,200 inbound RTP packets; `state_update`, `session_bootstrap`, `thinking_update`, `chat_message_delta`, `conversation_update`, `usage_update` events |
| Local close | All four components closed without error |
| Immediate new session on the same account after close | Accepted. This does not show the upstream released the account |
| Media through the stored account proxy (IPv6 egress) | Failed. The proxy refused TCP 443 to the upstream candidate; peer B ended `peer_b_failed`. Direct TCP 443 from the host worked |
| Media through an IPv4-egress SOCKS5 proxy, `socks5://` and `socks5h://` | Worked; peer B ready in about 9 s; about 2,200 inbound RTP packets |

| Same account, two simultaneous sessions | Accepted; first session undisturbed |
| Same account, resume with `conversation_id` + `parent_message_id` | Same conversation; earlier content known |
| Other account, resume with the first account's ids | Refused 3 of 3: `goodbye` (`reason=error`, `detail=internal_error`) about 1 s after ready, then close |
| Other account, fresh session | Normal |
| Fresh session, earlier history sent as one user text message | Accepted (`relay_message_processed`); no unprompted speech; later question about the history answered correctly. Without the text, the same question was not answerable |

Still not established: upstream release of the account (see "Termination
evidence").

## Termination evidence

Report `terminated=true` only when all of the following hold:

1. The exact `voice_session_id` and media generation match.
2. The lease ID and lease generation match.
3. Peer A, peer B, the DataChannel bridge, and any TCP tunnels closed without
   error.
4. The evidence row was written durably before the response.
5. The evidence cannot be overwritten for the same lease generation.

Treat these as unknown, and keep the lease blocked: a crash during close, one
side closing while the other fails, a close error, an evidence write failure, a
restart that finds only a session row, and a lost or timed-out release.

This is a gateway-attested local termination observation. It does not prove the
upstream service reclaimed the account. Say so wherever the evidence is
exposed.

New tables (names indicative):

- `voice_media_sessions`: session, media generation, account, lease, lease
  generation, state (`active`, `closing`, `terminated`, `unknown`), per-component
  close flags, close reason, timestamps. Primary key on session plus generation.
- `voice_termination_evidence`: evidence ID, the same identity fields,
  `observed_at`, reason. Unique on session, generation, lease, lease generation.

`call_sessions` gains the current generation, lease identity, and the latest
evidence reference.

## Session lifetime and opening context

A relayed call is not cut by idle TTL; its close handler reclaims the binding.
The upstream leg stays with the call until termination.
Legacy retired-leg columns remain intact, and an unclean historical retired leg
still prevents termination attestation.

Opening context is route-specific. WM has a proven upstream typed-text envelope;
this gateway uses that envelope only when a
live upstream write is available, and does not claim that it is silent. AVAS
creation accepts `initial_items`, which is the only proven silent creation form.
The AVAS sideband context-append event shape was observed by the disposable probe
(`session.context.append`), but no real upstream call has verified its behavior;
live AVAS opening-context injection therefore remains unsupported (501).

The WM opening-context API response reports `silent:"unverified"`: the envelope
is the typed-text form and may trigger a spoken reply. The live probe observed no
unprompted speech for the text, but this is not a protocol guarantee.
No forwarded `response.cancel`, system-role item, or `conversation.item.create`
silent form was found in this repository's traffic handling. Zoe must decide
whether to accept that uncertainty.

Measured 2026-10-30 (see "Live probe"): a different account cannot resume the
conversation; upstream sends `goodbye` (`reason=error`, `detail=internal_error`)
and closes (3 of 3). The same account resumes normally and may hold two sessions
at once.

## Account pool

Current behavior: least recently used pick; only an upstream 401 disables an
account, permanently; no cooldown; no expiry awareness. The pool stores only the
web `access_token`, with no refresh token, so tokens cannot be refreshed.

Changes:

- Add cooldown, failure count, and token-expiry fields. Skip accounts in
  cooldown or within an expiry guard window (configurable) when allocating.
- 429: cool down using `Retry-After`, minimum 10 seconds, exponential backoff
  without it. 5xx and network errors: short cooldown, do not mark invalid.
  Cloudflare challenge pages are not 401.
- 401: cool down and re-probe; mark invalid only on a repeated 401. Without a
  refresh token this is re-probing, not refreshing.
- Persist cooldown state so a restart does not erase it.
- Operational limit: the production pool has two accounts. Continuous voice with
  account change needs enough valid accounts and a process for replacing expired
  tokens.

## Phases

| Phase | Work | Main files |
|---|---|---|
| 0 | Decode the probe `error` event; repeat the probe with audio and a realistic `session`; record the `wm` request and response shape with a local fixture | `internal/voice/service.go`, tests |
| 1 | Port the Pion bridge into `internal/relay`; replace logrus, Gin, and CLIProxyAPI config; keep copyright notices | `internal/relay/*`, `go.mod` |
| 2 | Route `CreateSession` through peers A and B; return peer A's answer, not the upstream answer. Done behind `VOICE_RELAY_ENABLED`, with the per-account `media_route` field choosing direct or account-proxy media | `internal/voice/service.go` |
| 3 | Lease identity, generations, evidence tables, idempotent release. Done in `internal/mediaevidence`; generations are persisted and stay at 1 | `internal/store/schema.go`, `internal/callsessions`, `internal/api/voice.go` |
| 4 | Cooldown, expiry guard (done); restart recovery of unknown state (done in phase 3) | `internal/accounts/*` |

Phase 3 depends on phases 1 and 2. Phase 4 depends on phase 3. Phase 0 gates
phase 2.

## Tests

- Phase 1: bidirectional RTP; bidirectional `oai-events`; other channel labels
  closed; one-sided close; repeated close fires the handler once; concurrency
  limit.
- Phase 2: fixture upstream answer; A and B negotiation; 401, 429, 5xx; account
  retry; session resume.
- Phase 3: repeated release; stale generation; peer A or B close failure;
  evidence write failure; restart with unknown state; evidence uniqueness.
- Phase 4: allocation stops before token expiry; `Retry-After`; multi-account rotation;
  capacity at limit.

CI fixtures and loopback peers establish source and protocol behavior only. Real
provider acceptance needs a separate run with authorized accounts.

## Risks

1. The upstream endpoint is internal to ChatGPT web and may change or reject
   server-originated peers. The probe is one data point, not a guarantee.
2. The unexplained `error` event may mean a relayed peer is rejected in
   practice.
3. Termination evidence is local. Do not present it as proof that the upstream
   account is free.
5. Two pool accounts limit rotation regardless of code.
6. Relaying audio changes the network path the upstream sees (source address
   moves from the client to the gateway host). Account and proxy configuration
   must be reviewed.
7. This fork's `FORK.md` says Zoe lease support is a separate integration change;
   update it when phase 3 lands.

## Out of scope

Deployment, container resource limits, changes to Zoe, changes to CLIProxyAPI,
and changes to production configuration.
# Session continuity and AVAS route

`POST /v1/voice/sessions` adds `conversation`, whose value is `resumed` only
after a WM upstream event proves that both stored prior identifiers were echoed
by the same upstream conversation. At create time, before that event exists,
the conservative value is `new` with reason `unverified`; `conversation_confirmed`
is initially null and later becomes true or false from the event evidence.
`conversation_reason` may be `account_changed`, `unverified`, or
`upstream_refused`. `context_chars` counts
user and assistant transcript characters for the current upstream conversation
and resets for a new conversation. The gateway does not enforce a limit.

The default upstream remains WM. `VOICE_UPSTREAM_ROUTE=avas` selects the
probed Codex realtime create endpoint and requires its sideband alpha header;
AVAS always reports `new` with reason `avas_no_resume` because the probe did
not establish a resume identity field. AVAS failures are reported rather than falling back to WM. The probe did not
establish real-speech delegation, transcript text production, or initial-item
recall; those remain unverified.
