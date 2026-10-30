# Gateway media relay and account rotation plan

Status: proposed plan. No relay code exists yet. This document applies to work
in this fork that changes how voice media is terminated, how account leases are
released, and how pool accounts are selected for continuous voice.

## Goal

Zoe needs voice that can run continuously for 24 hours and can change ChatGPT
accounts without operator action. Today the pool is fenced after every hangup,
so continuous voice and automatic account change both fail.

## Why the pool is fenced today

- Media is client-direct. The client opens WebRTC to the upstream service; the
  gateway only proxies SDP (`internal/voice/service.go`, `CreateSession`) and has
  no PeerConnection.
- `DELETE /v1/voice/sessions/{id}` only drops the gateway binding and marks the
  call session released (`ReleaseSession`). It cannot show that the peer stopped.
- `ReleaseAccountLease` returns "cannot obtain upstream WebRTC termination
  evidence" while `Capabilities()` still advertises support. These two disagree.
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
  conversation event the gateway reads today (`UpdateSessionContext`).
- It did not show that closing peer B ends the upstream session or releases the
  account. Local close success is not remote termination.
- It ran from one host and one account. TCP candidate handling (the answer
  contains TCP host candidates) was not tested through an account proxy.
- It does not predict stability; the endpoint is an internal web interface and
  can change.

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

## Continuous operation

- Rebuild media every 30 to 60 minutes (tunable). Create the next generation
  first, release the old one only after the new one is connected and its
  DataChannel is ready. If the new generation fails, keep the old one.
- Increment the media generation and the lease generation on each rebuild.
  Size the relay capacity for steady sessions plus generations being replaced.
- Replace the 180 second binding TTL behavior. Expiry must mark the session
  `closing`, close both peers, then write evidence. Deleting the in-memory binding
  is not a close.
- Whether a conversation can continue across an account change is not known and
  must be tested. Until it is, state that a rebuild onto a different account may
  start a new upstream conversation.

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
| 2 | Route `CreateSession` through peers A and B; return peer A's answer, not the upstream answer | `internal/voice/service.go` |
| 3 | Lease identity, generations, evidence tables, idempotent release; make `Capabilities()` truthful | `internal/store/schema.go`, `internal/callsessions`, `internal/api/voice.go` |
| 4 | Cooldown, expiry guard, generation rebuild, restart recovery of unknown state | `internal/accounts/*`, `internal/config/config.go`, `internal/voice/service.go` |

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
- Phase 4: generation rebuild; new connection fails and old one survives;
  allocation stops before token expiry; `Retry-After`; multi-account rotation;
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
4. Rebuild failure handling is the most likely source of defects.
5. Two pool accounts limit rotation regardless of code.
6. Relaying audio changes the network path the upstream sees (source address
   moves from the client to the gateway host). Account and proxy configuration
   must be reviewed.
7. This fork's `FORK.md` says Zoe lease support is a separate integration change;
   update it when phase 3 lands.

## Out of scope

Deployment, container resource limits, changes to Zoe, changes to CLIProxyAPI,
and changes to production configuration.
