# Zoe gateway integration

## Deployment

Set `VOICE_UPSTREAM_ROUTE=avas`, enable the media relay, and configure:

| Environment variable | Value |
|---|---|
| `VOICE_ZOE_URL` | Zoe HTTPS base URL, using a trusted certificate. |
| `VOICE_ZOE_TOKEN_FILE` | Mounted secret file containing Zoe's voice runner bearer. Read at startup; never put the value in compose or logs. |
| `VOICE_ZOE_GATEWAY_API_KEY_ID` | Numeric ID of the enabled gateway API key whose secret Zoe uses as `ZOE_VOICE_GATEWAY_API_KEY`. Defaults to `0` (gateway-owned admission disabled). Do not set this to a secret, prefix, or instance ID. |

Expose the Apple routes at the HTTPS origin advertised by Zoe. If terminating
TLS at a reverse proxy, do not expose the plaintext gateway listener and
disable headers/body logging for these routes. Upgrade Zoe, gateway, and Apple
together. Keep the existing SQLite database and data volume: startup upgrades
the schema in place. Rotated Zoe instance keys take effect at Zoe without
gateway reload.

## Signaling and reconciliation

- `POST /client/v1/voice/calls` accepts the instance bearer plus `request_id`,
  `offer_sdp`, and optional `voice`, `voice_mode`, `language_code`. The gateway
  sends the key only in the runner-authenticated `call.start`. No relay or
  upstream allocation occurs until Zoe accepts the request. The answer and
  IDs are returned only after Zoe acknowledges `call.established`.
- `POST /client/v1/voice/calls/{gateway_session_id}/hangup` checks the instance
  bearer against Zoe's owner-scoped session read. An authenticated, matching
  close attempt returns an empty `204`, not a lease release confirmation.
- `GET /v1/voice/requests/{request_id}/outcome` takes Zoe's `gateway_request_id`.
  A failed pre-binding create is reconciled through this read, not a fabricated
  `call.ended`. A durable attempt fence prevents re-creating media on replay.
- `GET /v1/voice/sessions/{gateway_session_id}/termination-evidence` requires
  `media_generation`, `account_lease_id`, and `account_lease_generation`. It
  returns `released` with the exact joined evidence, `not_yet` for an active or
  closing tuple, or `unknown`. It never closes media or mutates a lease.
- These reads, config, and in-call result/context APIs use the existing gateway
  API-key authentication. Gateway-owned mode disables legacy downstream create,
  DELETE, and account-lease release; legacy standalone mode retains them.

The client owns the silence timer and uses the same hangup route as a manual
hangup; both report `user_hangup`. Hangup, upstream end, transport loss,
shutdown, and failure close both relay legs. Only established calls send end
reports and reuse one
immutable identity, reason, close timestamp, and optional evidence across the
short in-memory retries. Evidence certifies gateway-local cleanup only; an
unresolved account remains unavailable. A restart does not turn an unresolved
lease into evidence. In-place upstream swaps are disabled for gateway-owned calls.

## Turns and delegation

After establishment, the runner sends `POST /runner/v1/voice/turns`. Ordinary
turns batch for up to 500 ms (20 turns and 512 KiB per batch). Text is capped at
64 KiB at a UTF-8 boundary, and the ordinary 1000-turn queue drops oldest entries
on overflow. Network/5xx retry with long backoff; 404 and 409 are terminal for
established calls.

A client-targeted AVAS delegation marks the corresponding user sequence with
the upstream item ID, even if Zoe already received the unmarked turn. Marked
turns bypass batching and ordinary backoff, are overflow-exempt, and retry
only network failures and 5xx for up to 10 seconds with jitter. Marked text is
never truncated to make it executable. On expiry, terminal response, or
oversize, the gateway attempts to append a failure note to the still-open
delegation and queues the ordinary transcript. There is no synthetic final
turn sent to Apple. Reports and turn queues are in memory only.
