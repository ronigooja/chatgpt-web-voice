# Zoe transcript forwarding

Applies when `VOICE_ZOE_URL` and `VOICE_ZOE_TOKEN_FILE` are both nonempty and the
trimmed token file contains a bearer token. Either absent setting or an empty
token disables forwarding. Configuration validation rejects unreadable files,
credentials in URLs, queries/fragments, and non-HTTPS destinations except HTTP
on localhost/loopback. Token contents are read once at service startup.

## Operator deployment

On the gateway host, provision the token through the existing secret-management
process at `/opt/llm-auth-bridge/.secrets/zoe_voice_ingest_token`. Do not put its
value in compose, source, logs, or shell arguments. Add this to the host compose
configuration (the repository compose remains opt-in/unmodified):

```yaml
services:
  chatgpt-web-voice:
    environment:
      VOICE_ZOE_URL: https://zoe.nodemgr.uk
      VOICE_ZOE_TOKEN_FILE: /run/secrets/zoe_voice_ingest_token
    secrets:
      - zoe_voice_ingest_token
secrets:
  zoe_voice_ingest_token:
    file: /opt/llm-auth-bridge/.secrets/zoe_voice_ingest_token
```

The compose secret `zoe_voice_ingest_token` mounts at
`/run/secrets/zoe_voice_ingest_token`. Use an image built from the forwarding
branch; this change does not deploy an image, alter external services, or modify
any debug/probe switch. Media evidence must be enabled: forwarding uses exactly
the gateway session ID and `voice_session_generation` returned by session
creation, not upstream call IDs. Sessions without a recorded generation are
not forwarded and produce a metadata-only warning.

## Event and delivery contract

AVAS sideband events are observed after the existing normalizer/injector.
`turn.created` opens a turn; `turn.done` supplies final transcript/content/text.
Client-target `delegation.created` supplies final user text. User completion via
both representations is deduplicated within the current user turn. Text deltas
(`turn.text.delta`, `output_text.delta`, `output_audio_transcript.delta`) append
to the open turn. The legacy WM `chat_message_delta` snapshots and append ops
are supported separately. Next-message start and media close flush unfinished
turns. Non-conversation events are ignored. Neither collector changes the
existing handoff tail or relies on final-turn/debug/probe switches.

One worker per call sends ordered sequences from zero to
`POST /runner/v1/voice/turns`, batches at most 20 turns/512 KiB, and waits at most
500 ms to accumulate a small batch. Each individual turn is limited to 64 KiB;
oversized turns are dropped and logged, never delivered as truncated text.
The 1,000-turn queue drops oldest unsent turns on overflow, retaining sequence
gaps and reporting counts. An already in-flight batch may still arrive after
its queue entry has been evicted. IDs/sequence/text remain stable across retry.

Network/5xx retry exponentially from 1 s to 30 s. Unknown sessions (404) retry
for two minutes; 409 is terminal. 400/413 batches are isolated to single turns
so only rejected turns are dropped. 401/403 retry, logging at most once per
minute per call. HTTPS certificate verification is always enabled, independent
of upstream `SkipSSLVerify`. Redirects are not followed. Destination response
bodies and network error strings are never logged. No turn text or bearer is
logged, even when other debug switches are on.

Media close initiates an asynchronous flush with a 60 s overall deadline,
including in-flight requests. Call control/media never waits for delivery.
There is **no disk persistence**: gateway crash/restart loses all queued and
unfinished turns, plus any in-flight delivery not yet accepted by Zoe. During
normal healthy operation the queueing window is up to 500 ms plus HTTP time;
during outages the loss window spans the entire backlog (up to 1,000 turns).
Shutdown does not provide process-wide draining; an immediate process exit
can interrupt the per-call 60 s flush. This is bounded in-memory reliability,
not restart-durable or unconditional lossless delivery.
