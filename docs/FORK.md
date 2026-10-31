# Fork scope and provenance

This fork preserves the upstream ChatGPT Web Voice gateway and imports the
llm-auth-bridge `bridge-v1` compatibility API. Credential synchronization is
specified in [BRIDGE_V1.md](BRIDGE_V1.md). The bridge import itself does not supply
Zoe session lease support. This fork adds it separately: with
`VOICE_RELAY_ENABLED` defaults to `true`, so the gateway terminates media (set
it to `false` to retain client-direct media) and
`POST /v1/voice/account-leases/{lease_id}/release` returns gateway-attested
termination evidence. That evidence is a local observation that the gateway closed
its own peers; it does not prove the upstream service reclaimed the account.
Sessions whose media is client-direct, and any release the gateway cannot observe
cleanly, still fail closed with 503. The design, status, and limits are in
[RELAY_PLAN.md](RELAY_PLAN.md).

## Exact imported sources

- Upstream: <https://github.com/Space3044/chatgpt-web-voice>, commit
  `6338ac478070aaa34bfe7a3c0469caf9e9174c02`.
- Patch source: <https://github.com/ronigooja/llm-auth-bridge/blob/f9f5ad08292fac76da9e0bbe79ee50ed71f68a21/deploy/voice/bridge-v1.patch>.
- Patch SHA-256:
  `23b0d17b05c20ca36488774b2c19927cd41d251f78e0cc64502c28cb757440e0`.
- The patch was applied without modification, including its synthetic contract,
  persistence, transport, authentication, and device propagation tests.

## Verification and publishing

For pushes to `main` or `integration/**`, pull requests, and manual runs, the
Linux CI workflow uses GitHub-hosted Ubuntu runners. It runs the full Go test
suite, the full race suite, the server build, and a focused rerun of the imported
bridge and transport/device regression tests. Go follows the version in `go.mod`.
Actions are pinned to immutable commits and dependency caching is disabled.

When a verification command fails, CI shows and retains only its final 160 lines
within a 64 KiB limit. Failure excerpts expire after seven days; successful
command logs remain temporary runner files and are not uploaded. CI uses
synthetic credentials and loopback fixtures, with no account or deployment
secrets. Passing CI establishes source and fixture compatibility; it does not
establish real provider account, installation, or end-to-end voice acceptance.

The inherited image publishing workflow is removed. This fork does not publish
containers on branch pushes or tags. Upstream image references in inherited
usage examples do not contain this fork's bridge API. When deploying this fork,
build from an explicitly reviewed fork commit, and record that commit alongside
the resulting binary or image. Before upgrading an existing installation, follow
the database and encryption-key backup requirements in [BRIDGE_V1.md](BRIDGE_V1.md).
