# bridge-v1 compatibility API

This fork adds a credential synchronization surface for llm-auth-bridge. It has
synthetic contract coverage and is **unverified with real provider accounts**.
Administrator Basic authentication is required for all `/bridge/v1/` routes;
browser cookies and downstream API keys do not authorize this surface. Existing
native administrator and downstream routes retain their original authentication.

`GET /bridge/v1/capabilities` returns `protocol: bridge-v1`, `explicit_route: true`
and `credential_digest: true`.

`PUT /bridge/v1/accounts/{remote_id}` accepts an object containing `identity`,
`credential_type: chatgpt_web`, `credential: {access_token, device_id?}`, `route`
and optional numeric or null `expires_at`. Route is exactly `{type: direct}` or
`{type: proxy, url: <full URL>}`. Proxy schemes are http, https, socks5 and socks5h.
Refresh tokens and unknown fields are rejected. Bodies are limited to 1 MiB.
Remote IDs contain only ASCII letters, digits, `-_.@+`, have at most 200 characters,
and may not begin with a dot or contain consecutive dots.

Supply an `Idempotency-Key` of at most 512 bytes. Same key and canonical request
replays read current state without rewriting credentials. Same key with a changed
request returns 409. Each remote ID maps atomically to one native account. Identity
changes and tokens already belonging to another native account return 409; native
accounts are never silently adopted. Successful fresh PUTs for the same identity
may intentionally replace the credential and route. Native disabled status is
preserved on updates. Deleting a native account deletes its bridge mapping; a
historical idempotency-key replay cannot recreate it.

`GET /bridge/v1/accounts/{remote_id}` returns 404 when absent and otherwise reports
`identity`, `credential_type`, `route` and `credential_digest`. Exact proxy
credentials are visible only on this authenticated management surface. Digest is
lowercase SHA-256 over the actual persisted credential object using Python's
`json.dumps(sort_keys=True, separators=(',', ':'), ensure_ascii=False)` encoding,
including literal U+2028 and U+2029. Credentials themselves are never returned.
Readback is consistent with an acknowledged PUT and reflects later native edits.

Native accounts remain SQLite rows; bridge ID, optional device ID and expiration
metadata are stored in `bridge_accounts`. Tokens, native proxy URLs and bridge
device IDs are AES-GCM sealed with `VOICE_TOKEN_ENCRYPTION_KEY`. Existing plaintext
proxy values are sealed during startup without changing their route. Device IDs
are used by provider requests; absence preserves the global device fallback.
Explicit `direct` uses an internal proxy sentinel interpreted by every transport
as bypassing process proxies. Empty native proxy values retain legacy inheritance.
Expiration is retained as synchronization metadata; provider token expiry behavior
remains unchanged. No account login, refresh exchange or live provider probe occurs
during bridge management requests.

Back up the SQLite database and encryption key before upgrade. Older binaries do
not understand encrypted proxy values or the direct sentinel: rollback requires
restoring the previous database backup and binary together.
