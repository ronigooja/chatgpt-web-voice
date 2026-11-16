-- Applies to upgrade tests for databases migrated through commit 411251c.
-- CREATE statements copied from 411251c:internal/store/schema.go (formatting only).
-- Trailing ALTER statements materialize that commit's existing migration helpers.
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS accounts (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 email TEXT NOT NULL DEFAULT '',
 access_token TEXT NOT NULL,
 token_hash TEXT NOT NULL DEFAULT '',
 proxy TEXT NOT NULL DEFAULT '',
 status TEXT NOT NULL DEFAULT '正常',
 disabled INTEGER NOT NULL DEFAULT 0,
 invalid_at REAL NOT NULL DEFAULT 0,
 last_used_at TEXT,
 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_accounts_available ON accounts(disabled, status, last_used_at, id);
CREATE TABLE IF NOT EXISTS api_keys (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 name TEXT NOT NULL,
 key_prefix TEXT NOT NULL,
 secret_hash TEXT NOT NULL UNIQUE,
 enabled INTEGER NOT NULL DEFAULT 1,
 last_used_at TEXT,
 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_api_keys_enabled ON api_keys(enabled, id);
CREATE TABLE IF NOT EXISTS conversations (
 id TEXT PRIMARY KEY,
 owner TEXT NOT NULL,
 title TEXT NOT NULL DEFAULT '',
 title_locked INTEGER NOT NULL DEFAULT 0,
 preview TEXT NOT NULL DEFAULT '',
 account_id INTEGER NOT NULL DEFAULT 0,
 upstream_conversation_id TEXT NOT NULL DEFAULT '',
 upstream_parent_message_id TEXT NOT NULL DEFAULT '',
 upstream_voice_session_id TEXT NOT NULL DEFAULT '',
 conversation_state TEXT NOT NULL DEFAULT 'new',
 conversation_reason TEXT NOT NULL DEFAULT '',
 conversation_confirmed INTEGER,
 context_chars INTEGER NOT NULL DEFAULT 0,
 opening_context_version INTEGER NOT NULL DEFAULT 0,
 opening_context_kind TEXT NOT NULL DEFAULT '',
 opening_context_items INTEGER NOT NULL DEFAULT 0,
 opening_context_chars INTEGER NOT NULL DEFAULT 0,
 opening_context_applied_at TEXT NOT NULL DEFAULT '',
 gateway_voice_session_id TEXT NOT NULL DEFAULT '',
 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_conversations_owner_updated ON conversations(owner, updated_at DESC, id DESC);
CREATE TABLE IF NOT EXISTS call_sessions (
 voice_session_id TEXT PRIMARY KEY,
 owner TEXT NOT NULL,
 caller_kind TEXT NOT NULL DEFAULT '',
 caller_label TEXT NOT NULL DEFAULT '',
 api_key_id INTEGER NOT NULL DEFAULT 0,
 account_id INTEGER NOT NULL DEFAULT 0,
 upstream_conversation_id TEXT NOT NULL DEFAULT '',
 upstream_parent_message_id TEXT NOT NULL DEFAULT '',
 upstream_voice_session_id TEXT NOT NULL DEFAULT '',
 voice TEXT NOT NULL DEFAULT '',
 voice_mode TEXT NOT NULL DEFAULT '',
 language_code TEXT NOT NULL DEFAULT '',
 status TEXT NOT NULL DEFAULT 'active',
 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 released_at TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_call_sessions_updated ON call_sessions(updated_at DESC, voice_session_id DESC);
CREATE INDEX IF NOT EXISTS idx_call_sessions_owner ON call_sessions(owner, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_call_sessions_account ON call_sessions(account_id, updated_at DESC);
CREATE TABLE IF NOT EXISTS conversation_messages (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
 client_id TEXT NOT NULL,
 role TEXT NOT NULL CHECK (role IN ('user', 'assistant')),
 content TEXT NOT NULL,
 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 UNIQUE(conversation_id, client_id)
);
CREATE INDEX IF NOT EXISTS idx_conversation_messages_conversation ON conversation_messages(conversation_id, id);
CREATE TABLE IF NOT EXISTS auth_sessions (
 token_hash TEXT PRIMARY KEY,
 username TEXT NOT NULL,
 expires_at TEXT NOT NULL,
 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_auth_sessions_expires ON auth_sessions(expires_at);
CREATE TABLE IF NOT EXISTS voice_creation_outcomes (
 request_id TEXT NOT NULL,
 owner TEXT NOT NULL,
 outcome TEXT NOT NULL,
 attempt_id TEXT NOT NULL DEFAULT '',
 voice_session_id TEXT,
 media_generation INTEGER,
 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 PRIMARY KEY (owner, request_id)
);
CREATE INDEX IF NOT EXISTS idx_voice_creation_outcomes_updated ON voice_creation_outcomes(updated_at);
CREATE TABLE IF NOT EXISTS bridge_accounts (
 remote_id TEXT PRIMARY KEY,
 identity TEXT NOT NULL,
 account_id INTEGER NOT NULL UNIQUE REFERENCES accounts(id) ON DELETE CASCADE,
 device_id TEXT NOT NULL DEFAULT '',
 expires_at TEXT NOT NULL DEFAULT 'null'
);
CREATE TABLE IF NOT EXISTS bridge_idempotency (
 key TEXT PRIMARY KEY, remote_id TEXT NOT NULL, payload_digest TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS voice_media_sessions (
 voice_session_id TEXT NOT NULL,
 media_generation INTEGER NOT NULL,
 owner TEXT NOT NULL DEFAULT '',
 account_id INTEGER NOT NULL,
 lease_id TEXT NOT NULL,
 lease_generation INTEGER NOT NULL,
 relay_session_id TEXT NOT NULL DEFAULT '',
 state TEXT NOT NULL CHECK (state IN ('active', 'closing', 'terminated', 'unknown')),
 peer_a_closed INTEGER NOT NULL DEFAULT 0,
 peer_b_closed INTEGER NOT NULL DEFAULT 0,
 data_channels_closed INTEGER NOT NULL DEFAULT 0,
 tunnels_closed INTEGER NOT NULL DEFAULT 0,
 close_reason TEXT NOT NULL DEFAULT '',
 evidence_id TEXT NOT NULL DEFAULT '',
 evidence_error TEXT NOT NULL DEFAULT '',
 created_at TEXT NOT NULL,
 updated_at TEXT NOT NULL,
 terminated_at TEXT NOT NULL DEFAULT '',
 PRIMARY KEY (voice_session_id, media_generation)
);
CREATE INDEX IF NOT EXISTS idx_voice_media_sessions_state ON voice_media_sessions(state, updated_at);
CREATE TABLE IF NOT EXISTS voice_termination_evidence (
 evidence_id TEXT PRIMARY KEY,
 voice_session_id TEXT NOT NULL,
 media_generation INTEGER NOT NULL,
 account_id INTEGER NOT NULL,
 lease_id TEXT NOT NULL,
 lease_generation INTEGER NOT NULL,
 observed_at TEXT NOT NULL,
 reason TEXT NOT NULL DEFAULT '',
 UNIQUE (voice_session_id, media_generation, lease_id, lease_generation)
);
CREATE TRIGGER IF NOT EXISTS voice_termination_evidence_no_update
 BEFORE UPDATE ON voice_termination_evidence
 BEGIN SELECT RAISE(ABORT, 'termination evidence is immutable'); END;
CREATE TRIGGER IF NOT EXISTS voice_termination_evidence_no_delete
 BEFORE DELETE ON voice_termination_evidence
 BEGIN SELECT RAISE(ABORT, 'termination evidence is immutable'); END;
CREATE UNIQUE INDEX IF NOT EXISTS idx_accounts_token_hash ON accounts(token_hash) WHERE token_hash <> '';
ALTER TABLE accounts ADD COLUMN cooldown_until INTEGER NOT NULL DEFAULT 0;
ALTER TABLE accounts ADD COLUMN failure_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE accounts ADD COLUMN unauthorized_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE accounts ADD COLUMN expires_at INTEGER NOT NULL DEFAULT 0;
ALTER TABLE accounts ADD COLUMN outcome_at INTEGER NOT NULL DEFAULT 0;
ALTER TABLE accounts ADD COLUMN media_route TEXT NOT NULL DEFAULT 'direct';
ALTER TABLE call_sessions ADD COLUMN conversation_state TEXT NOT NULL DEFAULT 'new';
ALTER TABLE call_sessions ADD COLUMN conversation_reason TEXT NOT NULL DEFAULT '';
ALTER TABLE call_sessions ADD COLUMN conversation_confirmed INTEGER;
ALTER TABLE call_sessions ADD COLUMN context_chars INTEGER NOT NULL DEFAULT 0;
ALTER TABLE call_sessions ADD COLUMN opening_context_version INTEGER NOT NULL DEFAULT 0;
ALTER TABLE call_sessions ADD COLUMN opening_context_kind TEXT NOT NULL DEFAULT '';
ALTER TABLE call_sessions ADD COLUMN opening_context_items INTEGER NOT NULL DEFAULT 0;
ALTER TABLE call_sessions ADD COLUMN opening_context_chars INTEGER NOT NULL DEFAULT 0;
ALTER TABLE call_sessions ADD COLUMN opening_context_applied_at TEXT NOT NULL DEFAULT '';
ALTER TABLE voice_media_sessions ADD COLUMN retired_legs INTEGER NOT NULL DEFAULT 0;
ALTER TABLE voice_media_sessions ADD COLUMN retired_legs_unclean INTEGER NOT NULL DEFAULT 0;
ALTER TABLE voice_media_sessions ADD COLUMN current_account_id INTEGER NOT NULL DEFAULT 0;
