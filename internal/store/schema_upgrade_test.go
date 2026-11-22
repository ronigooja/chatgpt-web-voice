package store_test

import (
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/dyhhhhhh/chatgpt-web-voice/internal/callsessions"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/config"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/store"
	"github.com/dyhhhhhh/chatgpt-web-voice/internal/voice"
)

//go:embed testdata/schema_411251c.sql
var schema411251c string

func TestUpgradeFrom411251cPreservesRowsAndSupportsReceipts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "upgrade.db")
	legacy, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer legacy.Close()
	exec := func(conn *sql.DB, query string, args ...any) {
		t.Helper()
		if _, err := conn.Exec(query, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec(legacy, schema411251c)
	for i := 1; i <= 4; i++ {
		exec(legacy, `INSERT INTO accounts(id,email,access_token,token_hash,proxy,status,disabled,invalid_at,last_used_at,cooldown_until,failure_count,unauthorized_count,expires_at,outcome_at,media_route) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			i, fmt.Sprintf("fake-%d@example.invalid", i), fmt.Sprintf("tok-%d", i), fmt.Sprintf("hash-%d", i), "http://proxy.invalid", "正常", i%2, 12.5, "2026-01-01 00:00:00", 123, 2, 3, 456, 789, "account")
		exec(legacy, `INSERT INTO bridge_accounts(remote_id,identity,account_id,device_id,expires_at) VALUES(?,?,?,?,?)`, fmt.Sprintf("remote-%d", i), fmt.Sprintf("identity-%d", i), i, "device", "2030-01-01")
	}
	exec(legacy, `INSERT INTO bridge_idempotency VALUES('key','remote-1','digest')`)
	exec(legacy, `INSERT INTO api_keys(name,key_prefix,secret_hash) VALUES('fake','prefix','fake-hash')`)
	exec(legacy, `INSERT INTO auth_sessions(token_hash,username,expires_at) VALUES('fake','user','2030-01-01')`)
	exec(legacy, `INSERT INTO conversations(id,owner,title,title_locked,preview,account_id,upstream_conversation_id,upstream_parent_message_id,upstream_voice_session_id,gateway_voice_session_id,created_at,updated_at) VALUES('conversation','owner','title',1,'preview',1,'upstream','parent','upstream-voice','session','2026-01-01','2026-01-02')`)
	exec(legacy, `INSERT INTO conversation_messages(conversation_id,client_id,role,content) VALUES('conversation','message','user','synthetic')`)
	exec(legacy, `INSERT INTO call_sessions(voice_session_id,owner,account_id,upstream_conversation_id,context_chars,opening_context_version) VALUES('session','owner',1,'upstream',17,2),('released','owner',2,'other',19,1)`)
	exec(legacy, `INSERT INTO voice_creation_outcomes(request_id,owner,outcome,attempt_id,voice_session_id,media_generation) VALUES('request','owner','established','old-attempt','session',1)`)
	exec(legacy, `INSERT INTO voice_media_sessions(voice_session_id,media_generation,owner,account_id,lease_id,lease_generation,state,created_at,updated_at,retired_legs,retired_legs_unclean,current_account_id) VALUES('session',1,'owner',1,'lease',1,'active','2026-01-01','2026-01-01',2,1,3),('released',1,'owner',2,'lease-2',1,'terminated','2026-01-01','2026-01-01',0,0,2)`)
	exec(legacy, `INSERT INTO voice_termination_evidence(evidence_id,voice_session_id,media_generation,account_id,lease_id,lease_generation,observed_at,reason) VALUES('evidence','released',1,2,'lease-2',1,'2026-01-01','synthetic')`)

	// Capture every old column, including credentials and account/bridge metadata.
	tables := []string{"accounts", "bridge_accounts", "bridge_idempotency", "api_keys", "auth_sessions", "conversations", "conversation_messages", "call_sessions", "voice_creation_outcomes", "voice_media_sessions", "voice_termination_evidence"}
	queries := make(map[string]string)
	before := make(map[string][][]any)
	columnsBefore := make(map[string][][]any)
	for _, table := range tables {
		columnsBefore[table] = upgradeRows(t, legacy, "PRAGMA table_info("+table+")")
		rows, err := legacy.Query("SELECT * FROM " + table + " LIMIT 0")
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		rows.Close()
		if err != nil {
			t.Fatal(err)
		}
		queries[table] = "SELECT " + strings.Join(columns, ",") + " FROM " + table + " ORDER BY rowid"
		before[table] = upgradeRows(t, legacy, queries[table])
	}
	objectsQuery := `SELECT type,name,sql FROM sqlite_master WHERE type IN ('index','trigger') ORDER BY type,name`
	objects := upgradeRows(t, legacy, objectsQuery)
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}

	db, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	assertPreserved := func() {
		t.Helper()
		for _, table := range tables {
			// Preserve captured production columns and add only approved fields.
			wantColumns := append([][]any(nil), columnsBefore[table]...)
			if table == "call_sessions" {
				wantColumns = append(wantColumns,
					[]any{int64(26), "opening_context_hash", "TEXT", int64(1), "''", int64(0)},
					[]any{int64(27), "opening_context_generation", "INTEGER", int64(1), "0", int64(0)})
			}
			if table == "voice_creation_outcomes" {
				wantColumns = append(wantColumns, []any{int64(8), "gateway_owned", "INTEGER", int64(1), "0", int64(0)})
			}
			if table == "voice_media_sessions" {
				wantColumns = append(wantColumns,
					[]any{int64(21), "zoe_session_id", "TEXT", int64(1), "''", int64(0)},
					[]any{int64(22), "zoe_generation", "INTEGER", int64(1), "0", int64(0)})
			}
			if got := upgradeRows(t, db.Conn(), "PRAGMA table_info("+table+")"); !reflect.DeepEqual(got, wantColumns) {
				t.Errorf("%s columns after migration = %v; want %v", table, got, wantColumns)
			}
			if got := upgradeRows(t, db.Conn(), queries[table]); !reflect.DeepEqual(got, before[table]) {
				t.Errorf("%s rows changed during migration", table)
			}
		}
		// The new receipt table adds its own primary-key autoindex; all old
		// indexes (including partial uniqueness) and evidence triggers survive.
		got := upgradeRows(t, db.Conn(), objectsQuery)
		var oldObjects [][]any
		for _, row := range got {
			if !strings.Contains(fmt.Sprint(row[1]), "voice_result_receipts") {
				oldObjects = append(oldObjects, row)
			}
		}
		if !reflect.DeepEqual(oldObjects, objects) {
			t.Error("legacy indexes or triggers changed")
		}
	}
	assertPreserved()
	for _, row := range upgradeRows(t, db.Conn(), `SELECT opening_context_hash,opening_context_generation FROM call_sessions`) {
		if !reflect.DeepEqual(row, []any{"", int64(0)}) {
			t.Fatalf("opening defaults = %v", row)
		}
	}
	for _, column := range []struct{ name, kind, defaultValue string }{
		{"opening_context_hash", "TEXT", "''"},
		{"opening_context_generation", "INTEGER", "0"},
	} {
		var kind, defaultValue string
		var notNull int
		if err := db.Conn().QueryRow(`SELECT type,"notnull",dflt_value FROM pragma_table_info('call_sessions') WHERE name=?`, column.name).Scan(&kind, &notNull, &defaultValue); err != nil || kind != column.kind || notNull != 1 || defaultValue != column.defaultValue {
			t.Fatalf("column %s: type=%s notnull=%d default=%s err=%v", column.name, kind, notNull, defaultValue, err)
		}
	}
	// A second startup is a no-op, before exercising deliberate new writes.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	assertPreserved()

	sessions := callsessions.NewStore(db)
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte("result")))
	if err := sessions.CheckOpeningHash("owner", "session", 3, hash); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := sessions.FinalizeOpeningHash("owner", "session", 1, "opening", 3, 1, 5, hash, "2026-01-02"); err != nil {
			t.Fatal(err)
		}
	}
	if err := sessions.CheckOpeningHash("owner", "session", 3, hash); err != nil {
		t.Fatal(err)
	}
	wantOpening := [][]any{{hash, int64(1), int64(3), "opening", int64(1), int64(5), "2026-01-02", int64(22)}}
	openingQuery := `SELECT opening_context_hash,opening_context_generation,opening_context_version,opening_context_kind,opening_context_items,opening_context_chars,opening_context_applied_at,context_chars FROM call_sessions WHERE voice_session_id='session'`
	if got := upgradeRows(t, db.Conn(), openingQuery); !reflect.DeepEqual(got, wantOpening) {
		t.Fatalf("finalized opening = %v", got)
	}
	r := store.ResultReceipt{MediaGeneration: 1, DeliveryID: "delivery", ContentHash: hash}
	if inserted, err := db.InsertResultReceipt("owner", "session", r, "result"); err != nil || !inserted {
		t.Fatalf("inserted=%v err=%v", inserted, err)
	}
	if claimed, err := db.ClaimResultReceipt("owner", "session", r, "attempt"); err != nil || !claimed {
		t.Fatalf("claimed=%v err=%v", claimed, err)
	}
	if err := db.FinishResultReceipt("owner", "session", r, "attempt", "applied"); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`INSERT INTO voice_result_receipts VALUES('owner','session',0,'invalid','hash','text','pending','')`,
		`INSERT INTO voice_result_receipts VALUES('owner','session',1,'invalid','hash','text','invalid','')`,
		`INSERT INTO voice_result_receipts VALUES('owner','session',1,'delivery','hash','text','pending','')`,
	} {
		if _, err := db.Conn().Exec(query); err == nil {
			t.Fatal("receipt CHECK or primary key constraint missing")
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	r.State = "applied"
	if got, err := db.GetResultReceipt("owner", "session", 1, "delivery"); err != nil || got != r {
		t.Fatalf("persisted receipt=%+v err=%v", got, err)
	}
	// The public service can reconcile a submitted receipt after restart without
	// a live binding or transport replay, using the upgraded durable store.
	service := voice.New(config.Config{}, nil, nil).WithResultReceipts(db)
	request := voice.ResultDeliveryRequest{MediaGeneration: 1, DeliveryID: "delivery", ContentHash: hash, Text: "result"}
	if got, err := service.SubmitResultDelivery("owner", "session", request); err != nil || got != r {
		t.Fatalf("submit existing receipt=%+v err=%v", got, err)
	}
	if got, err := service.LookupResultDelivery("owner", "session", 1, "delivery"); err != nil || got != r {
		t.Fatalf("lookup receipt=%+v err=%v", got, err)
	}
	if got := upgradeRows(t, db.Conn(), openingQuery); !reflect.DeepEqual(got, wantOpening) {
		t.Fatalf("opening changed on reopen: %v", got)
	}
	for _, table := range []string{"accounts", "bridge_accounts"} {
		if got := upgradeRows(t, db.Conn(), queries[table]); !reflect.DeepEqual(got, before[table]) {
			t.Errorf("%s rows changed after receipt writes and reopen", table)
		}
	}
}

func upgradeRows(t *testing.T, conn *sql.DB, query string) [][]any {
	t.Helper()
	rows, err := conn.Query(query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var result [][]any
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			t.Fatal(err)
		}
		result = append(result, values)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}
