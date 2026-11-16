package store

import (
	"database/sql"
	"errors"
)

var ErrResultNotFound = errors.New("result receipt not found")
var ErrResultConflict = errors.New("result delivery content conflict")

type ResultReceipt struct {
	MediaGeneration int64  `json:"media_generation"`
	DeliveryID      string `json:"delivery_id"`
	ContentHash     string `json:"content_hash"`
	State           string `json:"state"`
}

// RecoverResultReceipts applies once at process startup, before serving requests.
// Neither an abandoned insert nor an abandoned write claim authorizes replay.
func (db *DB) RecoverResultReceipts() error {
	_, err := db.conn.Exec(`UPDATE voice_result_receipts SET state='unknown' WHERE state IN ('pending','inflight')`)
	return err
}

func (db *DB) GetResultReceipt(owner, session string, generation int64, id string) (ResultReceipt, error) {
	var r ResultReceipt
	err := db.conn.QueryRow(`SELECT media_generation,delivery_id,content_hash,state FROM voice_result_receipts WHERE owner=? AND voice_session_id=? AND media_generation=? AND delivery_id=?`, owner, session, generation, id).Scan(&r.MediaGeneration, &r.DeliveryID, &r.ContentHash, &r.State)
	if errors.Is(err, sql.ErrNoRows) {
		err = ErrResultNotFound
	}
	if r.State == "inflight" {
		r.State = "unknown"
	}
	return r, err
}

// InsertResultReceipt never changes an existing immutable payload.
func (db *DB) InsertResultReceipt(owner, session string, r ResultReceipt, text string) (bool, error) {
	db.Lock()
	defer db.Unlock()
	result, err := db.conn.Exec(`INSERT OR IGNORE INTO voice_result_receipts(owner,voice_session_id,media_generation,delivery_id,content_hash,text,state) VALUES(?,?,?,?,?,?,'pending')`, owner, session, r.MediaGeneration, r.DeliveryID, r.ContentHash, text)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	var hash, body string
	err = db.conn.QueryRow(`SELECT content_hash,text FROM voice_result_receipts WHERE owner=? AND voice_session_id=? AND media_generation=? AND delivery_id=?`, owner, session, r.MediaGeneration, r.DeliveryID).Scan(&hash, &body)
	if err == nil && (hash != r.ContentHash || body != text) {
		err = ErrResultConflict
	}
	return n == 1, err
}

func (db *DB) ClaimResultReceipt(owner, session string, r ResultReceipt, attempt string) (bool, error) {
	res, err := db.conn.Exec(`UPDATE voice_result_receipts SET state='inflight',attempt_id=? WHERE owner=? AND voice_session_id=? AND media_generation=? AND delivery_id=? AND content_hash=? AND state='pending' AND attempt_id=''`, attempt, owner, session, r.MediaGeneration, r.DeliveryID, r.ContentHash)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

func (db *DB) FinishResultReceipt(owner, session string, r ResultReceipt, attempt, state string) error {
	res, err := db.conn.Exec(`UPDATE voice_result_receipts SET state=? WHERE owner=? AND voice_session_id=? AND media_generation=? AND delivery_id=? AND content_hash=? AND attempt_id=? AND state='inflight'`, state, owner, session, r.MediaGeneration, r.DeliveryID, r.ContentHash, attempt)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err == nil && n != 1 {
		return errors.New("result attempt fenced")
	}
	return err
}
