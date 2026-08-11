package store

import (
	"database/sql"
	"fmt"
)

// SaveCheckpoint upserts one operation's execution checkpoint into the
// checkpoints table (op_id PRIMARY KEY, created by schemaV1). This is the
// server-side double-write of the agent's per-batch checkpoint: the server
// persists the progress events it observes so an operation can be resumed
// (startIdx = last_statement) even if the agent or the server restarts.
//
// errorsJSON is the JSON-encoded executor error list (e.g. "[]" or a
// "[{\"statement\":..,\"err\":..}]" array); pass "" when no errors are known.
// A later call with the same opID overwrites the previous row in place.
func SaveCheckpoint(db *sql.DB, opID string, lastStmt, total int, errorsJSON string) error {
	if opID == "" {
		return fmt.Errorf("store: SaveCheckpoint: operation id required")
	}
	_, err := db.Exec(
		`INSERT INTO checkpoints (op_id, last_statement, total, errors)
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT(op_id) DO UPDATE SET
		   last_statement = excluded.last_statement,
		   total          = excluded.total,
		   errors         = excluded.errors`,
		opID, lastStmt, total, errorsJSON)
	if err != nil {
		return fmt.Errorf("store: save checkpoint %s: %w", opID, err)
	}
	return nil
}
