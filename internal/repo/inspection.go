// Package repo saves inspections and their verdicts in Postgres.
package repo

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5/pgxpool"

	"xray-go/internal/model"
)

const schema = `
CREATE TABLE IF NOT EXISTS inspections (
	inspection_id TEXT PRIMARY KEY,
	msg_key       TEXT NOT NULL,
	trace_id      TEXT NOT NULL,
	content       TEXT NOT NULL,
	verdict       TEXT,
	reason        TEXT,
	details       JSONB,
	created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
	decided_at    TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS inspections_msg_key_idx ON inspections (msg_key);
`

// Migrate creates the table if it doesn't exist.
func Migrate(ctx context.Context, db *pgxpool.Pool) error {
	_, err := db.Exec(ctx, schema)
	return err
}

// SaveRequest stores a new inspection before it is sent to the engines.
func SaveRequest(ctx context.Context, db *pgxpool.Pool, r model.InspectRequest) error {
	_, err := db.Exec(ctx,
		`INSERT INTO inspections (inspection_id, msg_key, trace_id, content, created_at)
		 VALUES ($1, $2, $3, $4, $5)`,
		r.InspectionID, r.MsgKey, r.TraceID, r.Content, r.Timestamp)
	return err
}

// SaveVerdict stores the final verdict for an inspection.
func SaveVerdict(ctx context.Context, db *pgxpool.Pool, v model.Verdict) error {
	details, err := json.Marshal(v.Details)
	if err != nil {
		return err
	}
	_, err = db.Exec(ctx,
		`UPDATE inspections SET verdict = $2, reason = $3, details = $4, decided_at = now()
		 WHERE inspection_id = $1`,
		v.InspectionID, v.Verdict, v.Reason, details)
	return err
}
