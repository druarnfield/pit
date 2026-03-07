package meta

// NOTE: Use `trigger_source` not `trigger` (SQL reserved word)
const v1Schema = `
CREATE TABLE runs (
	id         TEXT PRIMARY KEY,
	dag_name   TEXT NOT NULL,
	status     TEXT NOT NULL,
	started_at TEXT NOT NULL,
	ended_at   TEXT,
	run_dir    TEXT NOT NULL,
	trigger_source TEXT,
	error      TEXT
);
CREATE INDEX idx_runs_dag_started ON runs(dag_name, started_at DESC);
CREATE INDEX idx_runs_status ON runs(status);

CREATE TABLE task_instances (
	run_id    TEXT NOT NULL REFERENCES runs(id),
	task_name TEXT NOT NULL,
	status    TEXT NOT NULL,
	started_at TEXT,
	ended_at   TEXT,
	attempts   INTEGER DEFAULT 1,
	error      TEXT,
	log_path   TEXT,
	PRIMARY KEY (run_id, task_name)
);
CREATE INDEX idx_ti_status ON task_instances(status);

CREATE TABLE env_snapshots (
	id         INTEGER PRIMARY KEY AUTOINCREMENT,
	dag_name   TEXT NOT NULL,
	hash_type  TEXT NOT NULL,
	hash_value TEXT NOT NULL,
	first_seen TEXT NOT NULL,
	run_id     TEXT REFERENCES runs(id)
);
CREATE INDEX idx_env_dag_type ON env_snapshots(dag_name, hash_type, first_seen DESC);

CREATE TABLE outputs (
	run_id   TEXT NOT NULL REFERENCES runs(id),
	dag_name TEXT NOT NULL,
	name     TEXT NOT NULL,
	type     TEXT,
	location TEXT,
	PRIMARY KEY (run_id, name)
);
CREATE INDEX idx_outputs_dag ON outputs(dag_name);
`

var migrations = []string{
	v1Schema,
}
