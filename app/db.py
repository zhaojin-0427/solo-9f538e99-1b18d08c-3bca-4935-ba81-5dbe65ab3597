"""SQLite schema and connection helpers.

Storage layout — two summary formats
------------------------------------
The service understands two on-disk summary formats.  Reads cover both in one
snapshot; writes route themselves by the per-combo migration state.

Old / legacy format (kept for pre-existing SQLite volumes)
``````````````````````````````````````````````````````````
``tag_summaries``
    One summary record per ``(district, ear_tag)``.  ``day_counts`` is a
    canonical JSON array of ``[day, count]`` pairs sorted by day, e.g.
    ``[["2026-03-01", 2], ["2026-03-05", 1]]``.  Every save has to read and
    rewrite the whole array, so its volume grows with the tag's history.
    Old volumes boot without any backfill and keep serving from this table
    until a ``(district, ear_tag)`` combination is migrated.

New / v2 format (born-new and migrated combinations)
````````````````````````````````````````````````````
``tag_day_counts``
    One row per ``(district, ear_tag, observed_on)`` — the normalized
    day-level summary.  A save reads and rewrites only the single day row it
    touches plus the constant-size ``summary_registry`` span row, so the
    summary volume handled by one save no longer grows with history.

``summary_registry``
    One row per known combination.  For ``format = 'new'`` it carries the
    first/last day span and the number of distinct days (the v2 metadata the
    statistics cap works on).  While a legacy combination is being migrated
    its ``mig_status`` is ``'running'`` and ``cursor_day`` records the
    resumable chunk cursor; statistics still use the legacy row until the
    atomic switch sets ``format = 'new'`` and removes that legacy row.

Detail
``````
``observations``
    The editable detail: one row per *identification record*
    (``recognition_id`` is the unique external key), never one row per photo.

``app_meta``
    A monotonically increasing ``state_version``, bumped once per successful
    write transaction.  Every read returns it so callers can identify the
    committed state an answer came from.

Transaction discipline
----------------------
Every write runs in one ``BEGIN IMMEDIATE`` transaction that updates the
detail row, the affected summary record(s) and the state version together.
Any failure rolls back all of them; a failed save leaves the previous detail
and the previous numbers intact.

Every read runs in one deferred transaction.  Under WAL that is a single
stable snapshot, so all detail rows and all summary pages of one query come
from the same committed state.

The legacy tables carry a ``committed`` flag pinned to 1 by every read.
Rows of an in-flight transaction are additionally invisible to other SQLite
connections, so no query can observe a half-applied write.
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager

_DEFAULT_DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "resighting.db",
)

_db_path: str | None = None

#: Test-only SQL trace hook, attached to every new connection when set.
_trace_hook = None


def set_trace_hook(hook) -> None:
    """Install or clear a ``sqlite3`` trace callback (tests only)."""
    global _trace_hook
    _trace_hook = hook


def get_db_path() -> str:
    """Current database path.

    Resolution order: an explicit :func:`configure_db` value, then the
    ``DB_PATH`` environment variable, then ``<repo>/data/resighting.db``.
    """
    if _db_path is not None:
        return _db_path
    return os.environ.get("DB_PATH", _DEFAULT_DB_PATH)


def configure_db(path: str | None) -> None:
    """Point the service at another database file (used by tests)."""
    global _db_path
    _db_path = path


#: Maximum number of summary records fetched by one SQL statement/page.
MAX_SUMMARY_ROWS = 1024

#: Maximum number of distinct dates rewritten by one migration write tx.
MIGRATION_BATCH_DAYS = 128

SCHEMA = """
CREATE TABLE IF NOT EXISTS observations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    recognition_id  TEXT    NOT NULL UNIQUE,
    ear_tag         TEXT    NOT NULL,
    district        TEXT    NOT NULL,
    observed_on     TEXT    NOT NULL,          -- ISO date, YYYY-MM-DD
    committed       INTEGER NOT NULL DEFAULT 1 CHECK (committed IN (0, 1)),
    created_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- One summary record per (district, ear_tag).  day_counts already has one key
-- per day, with the number of identification records behind that day.
CREATE TABLE IF NOT EXISTS tag_summaries (
    district           TEXT    NOT NULL,
    ear_tag            TEXT    NOT NULL,
    committed          INTEGER NOT NULL DEFAULT 1 CHECK (committed IN (0, 1)),
    first_observed_on  TEXT    NOT NULL,
    last_observed_on   TEXT    NOT NULL,
    day_counts         TEXT    NOT NULL,       -- canonical JSON [[day, count]]
    PRIMARY KEY (district, ear_tag, committed)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS app_meta (
    key             TEXT    PRIMARY KEY,
    value           INTEGER NOT NULL
) WITHOUT ROWID;

INSERT OR IGNORE INTO app_meta (key, value) VALUES ('state_version', 0);

-- Detail-side indexes: range/filter listing and the (unique) id lookup.
CREATE INDEX IF NOT EXISTS idx_observations_scope
    ON observations (district, ear_tag, observed_on, committed);
CREATE INDEX IF NOT EXISTS idx_observations_range
    ON observations (committed, observed_on, district, ear_tag);

-- Summary-side indexes: the span columns let a range query skip summary
-- records that cannot overlap the window, in district or global order.
CREATE INDEX IF NOT EXISTS idx_summaries_district_span
    ON tag_summaries (district, committed, first_observed_on, last_observed_on, ear_tag);
CREATE INDEX IF NOT EXISTS idx_summaries_span
    ON tag_summaries (committed, first_observed_on, last_observed_on, district, ear_tag);

-- ---------------------------------------------------------------------------
-- New normalized format: one day-level summary row per combination and date.
-- A save only reads/rewrites the single row for its own date.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tag_day_counts (
    district        TEXT    NOT NULL,
    ear_tag         TEXT    NOT NULL,
    observed_on     TEXT    NOT NULL,          -- ISO date, YYYY-MM-DD
    capture_count   INTEGER NOT NULL CHECK (capture_count > 0),
    PRIMARY KEY (district, ear_tag, observed_on)
) WITHOUT ROWID;

-- Fetch every in-window day of one district (or all districts) in one stmt.
CREATE INDEX IF NOT EXISTS idx_tag_day_counts_day
    ON tag_day_counts (observed_on, district, ear_tag);

-- Per-combination format / migration state and v2 span metadata.
-- format:
--   'new'     -> tag_day_counts is authoritative; no tag_summaries row
--   'dual'    -> legacy tag_summaries is authoritative and still serves
--                statistics; tag_day_counts is online staging kept in sync
--                while migration chunks are applied
-- mig_status: NULL for born-new combinations, otherwise 'running'/'done'.
CREATE TABLE IF NOT EXISTS summary_registry (
    district            TEXT    NOT NULL,
    ear_tag             TEXT    NOT NULL,
    format              TEXT    NOT NULL DEFAULT 'new'
                            CHECK (format IN ('new', 'dual')),
    first_observed_on   TEXT,
    last_observed_on    TEXT,
    observed_days       INTEGER NOT NULL DEFAULT 0,
    mig_status          TEXT CHECK (mig_status IN ('running', 'done')),
    cursor_day          TEXT,
    total_days          INTEGER NOT NULL DEFAULT 0,
    copied_days         INTEGER NOT NULL DEFAULT 0,
    chunk_count         INTEGER NOT NULL DEFAULT 0,
    last_error          TEXT,
    started_at          TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at          TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    completed_at        TEXT,
    PRIMARY KEY (district, ear_tag)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_summary_registry_format_span
    ON summary_registry (format, first_observed_on, last_observed_on, district, ear_tag);
"""


def _connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    if _trace_hook is not None:
        conn.set_trace_callback(_trace_hook)
    return conn


def init_db(path: str | None = None) -> None:
    """Create the database file and schema if needed.  Idempotent."""
    target = path or get_db_path()
    os.makedirs(os.path.dirname(os.path.abspath(target)), exist_ok=True)
    conn = _connect(target)
    try:
        # Journal mode is persistent in the database file, so it only needs to
        # be set when the file is created.  Doing it per connection would take
        # a lock and could fail while another connection is writing.
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript(SCHEMA)
    finally:
        conn.close()


@contextmanager
def write_transaction(path: str | None = None):
    """One atomic, serialized write transaction.

    ``BEGIN IMMEDIATE`` takes the write lock up front, so summary maintenance
    cannot interleave with another writer.  Any exception rolls back the
    detail row, the summary record(s) and the state version together.
    """
    conn = _connect(path or get_db_path())
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        try:
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
    finally:
        conn.close()


@contextmanager
def read_transaction(path: str | None = None):
    """One read transaction with a single WAL snapshot for all statements."""
    conn = _connect(path or get_db_path())
    try:
        conn.execute("BEGIN")
        try:
            yield conn
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        conn.execute("COMMIT")
    finally:
        conn.close()
