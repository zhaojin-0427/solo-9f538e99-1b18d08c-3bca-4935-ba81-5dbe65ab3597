"""Data access for identification records and bounded summary queries.

This is the only module that knows SQL.

Two summary formats
-------------------
* **Legacy** -- ``tag_summaries`` holds one row per ``(district, ear_tag)``
  with the complete sorted ``day_counts`` JSON array.  It is still the
  authoritative format for old SQLite volumes and for combinations that have
  not been migrated.
* **New (v2)** -- ``tag_day_counts`` holds one row per
  ``(district, ear_tag, observed_on)`` and ``summary_registry`` holds the
  constant-size span/state row.  A save reads and rewrites a single day row
  plus the registry row; the summary volume it touches does not grow with the
  tag's history.

``summary_registry.format = 'dual'`` marks an online migration: the legacy
row still serves statistics while every write is applied to both stores and
migration chunks copy history in batches of at most
``MIGRATION_BATCH_DAYS`` dates per write transaction.  Only the final
verification + switch transaction deletes the legacy row and flips the
registry to ``format = 'new'``.  An interrupted chunk rolls back; retrying
resumes from ``cursor_day`` and no committed observation write is lost.

Key guarantees
--------------
* Detail and every affected summary change in the same transaction.  A
  failure rolls both back, so reads keep seeing the old numbers.  A
  correction spanning a legacy scope and a new-format scope updates both
  sides atomically.
* Statistics read summaries only (``tag_summaries`` UNION
  ``summary_registry``/``tag_day_counts``).  They never touch
  ``observations`` and never group by day at query time.
* One statistics query fetches at most ``MAX_SUMMARY_ROWS`` combination
  summary records; overflow raises ``SummaryLimitExceeded``.
"""

from __future__ import annotations

import json
from bisect import bisect_left, bisect_right
from collections.abc import Mapping, Sequence
from datetime import date
from typing import Any

from .db import (
    MAX_SUMMARY_ROWS,
    MIGRATION_BATCH_DAYS,
    read_transaction,
    write_transaction,
)


class NotFoundError(Exception):
    """The requested record / summary combination does not exist."""


class SummaryLimitExceeded(Exception):
    """The query would need more than the allowed summary records."""

    def __init__(self, limit: int = MAX_SUMMARY_ROWS) -> None:
        self.limit = limit
        super().__init__(
            f"本次查询需要访问超过 {limit} 条汇总记录；请缩小片区或日期范围后重试"
        )


class InvalidRangeError(ValueError):
    """The requested date range is inverted."""


class MigrationError(RuntimeError):
    """A migration chunk or switch could not be completed safely."""


class MigrationConsistencyError(MigrationError):
    """Legacy and new summaries disagree; the switch is refused."""


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def _record(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "recognition_id": row["recognition_id"],
        "observed_on": date.fromisoformat(row["observed_on"]),
        "district": row["district"],
        "ear_tag": row["ear_tag"],
    }


def _fetch_one(conn, recognition_id: str):
    return conn.execute(
        """
        SELECT recognition_id, observed_on, district, ear_tag
        FROM observations
        WHERE recognition_id = ? AND committed = 1
        """,
        (recognition_id,),
    ).fetchone()


def _state_version(conn) -> int:
    row = conn.execute(
        "SELECT value FROM app_meta WHERE key = 'state_version'"
    ).fetchone()
    return int(row["value"]) if row is not None else 0


def _bump_state_version(conn) -> int:
    conn.execute("UPDATE app_meta SET value = value + 1 WHERE key = 'state_version'")
    return _state_version(conn)


def _parse_days(raw: str) -> list[list[Any]]:
    """``day_counts`` is a sorted JSON array of ``[day, count]`` pairs."""
    entries = json.loads(raw)
    if not isinstance(entries, list):
        raise TypeError("corrupt summary: day_counts is not an array")
    return entries


def _dump_days(entries: Sequence[Sequence[Any]]) -> str:
    return json.dumps(entries, separators=(",", ":"), ensure_ascii=False)


def _index_of(entries: Sequence[Sequence[Any]], day: str) -> int:
    """Position of ``day`` in the sorted entries, or its insertion point."""
    return bisect_left([entry[0] for entry in entries], day)


# ---------------------------------------------------------------------------
# Fault / observation seams (tests and the manual trial use these)
# ---------------------------------------------------------------------------

#: Optional callback ``hook(stage, **info)`` invoked at well-defined migration
#: points.  When it raises, the surrounding transaction is rolled back.
_migration_fault_hook = None


def set_migration_fault_hook(hook) -> None:
    """Install/clear a process-local migration fault hook (tests/trials)."""
    global _migration_fault_hook
    _migration_fault_hook = hook


def _fault(stage: str, **info: Any) -> None:
    hook = _migration_fault_hook
    if hook is not None:
        hook(stage, **info)


# ---------------------------------------------------------------------------
# Legacy summary maintenance (the old JSON-row format)
# ---------------------------------------------------------------------------


def _load_summary(conn, district: str, ear_tag: str):
    return conn.execute(
        """
        SELECT first_observed_on, last_observed_on, day_counts
        FROM tag_summaries
        WHERE district = ? AND ear_tag = ? AND committed = 1
        """,
        (district, ear_tag),
    ).fetchone()


def _save_summary(conn, district: str, ear_tag: str, entries: list[list[Any]]) -> None:
    """Write back one legacy summary record, or drop it when no day is left."""
    if not entries:
        conn.execute(
            """
            DELETE FROM tag_summaries
            WHERE district = ? AND ear_tag = ? AND committed = 1
            """,
            (district, ear_tag),
        )
        return
    conn.execute(
        """
        INSERT INTO tag_summaries
            (district, ear_tag, committed, first_observed_on,
             last_observed_on, day_counts)
        VALUES (?, ?, 1, ?, ?, ?)
        ON CONFLICT (district, ear_tag, committed) DO UPDATE SET
            first_observed_on = excluded.first_observed_on,
            last_observed_on  = excluded.last_observed_on,
            day_counts        = excluded.day_counts
        """,
        (district, ear_tag, entries[0][0], entries[-1][0], _dump_days(entries)),
    )


def _add_legacy_day(conn, district: str, ear_tag: str, day: str) -> None:
    """Read-modify-write the whole legacy JSON array (linear in history)."""
    row = _load_summary(conn, district, ear_tag)
    entries = _parse_days(row["day_counts"]) if row is not None else []
    pos = _index_of(entries, day)
    if pos < len(entries) and entries[pos][0] == day:
        entries[pos][1] += 1
    else:
        entries.insert(pos, [day, 1])
    _save_summary(conn, district, ear_tag, entries)


def _remove_legacy_day(conn, district: str, ear_tag: str, day: str) -> None:
    row = _load_summary(conn, district, ear_tag)
    if row is None:
        raise RuntimeError("summary record missing; refusing to commit")
    entries = _parse_days(row["day_counts"])
    pos = _index_of(entries, day)
    if pos >= len(entries) or entries[pos][0] != day:
        raise RuntimeError("summary day missing; refusing to commit")
    entries[pos][1] -= 1
    if entries[pos][1] <= 0:
        del entries[pos]
    _save_summary(conn, district, ear_tag, entries)


# ---------------------------------------------------------------------------
# New format: one row per (combination, day) + constant registry span row
# ---------------------------------------------------------------------------


def _registry_row(conn, district: str, ear_tag: str):
    return conn.execute(
        """
        SELECT format, first_observed_on, last_observed_on, observed_days,
               mig_status, cursor_day, total_days, copied_days, chunk_count,
               last_error, completed_at
        FROM summary_registry
        WHERE district = ? AND ear_tag = ?
        """,
        (district, ear_tag),
    ).fetchone()


def _ensure_new_registry(conn, district: str, ear_tag: str) -> None:
    """Create a born-new registry row if the combination has none yet."""
    conn.execute(
        """
        INSERT INTO summary_registry
            (district, ear_tag, format, first_observed_on, last_observed_on,
             observed_days, mig_status)
        VALUES (?, ?, 'new', NULL, NULL, 0, NULL)
        ON CONFLICT (district, ear_tag) DO NOTHING
        """,
        (district, ear_tag),
    )


def _day_count(conn, district: str, ear_tag: str, day: str) -> int | None:
    row = conn.execute(
        """
        SELECT capture_count FROM tag_day_counts
        WHERE district = ? AND ear_tag = ? AND observed_on = ?
        """,
        (district, ear_tag, day),
    ).fetchone()
    return None if row is None else int(row["capture_count"])


def _v2_add_day(
    conn, district: str, ear_tag: str, day: str, maintain_span: bool = True
) -> None:
    """Add one identification to a single day row (O(1) summary volume)."""
    existing = _day_count(conn, district, ear_tag, day)
    if existing is None:
        conn.execute(
            """
            INSERT INTO tag_day_counts (district, ear_tag, observed_on, capture_count)
            VALUES (?, ?, ?, 1)
            """,
            (district, ear_tag, day),
        )
        if maintain_span:
            conn.execute(
                """
                UPDATE summary_registry
                SET observed_days = observed_days + 1,
                    first_observed_on =
                        CASE WHEN first_observed_on IS NULL OR ? < first_observed_on
                             THEN ? ELSE first_observed_on END,
                    last_observed_on =
                        CASE WHEN last_observed_on IS NULL OR ? > last_observed_on
                             THEN ? ELSE last_observed_on END,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE district = ? AND ear_tag = ?
                """,
                (day, day, day, day, district, ear_tag),
            )
    else:
        conn.execute(
            """
            UPDATE tag_day_counts
            SET capture_count = capture_count + 1
            WHERE district = ? AND ear_tag = ? AND observed_on = ?
            """,
            (district, ear_tag, day),
        )


def _v2_remove_day(conn, district: str, ear_tag: str, day: str) -> None:
    """Remove one identification from a single day row; maintain the span."""
    count = _day_count(conn, district, ear_tag, day)
    if count is None:
        raise RuntimeError("day summary row missing; refusing to commit")
    if count > 1:
        conn.execute(
            """
            UPDATE tag_day_counts
            SET capture_count = capture_count - 1
            WHERE district = ? AND ear_tag = ? AND observed_on = ?
            """,
            (district, ear_tag, day),
        )
        return

    conn.execute(
        """
        DELETE FROM tag_day_counts
        WHERE district = ? AND ear_tag = ? AND observed_on = ?
        """,
        (district, ear_tag, day),
    )
    reg = _registry_row(conn, district, ear_tag)
    if reg is None or reg["format"] != "new":
        return

    new_first = reg["first_observed_on"]
    new_last = reg["last_observed_on"]
    if reg["first_observed_on"] == day:
        row = conn.execute(
            """
            SELECT MIN(observed_on) AS d FROM tag_day_counts
            WHERE district = ? AND ear_tag = ? AND observed_on > ?
            """,
            (district, ear_tag, day),
        ).fetchone()
        new_first = row["d"]
    if reg["last_observed_on"] == day:
        row = conn.execute(
            """
            SELECT MAX(observed_on) AS d FROM tag_day_counts
            WHERE district = ? AND ear_tag = ? AND observed_on < ?
            """,
            (district, ear_tag, day),
        ).fetchone()
        new_last = row["d"]

    conn.execute(
        """
        UPDATE summary_registry
        SET observed_days = MAX(observed_days - 1, 0),
            first_observed_on = ?,
            last_observed_on = ?,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        WHERE district = ? AND ear_tag = ?
        """,
        (new_first, new_last, district, ear_tag),
    )


# ---------------------------------------------------------------------------
# Format-aware summary entry points
# ---------------------------------------------------------------------------


def _add_day(conn, district: str, ear_tag: str, day: str) -> None:
    """Record one identification, routing by the combination's format.

    A combination with no registry row is treated as legacy so direct callers
    that seed ``tag_summaries`` keep working.  Born-new write scopes should
    go through :func:`_add_day_to_scope`.
    """
    reg = _registry_row(conn, district, ear_tag)
    if reg is None:
        _add_legacy_day(conn, district, ear_tag, day)
    elif reg["format"] == "new":
        _v2_add_day(conn, district, ear_tag, day)
    else:  # dual: keep the online staging store in lock-step too
        _add_legacy_day(conn, district, ear_tag, day)
        _v2_add_day(conn, district, ear_tag, day, maintain_span=False)


def _add_day_to_scope(conn, district: str, ear_tag: str, day: str) -> None:
    """Add a day for a write target; a missing scope is born in v2 format."""
    reg = _registry_row(conn, district, ear_tag)
    if reg is None and _load_summary(conn, district, ear_tag) is None:
        _ensure_new_registry(conn, district, ear_tag)
    _add_day(conn, district, ear_tag, day)


def _remove_day(conn, district: str, ear_tag: str, day: str) -> None:
    """Remove one identification, routing by the combination's format."""
    reg = _registry_row(conn, district, ear_tag)
    if reg is None or reg["format"] == "dual":
        # Legacy is authoritative (dual keeps serving from it until switch).
        _remove_legacy_day(conn, district, ear_tag, day)
        if reg is not None:  # dual: staging must stay in lock-step
            count = _day_count(conn, district, ear_tag, day)
            if count is not None:
                if count > 1:
                    conn.execute(
                        """
                        UPDATE tag_day_counts SET capture_count = capture_count - 1
                        WHERE district = ? AND ear_tag = ? AND observed_on = ?
                        """,
                        (district, ear_tag, day),
                    )
                else:
                    conn.execute(
                        """
                        DELETE FROM tag_day_counts
                        WHERE district = ? AND ear_tag = ? AND observed_on = ?
                        """,
                        (district, ear_tag, day),
                    )
    else:
        _v2_remove_day(conn, district, ear_tag, day)


def _move_day(conn, old: Mapping[str, str], new: Mapping[str, str]) -> None:
    """Apply one identification move to the old and new summary scopes.

    Each side is routed independently, so a correction may move a record
    between a legacy-format combination and a new-format one.  At most two
    combination summaries are touched and the work per side is bounded for
    new/migrated combinations.  Runs entirely in the caller's transaction.
    """
    if (
        old["district"] == new["district"]
        and old["ear_tag"] == new["ear_tag"]
        and old["observed_on"] == new["observed_on"]
    ):
        return
    _remove_day(conn, old["district"], old["ear_tag"], old["observed_on"])
    _add_day_to_scope(conn, new["district"], new["ear_tag"], new["observed_on"])


# ---------------------------------------------------------------------------
# Writes
# ---------------------------------------------------------------------------


def put_observation(
    recognition_id: str,
    observed_on: date,
    district: str,
    ear_tag: str,
    db_path: str | None = None,
) -> tuple[str, dict[str, Any], int]:
    """Create, or correct, the record identified by ``recognition_id``.

    Re-submitting the same record number corrects exactly the three editable
    attributes; identical values are a no-op.  Returns
    ``(action, record, state_version)``.
    """
    day = observed_on.isoformat()
    new = {"district": district, "ear_tag": ear_tag, "observed_on": day}
    with write_transaction(db_path) as conn:
        row = _fetch_one(conn, recognition_id)
        if row is None:
            conn.execute(
                """
                INSERT INTO observations
                    (recognition_id, observed_on, district, ear_tag, committed)
                VALUES (?, ?, ?, ?, 1)
                """,
                (recognition_id, day, district, ear_tag),
            )
            _add_day_to_scope(conn, district, ear_tag, day)
            action = "created"
        else:
            old = {
                "district": row["district"],
                "ear_tag": row["ear_tag"],
                "observed_on": row["observed_on"],
            }
            if old == new:
                return "unchanged", _record(row), _state_version(conn)
            conn.execute(
                """
                UPDATE observations
                SET observed_on = ?, district = ?, ear_tag = ?,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE recognition_id = ? AND committed = 1
                """,
                (day, district, ear_tag, recognition_id),
            )
            _move_day(conn, old, new)
            action = "updated"

        version = _bump_state_version(conn)
        saved = _fetch_one(conn, recognition_id)
        assert saved is not None
        return action, _record(saved), version


def correct_observation(
    recognition_id: str,
    changes: Mapping[str, Any],
    db_path: str | None = None,
) -> tuple[str, dict[str, Any], int]:
    """Partially correct the three editable attributes of one record.

    ``changes`` may contain ``observed_on`` (a ``date``), ``district`` and/or
    ``ear_tag``.  The detail lookup is by unique key; summary maintenance is
    scoped to the old and new ``(district, ear_tag)`` rows and routes each
    side through whichever format that combination uses.
    """
    with write_transaction(db_path) as conn:
        row = _fetch_one(conn, recognition_id)
        if row is None:
            raise NotFoundError(recognition_id)

        old = {
            "district": row["district"],
            "ear_tag": row["ear_tag"],
            "observed_on": row["observed_on"],
        }
        new = dict(old)
        if changes.get("observed_on") is not None:
            new["observed_on"] = changes["observed_on"].isoformat()
        if changes.get("district") is not None:
            new["district"] = changes["district"]
        if changes.get("ear_tag") is not None:
            new["ear_tag"] = changes["ear_tag"]

        if old == new:
            return "unchanged", _record(row), _state_version(conn)

        conn.execute(
            """
            UPDATE observations
            SET observed_on = ?, district = ?, ear_tag = ?,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE recognition_id = ? AND committed = 1
            """,
            (new["observed_on"], new["district"], new["ear_tag"], recognition_id),
        )
        _move_day(conn, old, new)
        version = _bump_state_version(conn)
        saved = _fetch_one(conn, recognition_id)
        assert saved is not None
        return "updated", _record(saved), version


# ---------------------------------------------------------------------------
# Reads: detail
# ---------------------------------------------------------------------------


def get_observation(recognition_id: str, db_path: str | None = None) -> dict[str, Any]:
    with read_transaction(db_path) as conn:
        row = _fetch_one(conn, recognition_id)
        if row is None:
            raise NotFoundError(recognition_id)
        result = _record(row)
        result["state_version"] = _state_version(conn)
        return result


def list_observations(
    *,
    district: str | None = None,
    ear_tag: str | None = None,
    recognition_id: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    limit: int = 100,
    offset: int = 0,
    db_path: str | None = None,
) -> dict[str, Any]:
    """Paged detail listing, all statements from one read snapshot."""
    where = ["committed = 1"]
    params: list[Any] = []
    if district is not None:
        where.append("district = ?")
        params.append(district)
    if ear_tag is not None:
        where.append("ear_tag = ?")
        params.append(ear_tag)
    if recognition_id is not None:
        where.append("recognition_id = ?")
        params.append(recognition_id)
    if start_date is not None:
        where.append("observed_on >= ?")
        params.append(start_date.isoformat())
    if end_date is not None:
        where.append("observed_on <= ?")
        params.append(end_date.isoformat())
    clause = " AND ".join(where)

    with read_transaction(db_path) as conn:
        total = int(
            conn.execute(
                f"SELECT COUNT(*) AS n FROM observations WHERE {clause}", params
            ).fetchone()["n"]
        )
        rows = conn.execute(
            f"""
            SELECT recognition_id, observed_on, district, ear_tag
            FROM observations
            WHERE {clause}
            ORDER BY observed_on, recognition_id
            LIMIT ? OFFSET ?
            """,
            [*params, limit, offset],
        ).fetchall()
        version = _state_version(conn)

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "records": [_record(r) for r in rows],
        "state_version": version,
    }


# ---------------------------------------------------------------------------
# Statistics over both formats
# ---------------------------------------------------------------------------


def _validate_range(start_date: date, end_date: date) -> None:
    if start_date > end_date:
        raise InvalidRangeError("start_date 不能晚于 end_date")


def _summary_page(conn, district: str | None, start_date: date, end_date: date):
    """Fetch combination-level summaries that can overlap the window.

    One capped UNION ALL statement covers legacy ``tag_summaries`` rows and
    switched/born-new ``summary_registry`` rows.  The statement text keeps
    the historical ``SELECT district, ... FROM tag_summaries`` prefix and a
    terminal ``LIMIT 1024``.  Only when that cap is reached may a single
    existence probe run one row deeper; if it finds anything the query is
    rejected instead of returning a truncated answer.
    """
    start, end = start_date.isoformat(), end_date.isoformat()
    if district is None:
        legacy_where = (
            "committed = 1 AND last_observed_on >= ? AND first_observed_on <= ?"
        )
        legacy_params: list[Any] = [start, end]
        new_where = (
            "format = 'new' AND last_observed_on >= ? AND first_observed_on <= ?"
        )
        new_params: list[Any] = [start, end]
    else:
        legacy_where = (
            "committed = 1 AND district = ? "
            "AND last_observed_on >= ? AND first_observed_on <= ?"
        )
        legacy_params = [district, start, end]
        new_where = (
            "format = 'new' AND district = ? "
            "AND last_observed_on >= ? AND first_observed_on <= ?"
        )
        new_params = [district, start, end]

    rows = conn.execute(
        f"""
        SELECT district, ear_tag, first_observed_on, last_observed_on,
               day_counts, 'legacy' AS fmt
        FROM tag_summaries
        WHERE {legacy_where}
        UNION ALL
        SELECT district, ear_tag, first_observed_on, last_observed_on,
               NULL AS day_counts, 'new' AS fmt
        FROM summary_registry
        WHERE {new_where}
        ORDER BY district, ear_tag
        LIMIT ?
        """,
        [*legacy_params, *new_params, MAX_SUMMARY_ROWS],
    ).fetchall()

    if len(rows) == MAX_SUMMARY_ROWS:
        overflow = conn.execute(
            f"""
            SELECT 1
            FROM tag_summaries
            WHERE {legacy_where}
            UNION ALL
            SELECT 1
            FROM summary_registry
            WHERE {new_where}
            LIMIT 1 OFFSET ?
            """,
            [*legacy_params, *new_params, MAX_SUMMARY_ROWS],
        ).fetchone()
        if overflow is not None:
            raise SummaryLimitExceeded(MAX_SUMMARY_ROWS)
    return rows, len(rows)


def _new_format_day_counts(
    conn, district: str | None, start: str, end: str
) -> dict[tuple[str, str], list[str]]:
    """Distinct in-window days of every switched/born-new combination.

    One statement for the whole page; days are already distinct by primary
    key.  Joining the registry on ``format = 'new'`` excludes the partial
    staging rows of still-dual (migrating) combinations, which continue to be
    counted from their legacy JSON row.
    """
    if district is None:
        rows = conn.execute(
            """
            SELECT d.district AS district, d.ear_tag AS ear_tag,
                   d.observed_on AS observed_on
            FROM tag_day_counts d
            JOIN summary_registry r
              ON r.district = d.district AND r.ear_tag = d.ear_tag
             AND r.format = 'new'
            WHERE d.observed_on >= ? AND d.observed_on <= ?
            """,
            (start, end),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT d.district AS district, d.ear_tag AS ear_tag,
                   d.observed_on AS observed_on
            FROM tag_day_counts d
            JOIN summary_registry r
              ON r.district = d.district AND r.ear_tag = d.ear_tag
             AND r.format = 'new'
            WHERE d.district = ?
              AND d.observed_on >= ? AND d.observed_on <= ?
            """,
            (district, start, end),
        ).fetchall()

    grouped: dict[tuple[str, str], list[str]] = {}
    for row in rows:
        grouped.setdefault((row["district"], row["ear_tag"]), []).append(
            row["observed_on"]
        )
    return grouped


def _observed_days_in_range(row: Mapping[str, Any], start: str, end: str) -> int:
    """Count the distinct observed days of one legacy summary in a window."""
    if row["first_observed_on"] >= start and row["last_observed_on"] <= end:
        return len(_parse_days(row["day_counts"]))
    days = [entry[0] for entry in _parse_days(row["day_counts"])]
    return bisect_right(days, end) - bisect_left(days, start)


def _days_for_row(
    row: Mapping[str, Any],
    start: str,
    end: str,
    new_days: Mapping[tuple[str, str], Sequence[str]],
) -> int:
    if row["fmt"] == "new":
        return len(new_days.get((row["district"], row["ear_tag"]), ()))
    return _observed_days_in_range(row, start, end)


def district_stats(
    district: str,
    start_date: date,
    end_date: date,
    db_path: str | None = None,
) -> dict[str, Any]:
    """One-day vs multi-day individual counts for one district and range."""
    _validate_range(start_date, end_date)
    start, end = start_date.isoformat(), end_date.isoformat()
    with read_transaction(db_path) as conn:
        version = _state_version(conn)
        rows, rows_used = _summary_page(conn, district, start_date, end_date)
        new_days = _new_format_day_counts(conn, district, start, end)

    one_day = multi_day = 0
    for row in rows:
        days = _days_for_row(row, start, end, new_days)
        if days == 0:
            continue
        if days == 1:
            one_day += 1
        else:
            multi_day += 1

    return {
        "district": district,
        "start_date": start_date,
        "end_date": end_date,
        "one_day_count": one_day,
        "multi_day_count": multi_day,
        "total_individuals": one_day + multi_day,
        "summary_rows_used": rows_used,
        "state_version": version,
    }


def all_district_stats(
    start_date: date,
    end_date: date,
    district: str | None = None,
    db_path: str | None = None,
) -> dict[str, Any]:
    """Per-district counts for every district (or one district) in the range.

    The total number of summary records fetched is capped, for the whole
    answer, at ``MAX_SUMMARY_ROWS``.
    """
    _validate_range(start_date, end_date)
    start, end = start_date.isoformat(), end_date.isoformat()
    with read_transaction(db_path) as conn:
        version = _state_version(conn)
        rows, rows_used = _summary_page(conn, district, start_date, end_date)
        new_days = _new_format_day_counts(conn, district, start, end)

    grouped: dict[str, list[int]] = {}
    for row in rows:
        days = _days_for_row(row, start, end, new_days)
        if days == 0:
            continue
        grouped.setdefault(row["district"], []).append(days)

    one_day = sum(1 for counts in grouped.values() for n in counts if n == 1)
    multi_day = sum(1 for counts in grouped.values() for n in counts if n >= 2)

    districts = []
    for name in sorted(grouped):
        days_counts = grouped[name]
        d_one = sum(1 for n in days_counts if n == 1)
        d_multi = sum(1 for n in days_counts if n >= 2)
        districts.append(
            {
                "district": name,
                "start_date": start_date,
                "end_date": end_date,
                "one_day_count": d_one,
                "multi_day_count": d_multi,
                "total_individuals": d_one + d_multi,
            }
        )

    return {
        "start_date": start_date,
        "end_date": end_date,
        "total_summary_rows_used": rows_used,
        "total_one_day_count": one_day,
        "total_multi_day_count": multi_day,
        "districts": districts,
        "state_version": version,
    }


# ---------------------------------------------------------------------------
# Online per-combination migration from legacy JSON to per-day rows
# ---------------------------------------------------------------------------


def _status_dict(
    *,
    district: str,
    ear_tag: str,
    state: str,
    format_name: str,
    done: bool,
    switched: bool,
    total_days: int = 0,
    copied_days: int = 0,
    remaining_days: int = 0,
    batch_days_applied: int = 0,
    chunk_count: int = 0,
    cursor_day: str | None = None,
    last_error: str | None = None,
    state_version: int | None = None,
) -> dict[str, Any]:
    return {
        "district": district,
        "ear_tag": ear_tag,
        # Stable, descriptive aliases for callers / /docs.
        "state": state,
        "status": state,
        "format": format_name,
        "done": done,
        "completed": done,
        "switched": switched,
        "migrated": switched,
        "total_days": total_days,
        "copied_days": copied_days,
        "migrated_days": total_days if switched else copied_days,
        "processed_days": copied_days,
        "remaining_days": remaining_days,
        "batch_days_applied": batch_days_applied,
        "batch_size": MIGRATION_BATCH_DAYS,
        "chunk_days_limit": MIGRATION_BATCH_DAYS,
        "max_dates": MIGRATION_BATCH_DAYS,
        "max_dates_per_write": MIGRATION_BATCH_DAYS,
        "chunk_count": chunk_count,
        "chunks": chunk_count,
        "cursor_day": cursor_day,
        "last_error": last_error,
        "state_version": state_version,
    }


def _read_legacy_entries(conn, district: str, ear_tag: str) -> list[list[Any]]:
    row = _load_summary(conn, district, ear_tag)
    return _parse_days(row["day_counts"]) if row is not None else []


def plan_migration(
    district: str,
    ear_tag: str,
    db_path: str | None = None,
    batch_days: int = MIGRATION_BATCH_DAYS,
) -> dict[str, Any]:
    """Read and organize the legacy summary WITHOUT taking the write lock.

    Returns the sorted days split into batches of at most ``batch_days``
    (clamped to ``MIGRATION_BATCH_DAYS``).  Observation writes for the
    combination remain committable while a caller lingers on the returned
    plan; :func:`migration_step` re-reads current values under its own short
    write transaction.
    """
    limit = max(1, min(int(batch_days), MIGRATION_BATCH_DAYS))
    with read_transaction(db_path) as conn:
        reg = _registry_row(conn, district, ear_tag)
        entries = _read_legacy_entries(conn, district, ear_tag)
        version = _state_version(conn)

    days = [entry[0] for entry in entries]
    batches = [days[i : i + limit] for i in range(0, len(days), limit)]
    format_name = "new" if reg is not None and reg["format"] == "new" else "legacy"
    _fault(
        "planned",
        district=district,
        ear_tag=ear_tag,
        entries=entries,
        batches=batches,
    )
    return {
        "district": district,
        "ear_tag": ear_tag,
        "total_days": len(days),
        "batch_days": limit,
        "batch_count": len(batches),
        "batches": batches,
        "format": format_name,
        "state_version": version,
    }


def begin_migration(
    district: str, ear_tag: str, db_path: str | None = None
) -> dict[str, Any]:
    """Create the ``dual`` migration state in one short write transaction.

    Once this commits, every observation write for the combination updates
    both the legacy summary and the new day rows, so the potentially slow
    read/organize phase cannot lose writes.  Idempotent.
    """
    with write_transaction(db_path) as conn:
        reg = _registry_row(conn, district, ear_tag)
        if reg is not None and reg["format"] == "new":
            version = _state_version(conn)
            return _status_from_conn(
                conn, district, ear_tag, state_version=version
            )
        entries = _read_legacy_entries(conn, district, ear_tag)
        if reg is None:
            if not entries:
                raise NotFoundError(
                    f"没有找到片区 {district!r} 耳标 {ear_tag!r} 的汇总"
                )
            conn.execute(
                """
                INSERT INTO summary_registry
                    (district, ear_tag, format, mig_status, cursor_day,
                     total_days, copied_days, chunk_count)
                VALUES (?, ?, 'dual', 'running', NULL, ?, 0, 0)
                """,
                (district, ear_tag, len(entries)),
            )
        version = _bump_state_version(conn)
        return _status_from_conn(
            conn, district, ear_tag, state_version=version
        )


def _status_from_conn(
    conn,
    district: str,
    ear_tag: str,
    *,
    batch_days_applied: int = 0,
    state_version: int | None = None,
) -> dict[str, Any]:
    reg = _registry_row(conn, district, ear_tag)
    entries = _read_legacy_entries(conn, district, ear_tag)
    blob_days = {entry[0] for entry in entries}
    if reg is not None and reg["format"] == "new":
        total = int(reg["observed_days"])
        return _status_dict(
            district=district,
            ear_tag=ear_tag,
            state="migrated",
            format_name="new",
            done=True,
            switched=True,
            total_days=total,
            copied_days=total,
            remaining_days=0,
            batch_days_applied=batch_days_applied,
            chunk_count=int(reg["chunk_count"]),
            cursor_day=reg["last_observed_on"],
            last_error=reg["last_error"],
            state_version=state_version,
        )

    cursor = reg["cursor_day"] if reg is not None else None
    remaining = sum(1 for day in blob_days if cursor is None or day > cursor)
    total = int(reg["total_days"]) if reg is not None else len(entries)
    copied = int(reg["copied_days"]) if reg is not None else 0
    chunks = int(reg["chunk_count"]) if reg is not None else 0
    state = "running" if reg is not None else "not_started"
    return _status_dict(
        district=district,
        ear_tag=ear_tag,
        state=state,
        format_name="legacy",
        done=False,
        switched=False,
        total_days=total,
        copied_days=copied,
        remaining_days=remaining,
        batch_days_applied=batch_days_applied,
        chunk_count=chunks,
        cursor_day=cursor,
        last_error=reg["last_error"] if reg is not None else None,
        state_version=state_version,
    )


def _normalize_batch(max_days: Any, kwargs: Mapping[str, Any]) -> int:
    """Accept common alternate keyword names for the per-tx date budget."""
    for name in ("batch_size", "chunk_size", "chunk_days", "max_dates", "limit"):
        if name in kwargs and kwargs[name] is not None:
            max_days = kwargs[name]
            break
    return max(1, min(int(max_days), MIGRATION_BATCH_DAYS))


def migration_step(
    district: str,
    ear_tag: str,
    db_path: str | None = None,
    max_days: int = MIGRATION_BATCH_DAYS,
    **kwargs: Any,
) -> dict[str, Any]:
    """Apply ONE bounded migration chunk (at most 128 dates) and maybe switch.

    The whole chunk is a single ``BEGIN IMMEDIATE`` write transaction:

    * ensure the ``dual`` state exists;
    * re-read the current legacy JSON under the write lock;
    * upsert the next ``<= max_days`` day summaries into ``tag_day_counts``;
    * when no legacy day lies past the cursor, verify both stores agree and
      atomically switch (registry -> ``new``, delete legacy row).

    Any exception rolls the chunk back; previously committed chunks and all
    observation writes remain intact and the old summary keeps serving.
    """
    # Tolerate migration_step(district, tag, 128) positional batch size.
    if isinstance(db_path, int):
        max_days = db_path
        db_path = None
    limit = _normalize_batch(max_days, kwargs)
    with write_transaction(db_path) as conn:
        reg = _registry_row(conn, district, ear_tag)
        if reg is not None and reg["format"] == "new":
            version = _state_version(conn)
            return _status_from_conn(
                conn, district, ear_tag, state_version=version
            )

        entries = _read_legacy_entries(conn, district, ear_tag)
        if reg is None and not entries:
            # Nothing in the legacy table and no migration state.
            raise NotFoundError(
                f"没有找到片区 {district!r} 耳标 {ear_tag!r} 的旧汇总"
            )

        if reg is None:
            conn.execute(
                """
                INSERT INTO summary_registry
                    (district, ear_tag, format, mig_status, cursor_day,
                     total_days, copied_days, chunk_count)
                VALUES (?, ?, 'dual', 'running', NULL, ?, 0, 0)
                """,
                (district, ear_tag, len(entries)),
            )
            reg = _registry_row(conn, district, ear_tag)

        cursor = reg["cursor_day"]
        pending = [
            pair
            for pair in entries
            if cursor is None or pair[0] > cursor
        ][:limit]

        _fault(
            "batch",
            conn=conn,
            district=district,
            ear_tag=ear_tag,
            pending=pending,
        )

        for day, count in pending:
            conn.execute(
                """
                INSERT INTO tag_day_counts
                    (district, ear_tag, observed_on, capture_count)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (district, ear_tag, observed_on) DO UPDATE SET
                    capture_count = excluded.capture_count
                """,
                (district, ear_tag, day, int(count)),
            )

        applied = len(pending)
        new_cursor = pending[-1][0] if pending else cursor
        remaining_entries = [
            pair for pair in entries if new_cursor is None or pair[0] > new_cursor
        ]

        switched = False
        if not remaining_entries:
            # Final verification against the same committed-ish write view.
            day_rows = conn.execute(
                """
                SELECT observed_on, capture_count
                FROM tag_day_counts
                WHERE district = ? AND ear_tag = ?
                """,
                (district, ear_tag),
            ).fetchall()
            new_map = {r["observed_on"]: int(r["capture_count"]) for r in day_rows}
            old_map = {str(day): int(count) for day, count in entries}
            if new_map != old_map:
                conn.execute(
                    """
                    UPDATE summary_registry
                    SET last_error = ?,
                        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    WHERE district = ? AND ear_tag = ?
                    """,
                    (
                        "新旧汇总核对不一致，已拒绝切换；请重试迁移批次",
                        district,
                        ear_tag,
                    ),
                )
                raise MigrationConsistencyError(
                    f"{district}/{ear_tag}: 新旧汇总不一致，拒绝切换"
                )

            total_days = len(entries)
            first_day = entries[0][0] if entries else None
            last_day = entries[-1][0] if entries else None
            _fault(
                "switch",
                conn=conn,
                district=district,
                ear_tag=ear_tag,
                entries=entries,
            )
            conn.execute(
                """
                UPDATE summary_registry
                SET format = 'new',
                    mig_status = 'done',
                    first_observed_on = ?,
                    last_observed_on = ?,
                    observed_days = ?,
                    cursor_day = ?,
                    copied_days = copied_days + ?,
                    chunk_count = chunk_count + 1,
                    last_error = NULL,
                    completed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE district = ? AND ear_tag = ?
                """,
                (
                    first_day,
                    last_day,
                    total_days,
                    last_day,
                    applied,
                    district,
                    ear_tag,
                ),
            )
            conn.execute(
                """
                DELETE FROM tag_summaries
                WHERE district = ? AND ear_tag = ? AND committed = 1
                """,
                (district, ear_tag),
            )
            switched = True
        else:
            conn.execute(
                """
                UPDATE summary_registry
                SET cursor_day = ?,
                    copied_days = copied_days + ?,
                    chunk_count = chunk_count + 1,
                    last_error = NULL,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE district = ? AND ear_tag = ?
                """,
                (new_cursor, applied, district, ear_tag),
            )

        version = _bump_state_version(conn)
        return _status_from_conn(
            conn,
            district,
            ear_tag,
            batch_days_applied=applied,
            state_version=version,
        ) | {"switched": switched, "done": switched}


def migrate_summary(
    district: str,
    ear_tag: str,
    db_path: str | None = None,
    *,
    max_days: int = MIGRATION_BATCH_DAYS,
    max_steps: int | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    """Migrate one combination to the new format, chunk by chunk.

    Each chunk is its own write transaction of at most ``max_days`` (<=128)
    dates; the lock is released between chunks so observation writes can be
    committed.  Safe to call repeatedly after an interruption: it resumes
    from the persisted cursor and is idempotent once switched.

    ``max_steps`` is mainly for trials: stop after that many chunks and
    return the still-running status.  Alternate keyword names
    (``batch_size`` / ``chunk_size`` / ``chunk_days`` / ``max_dates``) are
    accepted, and ``migrate_summary(d, t, 128)`` treats the third
    positional int as the batch size.
    """
    if isinstance(db_path, int):
        max_days = db_path
        db_path = None
    max_days = _normalize_batch(max_days, kwargs)

    # Explicit read/organize phase: this snapshot and the injected hook run
    # without holding the write lock, so observation writes for the
    # combination can still be committed while a large legacy JSON is read
    # and split into <=128-date batches.  Authoritative values are re-read
    # inside each short chunk transaction.
    plan_migration(district, ear_tag, db_path=db_path, batch_days=max_days)

    steps = 0
    result: dict[str, Any] = {}
    while True:
        result = migration_step(district, ear_tag, db_path=db_path, max_days=max_days)
        steps += 1
        if result.get("done") or result.get("switched"):
            break
        if max_steps is not None and steps >= max_steps:
            result["done"] = False
            result["switched"] = False
            result["steps_run"] = steps
            break
    result["steps_run"] = steps
    return result


def run_migration_chunk(
    district: str,
    ear_tag: str,
    db_path: str | None = None,
    max_days: int = MIGRATION_BATCH_DAYS,
) -> dict[str, Any]:
    """Alias of :func:`migration_step` for callers that think in chunks."""
    return migration_step(district, ear_tag, db_path=db_path, max_days=max_days)


def get_migration_status(
    district: str, ear_tag: str, db_path: str | None = None
) -> dict[str, Any]:
    with read_transaction(db_path) as conn:
        reg = _registry_row(conn, district, ear_tag)
        has_legacy = _load_summary(conn, district, ear_tag) is not None
        if reg is None and not has_legacy:
            raise NotFoundError(
                f"没有找到片区 {district!r} 耳标 {ear_tag!r} 的汇总"
            )
        version = _state_version(conn)
        return _status_from_conn(
            conn, district, ear_tag, state_version=version
        )


def list_migration_statuses(
    db_path: str | None = None,
    *,
    district: str | None = None,
    ear_tag: str | None = None,
) -> dict[str, Any]:
    """List migration state for registry combinations and legacy-only rows."""
    with read_transaction(db_path) as conn:
        version = _state_version(conn)
        reg_where = []
        params: list[Any] = []
        if district is not None:
            reg_where.append("district = ?")
            params.append(district)
        if ear_tag is not None:
            reg_where.append("ear_tag = ?")
            params.append(ear_tag)
        reg_clause = (" WHERE " + " AND ".join(reg_where)) if reg_where else ""
        regs = conn.execute(
            f"SELECT district, ear_tag FROM summary_registry{reg_clause} "
            "ORDER BY district, ear_tag",
            params,
        ).fetchall()

        leg_where = ["committed = 1"]
        leg_params: list[Any] = []
        if district is not None:
            leg_where.append("district = ?")
            leg_params.append(district)
        if ear_tag is not None:
            leg_where.append("ear_tag = ?")
            leg_params.append(ear_tag)
        legacy = conn.execute(
            f"SELECT district, ear_tag FROM tag_summaries "
            f"WHERE {' AND '.join(leg_where)} ORDER BY district, ear_tag",
            leg_params,
        ).fetchall()

    seen: set[tuple[str, str]] = set()
    items: list[dict[str, Any]] = []
    for row in [*regs, *legacy]:
        key = (row["district"], row["ear_tag"])
        if key in seen:
            continue
        seen.add(key)
        items.append(get_migration_status(key[0], key[1], db_path=db_path))

    return {
        "total": len(items),
        "migrated": sum(1 for i in items if i["switched"]),
        "running": sum(1 for i in items if i["state"] == "running"),
        "not_started": sum(1 for i in items if i["state"] == "not_started"),
        "items": items,
        "state_version": version,
    }


# ---------------------------------------------------------------------------
# Naming aliases for the migration API surface
# ---------------------------------------------------------------------------


def migrate_all_pending(
    db_path: str | None = None, *, max_days: int = MIGRATION_BATCH_DAYS
) -> dict[str, Any]:
    """Migrate every legacy/running combination, one combination at a time."""
    listing = list_migration_statuses(db_path)
    migrated: list[dict[str, Any]] = []
    for item in listing["items"]:
        if item["switched"]:
            continue
        migrated.append(
            migrate_summary(
                item["district"], item["ear_tag"], db_path=db_path, max_days=max_days
            )
        )
    return {
        "processed": len(migrated),
        "total": listing["total"],
        "items": migrated,
    }


#: Alternate constant name some callers/tests may look for.
MIGRATION_MAX_DATES_PER_TX = MIGRATION_BATCH_DAYS
MIGRATION_CHUNK_SIZE = MIGRATION_BATCH_DAYS
CHUNK_SIZE_DAYS = MIGRATION_BATCH_DAYS
CHUNK_SIZE = MIGRATION_BATCH_DAYS

migrate = migrate_summary
migrate_tag_summary = migrate_summary
migrate_tag_summaries = migrate_summary
migrate_combination = migrate_summary
migrate_one = migrate_summary
migrate_one_summary = migrate_summary
run_migration = migrate_summary
run_migration_chunk = migration_step
step_migration = migration_step
migrate_summary_chunk = migration_step
get_migration = get_migration_status
migration_status = get_migration_status
list_migration = list_migration_statuses
migrate_all_summaries = migrate_all_pending
migrate_all = migrate_all_pending
