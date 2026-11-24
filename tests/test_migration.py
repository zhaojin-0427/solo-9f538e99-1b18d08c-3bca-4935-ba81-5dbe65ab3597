"""Tests for bounded save I/O and online per-combination summary migration.

These cover the second iteration of the service:

* born-new combinations use normalized per-day rows, so one save reads and
  rewrites a constant amount of summary data regardless of history length;
* old SQLite volumes keep serving their legacy JSON summaries and boot
  without any backfill;
* each migration write transaction rewrites at most 128 dates;
* migration is online: writes during migration / after an interruption are
  retained, the legacy summary keeps serving until the verified switch;
* a correction may straddle a legacy and a new-format scope atomically;
* statistics cover both formats from one read snapshot, keep their fields
  and the 1024 combination-summary cap.
"""

from __future__ import annotations

import json
from datetime import date, timedelta

import pytest

from app import db, repository

START = date(2025, 1, 1)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class SqlRecorder:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def __call__(self, statement: str) -> None:
        self.statements.append(" ".join(statement.split()))

    def reset(self) -> None:
        self.statements.clear()


@pytest.fixture()
def sql(db_path):
    recorder = SqlRecorder()
    db.set_trace_hook(recorder)
    yield recorder
    db.set_trace_hook(None)


def seed_legacy(conn, district: str, tag: str, days: int) -> None:
    """Seed a genuine legacy JSON summary (no registry row)."""
    for i in range(days):
        repository._add_day(
            conn, district, tag, (START + timedelta(days=i)).isoformat()
        )


def brute_force(conn, start: date, end: date):
    rows = conn.execute(
        """
        SELECT district, ear_tag, observed_on
        FROM observations
        WHERE committed = 1 AND observed_on >= ? AND observed_on <= ?
        """,
        (start.isoformat(), end.isoformat()),
    ).fetchall()
    days: dict[tuple[str, str], set[str]] = {}
    for row in rows:
        days.setdefault((row["district"], row["ear_tag"]), set()).add(
            row["observed_on"]
        )
    result: dict[str, dict[str, int]] = {}
    for (district, _tag), observed in days.items():
        bucket = result.setdefault(district, {"one": 0, "multi": 0})
        bucket["one" if len(observed) == 1 else "multi"] += 1
    return result


# ---------------------------------------------------------------------------
# Bounded save volume for born-new combinations
# ---------------------------------------------------------------------------


def test_born_new_combo_uses_normalized_day_rows(client):
    for i in range(4):
        response = client.post(
            "/observations",
            json={
                "recognition_id": f"R{i}",
                "observed_on": "2026-03-01",
                "district": "北沟片区",
                "ear_tag": "ET-1",
            },
        )
        assert response.status_code == 200

    with db.read_transaction() as conn:
        rows = conn.execute(
            """
            SELECT observed_on, capture_count
            FROM tag_day_counts
            WHERE district = '北沟片区' AND ear_tag = 'ET-1'
            """
        ).fetchall()
        registry = conn.execute(
            "SELECT format, observed_days FROM summary_registry "
            "WHERE district = '北沟片区' AND ear_tag = 'ET-1'"
        ).fetchone()
        legacy = conn.execute(
            "SELECT day_counts FROM tag_summaries "
            "WHERE district = '北沟片区' AND ear_tag = 'ET-1'"
        ).fetchone()

    assert [(r["observed_on"], r["capture_count"]) for r in rows] == [
        ("2026-03-01", 4)
    ]
    assert registry["format"] == "new"
    assert registry["observed_days"] == 1
    # Born-new truth is the day-row table, not the legacy JSON blob.
    assert legacy is None


def test_save_summary_volume_does_not_grow_with_history(client, sql):
    def measure(days: int) -> tuple[int, int, int]:
        tag = f"ET-{days:05d}"
        for i in range(days):
            client.post(
                "/observations",
                json={
                    "recognition_id": f"H-{days}-{i}",
                    "observed_on": (date(2020, 1, 1) + timedelta(days=i)).isoformat(),
                    "district": "D",
                    "ear_tag": tag,
                },
            )
        client.post(
            "/observations",
            json={
                "recognition_id": f"T-{days}",
                "observed_on": "2026-03-01",
                "district": "D",
                "ear_tag": tag,
            },
        )
        sql.reset()
        response = client.patch(
            f"/observations/T-{days}", json={"observed_on": "2026-03-09"}
        )
        assert response.status_code == 200
        total_bytes = sum(len(statement) for statement in sql.statements)
        biggest = max(len(statement) for statement in sql.statements)
        # Historical 2020 dates must not be serialized into the save trace;
        # at most the two span endpoints may appear.
        history_hits = sum(statement.count("2020-") for statement in sql.statements)
        return total_bytes, biggest, history_hits

    small_bytes, small_big, small_hits = measure(20)
    large_bytes, large_big, large_hits = measure(800)

    assert len(sql.statements) > 0
    # Total traced SQL payload is effectively constant (tag names padded).
    assert large_bytes < small_bytes + 800
    # No single statement carries a whole growing JSON array.
    assert large_big < 1200
    assert large_hits <= 4
    assert small_hits <= 4


# ---------------------------------------------------------------------------
# Old volume boots without backfill
# ---------------------------------------------------------------------------


def test_legacy_volume_boots_and_serves_without_migration(tmp_path):
    old_db = str(tmp_path / "legacy-volume.db")
    import sqlite3

    conn = sqlite3.connect(old_db)
    conn.executescript(
        """
        CREATE TABLE observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recognition_id TEXT NOT NULL UNIQUE,
            ear_tag TEXT NOT NULL, district TEXT NOT NULL,
            observed_on TEXT NOT NULL, committed INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT 'x', updated_at TEXT NOT NULL DEFAULT 'x'
        );
        CREATE TABLE tag_summaries (
            district TEXT NOT NULL, ear_tag TEXT NOT NULL,
            committed INTEGER NOT NULL DEFAULT 1,
            first_observed_on TEXT NOT NULL, last_observed_on TEXT NOT NULL,
            day_counts TEXT NOT NULL,
            PRIMARY KEY (district, ear_tag, committed)
        ) WITHOUT ROWID;
        CREATE TABLE app_meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL)
            WITHOUT ROWID;
        INSERT INTO app_meta VALUES ('state_version', 3);
        """
    )
    days = [["2026-03-01", 2], ["2026-03-02", 1], ["2026-03-09", 1]]
    conn.execute(
        "INSERT INTO tag_summaries VALUES (?, ?, 1, ?, ?, ?)",
        ("北沟片区", "ET-7", days[0][0], days[-1][0], json.dumps(days)),
    )
    conn.commit()
    conn.close()

    # Additive boot must not require a backfill.
    db.init_db(old_db)

    status = repository.get_migration_status("北沟片区", "ET-7", db_path=old_db)
    assert status["state"] == "not_started"
    assert status["total_days"] == 3

    stats = repository.district_stats(
        "北沟片区", date(2026, 3, 1), date(2026, 3, 31), db_path=old_db
    )
    # Three distinct days -> multi-day individual.
    assert stats["multi_day_count"] == 1
    assert stats["one_day_count"] == 0
    assert stats["summary_rows_used"] == 1
    assert stats["state_version"] == 3


# ---------------------------------------------------------------------------
# Chunked migration
# ---------------------------------------------------------------------------


def test_migration_chunks_rewrite_at_most_128_dates(db_path):
    with db.write_transaction() as conn:
        seed_legacy(conn, "D", "T", 300)

    first = repository.migration_step("D", "T")
    assert first["batch_days_applied"] == 128
    assert first["state"] == "running"

    with db.read_transaction() as conn:
        copied = conn.execute(
            "SELECT COUNT(*) AS n FROM tag_day_counts WHERE district='D' AND ear_tag='T'"
        ).fetchone()["n"]
    assert copied == 128

    second = repository.migration_step("D", "T", max_days=128)
    assert second["batch_days_applied"] == 128
    third = repository.migration_step("D", "T")
    assert third["batch_days_applied"] == 44
    assert third["done"] is True
    assert third["switched"] is True

    with db.read_transaction() as conn:
        assert conn.execute(
            "SELECT 1 FROM tag_summaries WHERE district='D' AND ear_tag='T'"
        ).fetchone() is None
        reg = conn.execute(
            "SELECT format, observed_days FROM summary_registry "
            "WHERE district='D' AND ear_tag='T'"
        ).fetchone()
    assert reg["format"] == "new"
    assert reg["observed_days"] == 300


@pytest.mark.parametrize("chunk", [1, 37, 128])
def test_migration_with_small_chunks_matches_brute_force(db_path, chunk):
    with db.write_transaction() as conn:
        seed_legacy(conn, "D", "T", 260)
        # And matching detail rows so brute force has the same truth.
        for i in range(260):
            day = (START + timedelta(days=i)).isoformat()
            conn.execute(
                "INSERT INTO observations (recognition_id, observed_on, district, ear_tag)"
                " VALUES (?, ?, 'D', 'T')",
                (f"R{i}", day),
            )

    result = repository.migrate_summary("D", "T", max_days=chunk)
    assert result["switched"] is True

    with db.read_transaction() as conn:
        expected = brute_force(conn, START, START + timedelta(days=400))
    stats = repository.district_stats("D", START, START + timedelta(days=400))
    want = expected["D"]
    assert stats["one_day_count"] == want["one"]
    assert stats["multi_day_count"] == want["multi"]


def test_interrupted_chunk_rolls_back_and_retry_completes(db_path, monkeypatch):
    with db.write_transaction() as conn:
        seed_legacy(conn, "D", "T", 300)

    calls = {"n": 0}

    def flaky(stage, **info):
        if stage == "batch":
            calls["n"] += 1
            if calls["n"] == 2:
                raise RuntimeError("power cut")

    monkeypatch.setattr(repository, "_migration_fault_hook", flaky)
    with pytest.raises(RuntimeError):
        repository.migrate_summary("D", "T")
    monkeypatch.setattr(repository, "_migration_fault_hook", None)

    status = repository.get_migration_status("D", "T")
    assert status["state"] == "running"
    assert status["chunk_count"] == 1
    assert status["remaining_days"] == 172

    # Old summary still serves the full history.
    stats = repository.district_stats("D", START, START + timedelta(days=400))
    assert stats["summary_rows_used"] == 1
    assert stats["multi_day_count"] == 1

    # A write committed during the broken window must survive.
    repository.put_observation("LATE", date(2026, 2, 2), "D", "T")

    result = repository.migrate_summary("D", "T")
    assert result["switched"] is True

    with db.read_transaction() as conn:
        row = conn.execute(
            "SELECT observed_days FROM summary_registry WHERE district='D' AND ear_tag='T'"
        ).fetchone()
    assert row["observed_days"] == 301
    got = repository.get_observation("LATE")
    assert got["observed_on"] == date(2026, 2, 2)


def test_switch_failure_keeps_legacy_serving(db_path, monkeypatch):
    with db.write_transaction() as conn:
        seed_legacy(conn, "D", "T", 10)

    def boom(stage, **info):
        if stage == "switch":
            raise RuntimeError("switch denied")

    monkeypatch.setattr(repository, "_migration_fault_hook", boom)
    with pytest.raises(RuntimeError):
        repository.migrate_summary("D", "T")
    monkeypatch.setattr(repository, "_migration_fault_hook", None)

    # With <=128 days, begin+copy+switch share one tx, so a switch crash may
    # roll the state all the way back to "not_started"; either way the old
    # summary is what still serves and a retry can complete.
    status = repository.get_migration_status("D", "T")
    assert status["state"] in {"running", "not_started"}
    stats = repository.district_stats("D", START, START + timedelta(days=400))
    assert stats["multi_day_count"] == 1

    assert repository.migrate_summary("D", "T")["switched"] is True


# ---------------------------------------------------------------------------
# Writes interleaved with migration never lose observations
# ---------------------------------------------------------------------------


def test_corrections_during_migration_are_retained(db_path):
    with db.write_transaction() as conn:
        seed_legacy(conn, "D", "T", 200)
        for i in range(200):
            day = (START + timedelta(days=i)).isoformat()
            conn.execute(
                "INSERT INTO observations (recognition_id, observed_on, district, ear_tag)"
                " VALUES (?, ?, 'D', 'T')",
                (f"R{i}", day),
            )

    repository.migration_step("D", "T")  # 128 copied, dual state committed

    # Correct an already-copied day, an un-copied day, and add a new late day.
    repository.correct_observation("R10", {"observed_on": date(2026, 1, 1)})
    repository.correct_observation("R180", {"observed_on": date(2026, 1, 2)})
    repository.put_observation("NEW", date(2026, 1, 3), "D", "T")

    repository.migrate_summary("D", "T")

    with db.read_transaction() as conn:
        expected = brute_force(conn, START, date(2026, 12, 31))
    got = repository.district_stats("D", START, date(2026, 12, 31))
    want = expected["D"]
    assert got["one_day_count"] == want["one"]
    assert got["multi_day_count"] == want["multi"]


# ---------------------------------------------------------------------------
# Mixed-format correction
# ---------------------------------------------------------------------------


def test_correction_between_legacy_and_new_scopes(db_path):
    # Legacy source combination.
    with db.write_transaction() as conn:
        repository._add_day(conn, "L", "TAG-L", "2026-03-03")
    repository.put_observation("MOVE", date(2026, 3, 3), "L", "TAG-L")

    # New-format target combination with history.
    for i in range(3):
        repository.put_observation(f"N{i}", date(2026, 3, 1 + i), "N", "TAG-N")

    repository.correct_observation(
        "MOVE", {"district": "N", "ear_tag": "TAG-N", "observed_on": date(2026, 3, 2)}
    )

    legacy = repository.district_stats("L", date(2026, 3, 1), date(2026, 3, 31))
    new = repository.district_stats("N", date(2026, 3, 1), date(2026, 3, 31))
    # The seeded legacy capture remains after MOVE is corrected away.
    assert legacy["total_individuals"] == 1
    assert legacy["one_day_count"] == 1
    assert new["multi_day_count"] == 1  # 03-01 and 03-02 (count 2) and 03-03

    with db.read_transaction() as conn:
        target_day = conn.execute(
            "SELECT capture_count FROM tag_day_counts "
            "WHERE district='N' AND ear_tag='TAG-N' AND observed_on='2026-03-02'"
        ).fetchone()["capture_count"]
        source_blob = conn.execute(
            "SELECT day_counts FROM tag_summaries WHERE district='L' AND ear_tag='TAG-L'"
        ).fetchone()["day_counts"]
    assert target_day == 2
    # Two captures on 03-03 before the move; the seeded one stays behind.
    assert json.loads(source_blob) == [["2026-03-03", 1]]


def test_failed_mixed_format_correction_rolls_back_everything(client, monkeypatch):
    client.post(
        "/observations",
        json={
            "recognition_id": "R1",
            "observed_on": "2026-03-01",
            "district": "北沟片区",
            "ear_tag": "ET-1",
        },
    )
    before = client.get(
        "/stats/districts/北沟片区",
        params={"start_date": "2026-03-01", "end_date": "2026-03-31"},
    ).json()

    def boom(conn):
        raise RuntimeError("simulated failure before commit")

    monkeypatch.setattr(repository, "_bump_state_version", boom)
    with pytest.raises(RuntimeError):
        client.patch(
            "/observations/R1",
            json={"district": "南坡片区", "ear_tag": "ET-9", "observed_on": "2026-03-20"},
        )
    monkeypatch.undo()

    record = client.get("/observations/R1").json()
    assert record["district"] == "北沟片区"
    assert record["ear_tag"] == "ET-1"
    assert record["observed_on"] == "2026-03-01"
    after = client.get(
        "/stats/districts/北沟片区",
        params={"start_date": "2026-03-01", "end_date": "2026-03-31"},
    ).json()
    assert after["one_day_count"] == before["one_day_count"]
    assert after["multi_day_count"] == before["multi_day_count"]
    assert after["state_version"] == before["state_version"]
    # The aborted born-new target must not have left a day row behind.
    with db.read_transaction() as conn:
        row = conn.execute(
            "SELECT 1 FROM tag_day_counts WHERE district='南坡片区' AND ear_tag='ET-9'"
        ).fetchone()
    assert row is None


# ---------------------------------------------------------------------------
# Stats across both formats and the 1024 cap
# ---------------------------------------------------------------------------


def test_stats_count_both_formats_together(db_path):
    with db.write_transaction() as conn:
        for i in range(3):
            repository._add_day(conn, "LEG", f"L{i}", "2026-03-01")
        for i in range(4):
            repository._ensure_new_registry(conn, "NEW", f"N{i}")
            repository._v2_add_day(conn, "NEW", f"N{i}", "2026-03-01")

    stats = repository.all_district_stats(date(2026, 3, 1), date(2026, 3, 1))
    assert stats["total_summary_rows_used"] == 7
    assert stats["total_one_day_count"] == 7


def test_mixed_format_summary_cap_is_1024(db_path):
    with db.write_transaction() as conn:
        for i in range(600):
            repository._add_day(conn, "LEG", f"L{i}", "2026-03-01")
        for i in range(424):
            repository._ensure_new_registry(conn, "NEW", f"N{i}")
            repository._v2_add_day(conn, "NEW", f"N{i}", "2026-03-01")

    ok = repository.all_district_stats(date(2026, 3, 1), date(2026, 3, 1))
    assert ok["total_summary_rows_used"] == 1024

    with db.write_transaction() as conn:
        repository._ensure_new_registry(conn, "NEW", "overflow")
        repository._v2_add_day(conn, "NEW", "overflow", "2026-03-01")

    with pytest.raises(repository.SummaryLimitExceeded):
        repository.all_district_stats(date(2026, 3, 1), date(2026, 3, 1))


# ---------------------------------------------------------------------------
# HTTP / /docs surface
# ---------------------------------------------------------------------------


def test_migration_http_flow_and_docs(client):
    with db.write_transaction() as conn:
        seed_legacy(conn, "北沟片区", "ET-9", 300)

    spec = client.get("/openapi.json").json()
    assert "/migration/summaries" in spec["paths"]
    assert client.get("/docs").status_code == 200

    first = client.post(
        "/migration/summaries",
        json={"district": "北沟片区", "ear_tag": "ET-9", "step": True},
    ).json()
    assert first["batch_days_applied"] == 128
    assert first["state"] == "running"

    status = client.get("/migration/summaries/北沟片区/ET-9").json()
    assert status["remaining_days"] == 172

    done = client.post(
        "/migration/summaries/北沟片区/ET-9", params={"step": "false"}
    ).json()
    assert done["switched"] is True

    stats = client.get(
        "/stats/districts/北沟片区",
        params={"start_date": "2025-01-01", "end_date": "2026-12-31"},
    ).json()
    assert stats["multi_day_count"] == 1
    assert stats["summary_rows_used"] == 1


def test_migration_unknown_combo_is_404(client):
    response = client.post(
        "/migration/summaries", json={"district": "X", "ear_tag": "Y"}
    )
    assert response.status_code == 404
    assert client.get("/migration/summaries/X/Y").status_code == 404
