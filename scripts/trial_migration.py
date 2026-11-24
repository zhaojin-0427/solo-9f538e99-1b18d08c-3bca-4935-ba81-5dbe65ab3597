#!/usr/bin/env python3
"""端到端试跑：在线分块迁移、迁移中改期、中断重试、跨格式更正与写入量度量。

运行（在仓库根目录）：

    python scripts/trial_migration.py

脚本会在系统临时目录新建一个“旧 SQLite 卷”（只有初版三张表和整块 JSON
汇总），全部操作落在该临时库，不影响仓库里的 data/resighting.db。
每一步都会打印核对到的实际改动，最后用明细暴力重算核对统计。
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app import db, repository  # noqa: E402

OK = "✅"
RUN = "▶️ "


def section(title: str) -> None:
    print(f"\n{RUN}{title}")
    print("-" * 72)


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
    out: dict[str, dict[str, int]] = {}
    for (district, _tag), observed in days.items():
        b = out.setdefault(district, {"one": 0, "multi": 0})
        b["one" if len(observed) == 1 else "multi"] += 1
    return out


def build_old_volume(path: str, n_days: int = 320) -> None:
    """Create a baseline-era database with only the old JSON summary."""
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recognition_id TEXT NOT NULL UNIQUE,
            ear_tag TEXT NOT NULL, district TEXT NOT NULL,
            observed_on TEXT NOT NULL,
            committed INTEGER NOT NULL DEFAULT 1 CHECK (committed IN (0,1)),
            created_at TEXT NOT NULL DEFAULT 'x',
            updated_at TEXT NOT NULL DEFAULT 'x'
        );
        CREATE TABLE tag_summaries (
            district TEXT NOT NULL, ear_tag TEXT NOT NULL,
            committed INTEGER NOT NULL DEFAULT 1,
            first_observed_on TEXT NOT NULL, last_observed_on TEXT NOT NULL,
            day_counts TEXT NOT NULL,
            PRIMARY KEY (district, ear_tag, committed)
        ) WITHOUT ROWID;
        CREATE TABLE app_meta (`key` TEXT PRIMARY KEY, value INTEGER NOT NULL)
            WITHOUT ROWID;
        INSERT INTO app_meta VALUES ('state_version', 41);
        """
    )
    start = date(2025, 1, 1)
    entries: list[list] = []
    for i in range(n_days):
        day = (start + timedelta(days=i)).isoformat()
        entries.append([day, 1])
        conn.execute(
            "INSERT INTO observations (recognition_id, observed_on, district, ear_tag)"
            " VALUES (?, ?, '北沟片区', 'ET-MIG')",
            (f"R{i:04d}", day),
        )
    # Two captures on day 1 to prove counts migrate too.
    conn.execute(
        "INSERT INTO observations (recognition_id, observed_on, district, ear_tag)"
        " VALUES ('EXTRA', ?, '北沟片区', 'ET-MIG')",
        (entries[0][0],),
    )
    entries[0][1] = 2
    conn.execute(
        "INSERT INTO tag_summaries VALUES (?, ?, 1, ?, ?, ?)",
        (
            "北沟片区",
            "ET-MIG",
            entries[0][0],
            entries[-1][0],
            json.dumps(entries, separators=(",", ":")),
        ),
    )
    conn.commit()
    conn.close()


class Recorder:
    def __init__(self):
        self.statements: list[str] = []

    def __call__(self, statement: str) -> None:
        self.statements.append(" ".join(statement.split()))

    def reset(self):
        self.statements.clear()


def main() -> int:
    tmp = tempfile.mkdtemp(prefix="migration-trial-")
    old_db = os.path.join(tmp, "resighting.db")
    build_old_volume(old_db)
    print(f"临时旧卷: {old_db}")

    # Point the service at this file and run additive init.
    db.configure_db(old_db)
    db.init_db(old_db)

    failures: list[str] = []

    def check(cond: bool, msg: str) -> None:
        print(f"  {OK if cond else '❌'} {msg}")
        if not cond:
            failures.append(msg)

    section("1. 旧 SQLite 卷免回填启动，旧汇总继续供数")
    with db.read_transaction(old_db) as conn:
        tables = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    check({"tag_summaries", "tag_day_counts", "summary_registry"} <= tables,
          "附加式建表完成，旧 tag_summaries 原样保留")
    st = repository.get_migration_status("北沟片区", "ET-MIG", db_path=old_db)
    check(st["state"] == "not_started" and st["total_days"] == 320,
          f"识别为未迁移，旧汇总共 {st['total_days']} 天")
    before = repository.district_stats(
        "北沟片区", date(2025, 1, 1), date(2026, 12, 31), db_path=old_db
    )
    check(before["multi_day_count"] == 1 and before["summary_rows_used"] == 1,
          f"旧格式统计正常：multi={before['multi_day_count']} "
          f"rows={before['summary_rows_used']} version={before['state_version']}")

    section("2. 新建组合单次保存的汇总读写量不随历史天数增长")
    rec = Recorder()
    db.set_trace_hook(rec)

    def save_bytes(n: int) -> tuple[int, int]:
        tag = f"ET-SCALE-{n:05d}"
        for i in range(n):
            repository.put_observation(
                f"S-{n}-{i}", date(2020, 1, 1) + timedelta(days=i), "D", tag,
                db_path=old_db,
            )
        rid = f"S-T-{n}"
        repository.put_observation(rid, date(2026, 3, 1), "D", tag, db_path=old_db)
        rec.reset()
        repository.correct_observation(
            rid, {"observed_on": date(2026, 3, 9)}, db_path=old_db
        )
        total = sum(len(s) for s in rec.statements)
        biggest = max(len(s) for s in rec.statements)
        return total, biggest

    small, small_max = save_bytes(20)
    large, large_max = save_bytes(800)
    print(f"  20 天: 保存 trace {small} 字符, 最大单句 {small_max}")
    print(f" 800 天: 保存 trace {large} 字符, 最大单句 {large_max}")
    check(large < small + 800 and large_max < 1200,
          "历史 ×40，单次保存的 SQL 载荷基本不变，且没有整段 JSON 重写")
    db.set_trace_hook(None)

    section("3. 分块迁移：每个写事务最多改写 128 个日期")
    r1 = repository.migration_step("北沟片区", "ET-MIG", db_path=old_db)
    check(r1["batch_days_applied"] == 128 and r1["state"] == "running",
          f"第一块改写 {r1['batch_days_applied']} 天（=128 上限），状态 running")
    with db.read_transaction(old_db) as conn:
        n_day = conn.execute(
            "SELECT COUNT(*) c FROM tag_day_counts "
            "WHERE district='北沟片区' AND ear_tag='ET-MIG'"
        ).fetchone()["c"]
        blob_still = conn.execute(
            "SELECT 1 FROM tag_summaries "
            "WHERE district='北沟片区' AND ear_tag='ET-MIG'"
        ).fetchone()
    check(n_day == 128 and blob_still is not None,
          f"新表已写 {n_day} 行；切换前旧 JSON 行仍在并继续供数")

    section("4. 迁移中改期 + 新录入（双写，不丢观测）")
    moved_action, _moved_rec, _ = repository.correct_observation(
        "R0010", {"observed_on": date(2026, 2, 2)}, db_path=old_db
    )
    created_action, _created_rec, _ = repository.put_observation(
        "MID-NEW", date(2026, 2, 3), "北沟片区", "ET-MIG", db_path=old_db
    )
    check(moved_action == "updated" and created_action == "created",
          "第一块之后提交的改期与录入均成功")
    with db.read_transaction(old_db) as conn:
        # R0010 原日 2025-01-11：只移动这一条，计数 1 -> 该日消失
        old_day = conn.execute(
            "SELECT capture_count FROM tag_day_counts "
            "WHERE district='北沟片区' AND ear_tag='ET-MIG' AND observed_on='2025-01-11'"
        ).fetchone()
        new_day = conn.execute(
            "SELECT capture_count FROM tag_day_counts "
            "WHERE district='北沟片区' AND ear_tag='ET-MIG' AND observed_on='2026-02-02'"
        ).fetchone()
        mid_new = conn.execute(
            "SELECT capture_count FROM tag_day_counts "
            "WHERE district='北沟片区' AND ear_tag='ET-MIG' AND observed_on='2026-02-03'"
        ).fetchone()
    check(old_day is None and new_day is not None and mid_new is not None,
          "改期日与新录入日已同步到新表（双写），原日计数清空")

    section("5. 中断 / 失败回滚，旧汇总继续供数，重试成功才切换")
    calls = {"n": 0}

    def crash_on_second_batch(stage, **info):
        if stage == "batch":
            calls["n"] += 1
            if calls["n"] == 2:
                raise RuntimeError("模拟进程被杀死 / 磁盘错误")

    repository.set_migration_fault_hook(crash_on_second_batch)
    crashed = False
    try:
        repository.migrate_summary("北沟片区", "ET-MIG", db_path=old_db)
    except RuntimeError:
        crashed = True
    repository.set_migration_fault_hook(None)
    check(crashed, "第二块注入故障，迁移事务抛出")
    st2 = repository.get_migration_status("北沟片区", "ET-MIG", db_path=old_db)
    check(st2["state"] == "running" and st2["chunk_count"] == 2,
          f"故障块整体回滚；此前两块已落库（chunk_count={st2['chunk_count']}，"
          "第 2 次 migrate 内首个事务成功、第二个事务回滚）")
    mid = repository.district_stats(
        "北沟片区", date(2025, 1, 1), date(2026, 12, 31), db_path=old_db
    )
    check(mid["multi_day_count"] == 1, "中断期间旧汇总仍给出统计")

    done = repository.migrate_summary("北沟片区", "ET-MIG", db_path=old_db)
    check(done["switched"] is True and done["done"] is True,
          f"重试成功并切换（共 {done['chunk_count']} 个写事务）")
    with db.read_transaction(old_db) as conn:
        blob = conn.execute(
            "SELECT 1 FROM tag_summaries "
            "WHERE district='北沟片区' AND ear_tag='ET-MIG'"
        ).fetchone()
        reg = conn.execute(
            "SELECT format, observed_days FROM summary_registry "
            "WHERE district='北沟片区' AND ear_tag='ET-MIG'"
        ).fetchone()
        nday = conn.execute(
            "SELECT COUNT(*) c, SUM(capture_count) s FROM tag_day_counts "
            "WHERE district='北沟片区' AND ear_tag='ET-MIG'"
        ).fetchone()
    check(blob is None and reg["format"] == "new",
          "切换后旧 JSON 行删除，registry=format:new")
    print(f"     实际新表：{nday['c']} 个不同日期，次数合计 {nday['s']}，"
          f"registry.observed_days={reg['observed_days']}")

    section("6. 与明细暴力重算核对（含迁移中改动）")
    start, end = date(2025, 1, 1), date(2026, 12, 31)
    got = repository.district_stats("北沟片区", start, end, db_path=old_db)
    with db.read_transaction(old_db) as conn:
        want = brute_force(conn, start, end)["北沟片区"]
    check(got["one_day_count"] == want["one"]
          and got["multi_day_count"] == want["multi"],
          f"统计 one={got['one_day_count']} multi={got['multi_day_count']} "
          f"与明细重算 one={want['one']} multi={want['multi']} 一致")
    check(got["summary_rows_used"] == 1,
          f"仍按 1 条组合汇总计（summary_rows_used={got['summary_rows_used']}）")

    section("7. 跨新旧格式更正：两边汇总同一事务生效，失败整体回滚")
    with db.write_transaction(old_db) as conn:
        repository._add_day(conn, "LEG片区", "TAG-L", "2026-03-03")
    repository.put_observation("CROSS", date(2026, 3, 3), "LEG片区", "TAG-L",
                              db_path=old_db)
    for i in range(2):
        repository.put_observation(
            f"NW{i}", date(2026, 3, 1 + i), "NEW片区", "TAG-N", db_path=old_db
        )
    repository.correct_observation(
        "CROSS",
        {"district": "NEW片区", "ear_tag": "TAG-N", "observed_on": date(2026, 3, 2)},
        db_path=old_db,
    )
    ls = repository.district_stats("LEG片区", date(2026, 3, 1), date(2026, 3, 31),
                                   db_path=old_db)
    ns = repository.district_stats("NEW片区", date(2026, 3, 1), date(2026, 3, 31),
                                   db_path=old_db)
    check(ls["one_day_count"] == 1 and ns["multi_day_count"] == 1,
          "旧格式侧留下种子计数，新格式侧并入后成为多天个体")

    original_bump = repository._bump_state_version

    def boom(conn):
        raise RuntimeError("模拟保存最后阶段失败")

    repository._bump_state_version = boom
    rollback_ok = False
    try:
        repository.correct_observation(
            "NW0", {"district": "LEG片区", "ear_tag": "TAG-L"}, db_path=old_db
        )
    except RuntimeError:
        rollback_ok = True
    finally:
        repository._bump_state_version = original_bump
    check(rollback_ok, "注入失败，更正事务报错")
    rec_nw0 = repository.get_observation("NW0", db_path=old_db)
    check(rec_nw0["district"] == "NEW片区",
          f"失败后明细仍是原值（district={rec_nw0['district']}），两边汇总一并回滚")

    section("结果")
    if failures:
        print(f"❌ {len(failures)} 项核对失败")
        for f in failures:
            print("  -", f)
        return 1
    print(f"{OK} 全部核对通过。临时库保留在 {old_db}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
