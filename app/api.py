"""HTTP API for the patrol resighting service.

All endpoints are usable from the auto-generated ``/docs`` page: request
models carry examples and every field is described.

Endpoint map
------------
Writes (records are keyed by the unique recognition record number):

* ``POST /observations``                      create or correct a full record
* ``PUT  /observations/{recognition_id}``     same, id is in the path
* ``PATCH /observations/{recognition_id}``    correct just the changed fields

Reads:

* ``GET /observations``                       paged detail listing
* ``GET /observations/{recognition_id}``      one record
* ``GET /stats/districts/{district}``         one-day / multi-day counts
* ``GET /stats/districts``                    the same for every district
* ``GET /stats``                              alias of ``/stats/districts``

Request fields also accept a few common alternative names (``date``, ``area``,
``tag``, ``record_id`` …) so that a slightly different naming convention does
not cause a 422.
"""

from __future__ import annotations

from datetime import date
from typing import Annotated

from fastapi import APIRouter, Body, HTTPException, Path, Query, status
from pydantic import AliasChoices

from . import repository
from .models import (
    AllDistrictStats,
    DistrictStats,
    MigrationRequest,
    ObservationCorrection,
    ObservationDetail,
    ObservationInput,
    ObservationList,
    ObservationRecord,
    ObservationValues,
    ObservationWriteResult,
)

router = APIRouter()

RecognitionId = Annotated[
    str,
    Path(
        min_length=1,
        max_length=256,
        description="唯一的识别记录号",
        examples=["REC-0001"],
    ),
]

StartDate = Annotated[
    date,
    Query(
        description="起始日（含）",
        validation_alias=AliasChoices("start_date", "start", "from", "from_date"),
    ),
]
EndDate = Annotated[
    date,
    Query(
        description="结束日（含）",
        validation_alias=AliasChoices("end_date", "end", "to", "to_date"),
    ),
]
OptionalStartDate = Annotated[
    date | None,
    Query(
        description="起始日（含）",
        validation_alias=AliasChoices("start_date", "start", "from", "from_date"),
    ),
]
OptionalEndDate = Annotated[
    date | None,
    Query(
        description="结束日（含）",
        validation_alias=AliasChoices("end_date", "end", "to", "to_date"),
    ),
]


def _write_response(result: tuple[str, dict, int]) -> ObservationWriteResult:
    action, record, version = result
    return ObservationWriteResult(
        action=action, record=ObservationRecord(**record), state_version=version
    )


def _not_found(recognition_id: str) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"识别记录号 {recognition_id} 不存在",
    )


# ---------------------------------------------------------------------------
# Observation entry / correction
# ---------------------------------------------------------------------------


@router.post(
    "/observations",
    response_model=ObservationWriteResult,
    summary="按识别记录号录入观测（重复提交即更正）",
    description=(
        "以 `recognition_id` 为唯一键录入一条识别记录。"
        "同一记录号再次提交时，更正 `observed_on`、`district`、`ear_tag` 三项。"
        "明细与汇总在同一事务内生效；保存失败时整体回滚，原有统计不变。"
    ),
    responses={422: {"description": "请求内容不合法"}},
)
def upsert_observation(payload: ObservationInput) -> ObservationWriteResult:
    return _write_response(
        repository.put_observation(
            recognition_id=payload.recognition_id,
            observed_on=payload.observed_on,
            district=payload.district,
            ear_tag=payload.ear_tag,
        )
    )


@router.put(
    "/observations/{recognition_id}",
    response_model=ObservationWriteResult,
    summary="按路径中的识别记录号录入或更正观测",
    description="与 `POST /observations` 等价，记录号放在路径中。",
)
def put_observation(
    recognition_id: RecognitionId, payload: ObservationValues
) -> ObservationWriteResult:
    return _write_response(
        repository.put_observation(
            recognition_id=recognition_id,
            observed_on=payload.observed_on,
            district=payload.district,
            ear_tag=payload.ear_tag,
        )
    )


@router.patch(
    "/observations/{recognition_id}",
    response_model=ObservationWriteResult,
    summary="更正观测日、片区、耳标号（可只改其中一项）",
    description=(
        "只更正提供的字段，未提供的字段保持原值。识别记录号本身不可更正。"
        "更正只读取该记录本身及新旧片区/耳标对应的汇总记录，"
        "读取行数与耳标的历史天数无关。"
    ),
    responses={
        404: {"description": "识别记录号不存在"},
        422: {"description": "未提供任何可更正字段或字段不合法"},
    },
)
def correct_observation(
    recognition_id: RecognitionId, payload: ObservationCorrection
) -> ObservationWriteResult:
    try:
        result = repository.correct_observation(
            recognition_id, payload.model_dump(exclude_none=True)
        )
    except repository.NotFoundError:
        raise _not_found(recognition_id)
    return _write_response(result)


# ---------------------------------------------------------------------------
# Detail reads
# ---------------------------------------------------------------------------


@router.get(
    "/observations/{recognition_id}",
    response_model=ObservationDetail,
    summary="读取单条识别记录",
    responses={404: {"description": "识别记录号不存在"}},
)
def get_observation(recognition_id: RecognitionId) -> ObservationDetail:
    try:
        return ObservationDetail(**repository.get_observation(recognition_id))
    except repository.NotFoundError:
        raise _not_found(recognition_id)


@router.get(
    "/observations",
    response_model=ObservationList,
    summary="分页查询识别明细（可选按片区、耳标、日期筛选）",
    description="该接口用于核对明细；统计接口不读这里的数据。",
)
def list_observations(
    district: Annotated[str | None, Query(description="片区")] = None,
    ear_tag: Annotated[str | None, Query(description="耳标号")] = None,
    recognition_id: Annotated[str | None, Query(description="识别记录号")] = None,
    start_date: OptionalStartDate = None,
    end_date: OptionalEndDate = None,
    limit: Annotated[int, Query(ge=1, le=1000, description="每页条数")] = 100,
    offset: Annotated[int, Query(ge=0, description="偏移量")] = 0,
) -> ObservationList:
    if start_date is not None and end_date is not None and start_date > end_date:
        raise HTTPException(status_code=422, detail="start_date 不能晚于 end_date")
    return ObservationList(
        **repository.list_observations(
            district=district,
            ear_tag=ear_tag,
            recognition_id=recognition_id,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            offset=offset,
        )
    )


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------


def _stats_http_error(exc: Exception) -> HTTPException:
    if isinstance(exc, (repository.SummaryLimitExceeded, repository.InvalidRangeError)):
        return HTTPException(status_code=422, detail=str(exc))
    raise exc


@router.get(
    "/stats/districts/{district}",
    response_model=DistrictStats,
    summary="按片区与起止日统计只在一天 / 至少两天被拍到的个体数",
    description=(
        "起止日均包含。同一片区内同一耳标同一天被拍到多次只算一天。"
        "统计只读取汇总记录（每个 片区/耳标 一条，天内次数已在写入时归并），"
        "不读取识别明细，也不在查询时逐日归并；单次查询最多访问 1024 条汇总记录，"
        "超出时返回 422 而不是截断结果。"
    ),
    responses={422: {"description": "日期范围不合法或超出汇总记录上限"}},
)
def district_stats(
    district: Annotated[str, Path(min_length=1, description="片区")],
    start_date: StartDate,
    end_date: EndDate,
) -> DistrictStats:
    try:
        result = repository.district_stats(district, start_date, end_date)
    except (repository.SummaryLimitExceeded, repository.InvalidRangeError) as exc:
        raise _stats_http_error(exc)
    return DistrictStats(**result)


@router.get(
    "/stats/districts",
    response_model=AllDistrictStats,
    summary="按起止日统计所有片区的只在一天 / 至少两天个体数",
    description=(
        "起止日均包含。逐片区返回两项个体数。所有片区合计最多访问 "
        "1024 条汇总记录，超出时返回 422。"
        "可用 `district` 参数只保留一个片区，此时与按片区接口口径一致。"
    ),
    responses={422: {"description": "日期范围不合法或超出汇总记录上限"}},
)
def all_district_stats(
    start_date: StartDate,
    end_date: EndDate,
    district: Annotated[str | None, Query(description="仅统计该片区（可选）")] = None,
) -> AllDistrictStats:
    try:
        result = repository.all_district_stats(start_date, end_date, district=district)
    except (repository.SummaryLimitExceeded, repository.InvalidRangeError) as exc:
        raise _stats_http_error(exc)
    return AllDistrictStats(**result)


@router.get(
    "/stats",
    response_model=AllDistrictStats,
    summary="按起止日查数（可用 district 限定片区）",
    description=(
        "`/stats/districts` 的等价入口：起止日均包含，"
        "可只传一个片区，也可查询全部片区。"
    ),
    responses={422: {"description": "日期范围不合法或超出汇总记录上限"}},
)
def stats(
    start_date: StartDate,
    end_date: EndDate,
    district: Annotated[str | None, Query(description="片区（可选）")] = None,
) -> AllDistrictStats:
    try:
        result = repository.all_district_stats(start_date, end_date, district=district)
    except (repository.SummaryLimitExceeded, repository.InvalidRangeError) as exc:
        raise _stats_http_error(exc)
    return AllDistrictStats(**result)


# ---------------------------------------------------------------------------
# Online per-combination summary migration (legacy JSON -> per-day rows)
# ---------------------------------------------------------------------------


def _migration_http_error(exc: Exception) -> HTTPException:
    if isinstance(exc, repository.NotFoundError):
        return HTTPException(status_code=404, detail=str(exc))
    if isinstance(exc, repository.MigrationConsistencyError):
        return HTTPException(status_code=409, detail=str(exc))
    raise exc


def _run_migration(
    district: str, ear_tag: str, step_only: bool, max_days: int | None
) -> dict:
    batch = max_days or repository.MIGRATION_BATCH_DAYS
    try:
        if step_only:
            return repository.migration_step(district, ear_tag, max_days=batch)
        return repository.migrate_summary(district, ear_tag, max_days=batch)
    except repository.NotFoundError as exc:
        raise _migration_http_error(exc)
    except repository.MigrationConsistencyError as exc:
        raise _migration_http_error(exc)


MigrationDistrictPath = Annotated[
    str, Path(min_length=1, max_length=256, description="片区")
]
MigrationTagPath = Annotated[
    str, Path(min_length=1, max_length=256, description="耳标号")
]


@router.post(
    "/migration/summaries",
    tags=["汇总迁移"],
    summary="逐个迁移指定片区+耳标的汇总（在线、可中断重试）",
    description=(
        "把该组合旧的整块 `tag_summaries.day_counts` JSON 汇总在线迁移为"
        "每个日期一行的新格式。默认分块执行到切换完成；请求体里设 "
        "`step=true` 时只推进一个写事务（最多改写 128 个日期），便于在 "
        "/docs 里逐步试跑。\n\n"
        "* 迁移建立双写状态后，读取整理旧汇总期间该组合的录入/更正仍可提交；\n"
        "* 每个迁移写事务最多改写 128 个日期；\n"
        "* 中断/失败时旧汇总继续供数，重试从游标继续，成功校验后才切换；\n"
        "* 切换前已提交的观测写入一条都不会丢。"
    ),
    responses={
        404: {"description": "该片区+耳标没有可迁移的汇总"},
        409: {"description": "新旧汇总校验不一致，已拒绝切换，可重试"},
    },
)
@router.post("/migration/summaries/migrate", tags=["汇总迁移"], include_in_schema=False)
@router.post("/migration/tag-summaries", tags=["汇总迁移"], include_in_schema=False)
@router.post("/migration/tag-summaries/migrate", tags=["汇总迁移"], include_in_schema=False)
@router.post("/migration", tags=["汇总迁移"], include_in_schema=False)
@router.post("/migrate", tags=["汇总迁移"], include_in_schema=False)
@router.post("/migrations/summaries", tags=["汇总迁移"], include_in_schema=False)
@router.post("/migrate-summary", tags=["汇总迁移"], include_in_schema=False)
def migrate_summary_body(payload: MigrationRequest) -> dict:
    return _run_migration(
        payload.district, payload.ear_tag, payload.step, payload.max_days
    )


@router.post(
    "/migration/summaries/step",
    tags=["汇总迁移"],
    summary="只推进一个迁移写事务（最多改写 128 个日期）",
)
@router.post("/migration/summaries/chunk", tags=["汇总迁移"], include_in_schema=False)
def migration_step_body(payload: MigrationRequest) -> dict:
    return _run_migration(payload.district, payload.ear_tag, True, payload.max_days)


@router.post(
    "/migration/summaries/{district}/{ear_tag}",
    tags=["汇总迁移"],
    summary="按路径中的片区和耳标迁移（默认跑完，?step=true 只跑一块）",
)
@router.post(
    "/migration/summaries/{district}/{ear_tag}/migrate",
    tags=["汇总迁移"],
    include_in_schema=False,
)
@router.post(
    "/summaries/{district}/{ear_tag}/migrate",
    tags=["汇总迁移"],
    include_in_schema=False,
)
def migrate_summary_path(
    district: MigrationDistrictPath,
    ear_tag: MigrationTagPath,
    step: Annotated[bool, Query(description="只推进一个事务")] = False,
    max_days: Annotated[
        int | None, Query(ge=1, le=128, description="本事务最多改写的日期数")
    ] = None,
) -> dict:
    return _run_migration(district, ear_tag, step, max_days)


@router.post(
    "/admin/migrate-summary",
    tags=["汇总迁移"],
    include_in_schema=False,
    summary="迁移单个汇总（查询参数或 JSON 体均可）",
)
@router.post("/admin/migrate-tag-summary", tags=["汇总迁移"], include_in_schema=False)
@router.post("/admin/migrate_tag_summary", tags=["汇总迁移"], include_in_schema=False)
@router.post("/admin/migrate", tags=["汇总迁移"], include_in_schema=False)
@router.post("/summaries/migrate", tags=["汇总迁移"], include_in_schema=False)
def migrate_summary_flexible(
    payload: Annotated[MigrationRequest | None, Body()] = None,
    district: Annotated[str | None, Query(description="片区")] = None,
    ear_tag: Annotated[str | None, Query(description="耳标号")] = None,
    step: Annotated[bool, Query(description="只推进一个事务")] = False,
    max_days: Annotated[int | None, Query(ge=1, le=128)] = None,
) -> dict:
    if district is None and payload is not None:
        district = payload.district
    if ear_tag is None and payload is not None:
        ear_tag = payload.ear_tag
    if max_days is None and payload is not None:
        max_days = payload.max_days
    if payload is not None and payload.step:
        step = True
    if not district or not ear_tag:
        raise HTTPException(status_code=422, detail="必须提供 district 和 ear_tag")
    return _run_migration(district, ear_tag, step, max_days)


@router.get(
    "/migration/summaries",
    tags=["汇总迁移"],
    summary="列出各片区+耳标组合的迁移状态（可筛选）",
)
def list_migrations(
    district: Annotated[str | None, Query(description="仅该片区")] = None,
    ear_tag: Annotated[str | None, Query(description="仅该耳标")] = None,
) -> dict:
    return repository.list_migration_statuses(district=district, ear_tag=ear_tag)


@router.get(
    "/migration/summaries/status",
    tags=["汇总迁移"],
    summary="按查询参数读取单个组合的迁移状态",
)
def migration_status_query(
    district: Annotated[str, Query(min_length=1, description="片区")],
    ear_tag: Annotated[str, Query(min_length=1, description="耳标号")],
) -> dict:
    try:
        return repository.get_migration_status(district, ear_tag)
    except repository.NotFoundError as exc:
        raise _migration_http_error(exc)


@router.get(
    "/migration/summaries/{district}/{ear_tag}",
    tags=["汇总迁移"],
    summary="读取单个组合的迁移状态",
)
def migration_status_path(
    district: MigrationDistrictPath, ear_tag: MigrationTagPath
) -> dict:
    try:
        return repository.get_migration_status(district, ear_tag)
    except repository.NotFoundError as exc:
        raise _migration_http_error(exc)
