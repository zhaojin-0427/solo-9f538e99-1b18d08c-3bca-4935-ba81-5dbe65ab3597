"""FastAPI application factory and lifespan wiring."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from . import db
from .api import router

API_DESCRIPTION = """
保护区巡护补拍计划的取数接口。数据存放在 SQLite。

**录入与更正**

每观测到一只带标动物，按唯一的 `recognition_id`（识别记录号）录入，
可更正的三项是 `observed_on`（观测日）、`district`（片区）、`ear_tag`（耳标号）。
同一记录号重复提交即更正，不会产生重复记录。
同一片区内同一耳标同一天无论被拍到多少次，统计时只算一天。

**统计口径**

`/stats/districts/{district}` 与 `/stats/districts`（以及等价的 `/stats`）
按起止日（含首尾两天）返回两项个体数：

* `one_day_count`：区间内只在一天被拍到的耳标个体数；
* `multi_day_count`：区间内被拍到至少两天的耳标个体数。

统计只读取按「片区 / 耳标」预聚合的汇总记录，不读取识别明细、不在查询时
逐日归并，单次查询最多访问 1024 条汇总记录。

**两种汇总格式与在线迁移**

新录入的组合直接使用每个日期一行的新格式（`tag_day_counts`），单次保存
只读写当天那一行和一条定长状态行，读写量不随历史天数增长。旧 SQLite 卷
里的整块 JSON 汇总（`tag_summaries`）无需预先回填即可启动并继续供数；
可在 **汇总迁移** 分组的 `/migration/summaries` 接口按片区 + 耳标逐个、
分块（每个写事务最多改写 128 个日期）在线迁移，支持中途改期/录入、
中断重试，校验成功后才切换。统计在同一读取快照内同时覆盖两种格式。

**一致性**

明细与汇总在同一事务内生效；跨新旧两种格式的更正会同时更新两边汇总。
查询在单个读取快照中进行，明细与汇总总是来自同一个已提交状态；保存失败时
整体回滚，仍能查到原有数字。响应的 `state_version` 标明本次读写对应的已
提交状态版本。
"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    yield


def create_app() -> FastAPI:
    app = FastAPI(
        title="保护区巡护补拍取数接口",
        description=API_DESCRIPTION,
        version="1.0.0",
        lifespan=lifespan,
    )
    app.include_router(router)

    @app.get("/health", tags=["运维"], summary="健康检查")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    return app


app = create_app()
