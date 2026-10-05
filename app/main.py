"""FastAPI 入口：gzip NDJSON 批次接收与查询。"""
from __future__ import annotations

import sqlite3
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from . import cursor as cursor_mod
from . import db, ingest
from .cursor import CursorError
from .ingest import IngestError, PayloadTooLarge


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Compose 启动时即完成数据库初始化（含暂存表残留清理）
    db.init_db()
    yield


app = FastAPI(
    title="gzip NDJSON Batch API",
    version="1.0.0",
    lifespan=lifespan,
)


@app.post("/batches", status_code=201)
async def upload_batch(request: Request) -> dict[str, Any]:
    """上传单个 gzip 流，校验通过后整批落库；成功返回批次 ID 与行数。

    有效行在 gzip 流完整结束、CRC 正确且请求结束、整批事务提交前不可查询。
    """
    content_length = request.headers.get("content-length")
    cl = int(content_length) if content_length is not None else None

    conn = db.connect()
    try:
        try:
            batch_id, line_count = await ingest.ingest_stream(
                conn, request.stream(), cl
            )
        except PayloadTooLarge as exc:
            raise HTTPException(status_code=413, detail=str(exc))
        except IngestError as exc:
            raise HTTPException(status_code=422, detail=str(exc))
        except sqlite3.Error as exc:
            raise HTTPException(status_code=500, detail=f"database error: {exc}")
        return {"batch_id": batch_id, "line_count": line_count}
    finally:
        conn.close()


@app.get("/batches")
def list_batches(
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> JSONResponse:
    """列出已提交批次，按提交顺序（批次 ID 序号）分页。"""
    with db.session() as conn:
        rows, total = db.list_batches(conn, limit, offset)
    return JSONResponse(
        {
            "total": total,
            "limit": limit,
            "offset": offset,
            "items": [
                {
                    "batch_id": row["id"],
                    "line_count": row["line_count"],
                    "created_at": row["created_at"],
                }
                for row in rows
            ],
        }
    )


@app.get("/batches/{batch_id}/lines")
def get_batch_lines(
    batch_id: str,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> JSONResponse:
    """按批次 ID 分页读行，保持上传时的原始顺序；未知 ID 返回 404。"""
    with db.session() as conn:
        batch = db.get_batch(conn, batch_id)
        if batch is None:
            raise HTTPException(status_code=404, detail="batch not found")
        rows = db.get_lines(conn, batch_id, limit, offset)
        return JSONResponse(
            {
                "batch_id": batch_id,
                "line_count": batch["line_count"],
                "limit": limit,
                "offset": offset,
                "items": [
                    {"seq": row["seq"], "id": row["id"], "message": row["message"]}
                    for row in rows
                ],
            }
        )


@app.get("/logs/search")
def search_logs(
    q: str | None = Query(default=None, max_length=200),
    cursor: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=100),
) -> JSONResponse:
    """跨所有已提交批次按 message 的字面子串检索日志。

    - 大小写敏感、逐字节匹配：``%``、``_`` 等不做通配符，查询词中的空白
      （含首尾）原样保留；长度 1～200 个字符。
    - 首请求带 ``q``：页大小 ``limit`` 取 1～100，并固定当时已提交批次
      范围（快照上界）。续页只带 ``cursor`` 与 ``limit``，查询词与范围沿用
      首请求；翻页期间新提交的批次不会混入旧查询。暂存中的批次不参与。
    - 游标已签名且持久化，服务重启后仍可续用；游标损坏或字段不合法返回
      422。无命中或已到末页时返回空 items / ``next_cursor=null``。
    """
    # q 与 cursor 必须二选一：续页沿用首请求查询词，不允许在续页改词
    if (q is None) == (cursor is None):
        raise HTTPException(
            status_code=422,
            detail="exactly one of query parameters 'q' (first page) or "
            "'cursor' (next page) is required",
        )
    if q is not None and not (1 <= len(q) <= 200):
        raise HTTPException(
            status_code=422, detail="q must be 1..200 characters"
        )

    if cursor is not None:
        try:
            state = cursor_mod.decode_cursor(cursor)
        except CursorError as exc:
            raise HTTPException(status_code=422, detail=f"invalid cursor: {exc}")
    else:
        assert q is not None
        # 首请求固定快照上界；与检索分属两条短语句，autocommit 下均不持锁
        with db.session() as conn:
            bound = db.max_batch_rowid(conn)
        state = {"q": q, "m": bound, "b": 0, "s": 0}

    with db.session() as conn:
        # 多取 1 行判断是否有下一页，避免缓存全量命中
        rows = db.search_lines(
            conn,
            state["q"],
            state["m"],
            state["b"],
            state["s"],
            limit + 1,
        )

    next_cursor = None
    if len(rows) > limit:
        last = rows[limit - 1]
        next_cursor = cursor_mod.encode_cursor(
            state["q"], state["m"], last["_browid"], last["seq"]
        )
        rows = rows[:limit]

    return JSONResponse(
        {
            "items": [
                {
                    "batch_id": row["batch_id"],
                    "seq": row["seq"],
                    "id": row["id"],
                    "message": row["message"],
                }
                for row in rows
            ],
            "next_cursor": next_cursor,
        }
    )
