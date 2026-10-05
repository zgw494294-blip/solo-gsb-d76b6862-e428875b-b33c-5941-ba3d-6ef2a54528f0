"""FastAPI 入口：gzip NDJSON 批次接收与查询。"""
from __future__ import annotations

import sqlite3
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from . import db, ingest
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
