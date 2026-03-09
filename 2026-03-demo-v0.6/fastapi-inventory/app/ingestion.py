# app/ingestion.py
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.db import get_ingestion_control_collection, get_ingestion_runs_collection

CONTROL_DOC_ID = os.getenv("CONTROL_DOC_ID", "ecmwf_ifs_ingestion")

templates = Jinja2Templates(directory="app/templates")
router = APIRouter(tags=["ingestion"])


def _fmt_dt(v: Any) -> str:
    """
    UI 표시용
    - datetime이면 UTC 문자열로
    - str이면 그대로(이미 ISO/Z 포맷인 케이스)
    """
    if v is None:
        return ""
    if isinstance(v, str):
        return v
    if isinstance(v, datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    return str(v)


def _safe_counters(c: Any) -> Dict[str, int]:
    if not isinstance(c, dict):
        return {"downloaded": 0, "existed": 0, "uploaded": 0, "mongo_written": 0, "failed": 0}
    return {
        "downloaded": int(c.get("downloaded", 0) or 0),
        "existed": int(c.get("existed", 0) or 0),
        "uploaded": int(c.get("uploaded", 0) or 0),
        "mongo_written": int(c.get("mongo_written", 0) or 0),
        "failed": int(c.get("failed", 0) or 0),
    }


@router.get("/ingestion", response_class=HTMLResponse, include_in_schema=False)
async def ingestion_page(
    request: Request,
    status: Optional[str] = Query(None),
    variable: Optional[str] = Query(None),
    stream: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
):
    control_col = await get_ingestion_control_collection()
    runs_col = await get_ingestion_runs_collection()

    control = await control_col.find_one({"_id": CONTROL_DOC_ID}) or {}

    q: Dict[str, Any] = {}
    if status:
        q["status"] = status
    if variable:
        q["variable"] = variable
    if stream:
        q["stream"] = stream

    # ✅ 핵심: 새 스키마 필드들을 projection에 포함
    # attempts는 너무 커질 수 있으니 최근 몇 개만(원하면 -10 등으로)
    projection: Dict[str, Any] = {
        "_id": 1,
        "run_time_utc": 1,
        "stream": 1,
        "variable": 1,
        "status": 1,
        "updated_at": 1,
        "running": 1,

        # new schema
        "attempts_cnt": 1,
        "started_at_last": 1,
        "finished_at_last": 1,
        "duration_sec_last": 1,
        "trigger_last": 1,
        "counters_last": 1,
        "counters_total": 1,
        "notes_last": 1,
        "attempts": {"$slice": -5},

        # legacy compatibility(혹시 남아있을 수도)
        "started_at": 1,
        "finished_at": 1,
        "counters": 1,
        "trigger": 1,
        "errors": 1,
    }

    raw_runs = (
        await runs_col.find(q, projection)
        .sort([("run_time_utc", -1), ("variable", 1)])
        .limit(limit)
        .to_list(length=limit)
    )

    # dropdown 후보(최근 데이터 기반)
    vars_ = sorted({r.get("variable") for r in raw_runs if r.get("variable")})
    streams_ = sorted({r.get("stream") for r in raw_runs if r.get("stream")})
    statuses_ = ["running", "success", "partial", "failed"]

    # ✅ 템플릿이 기대하는 형태로 "정규화"해서 내려주기
    runs: List[Dict[str, Any]] = []
    for r in raw_runs:
        attempts = r.get("attempts") or []
        attempts_cnt_val = r.get("attempts_cnt")
        if attempts_cnt_val is None:
            attempts_cnt_val = len(attempts)

        # last 우선, 없으면 legacy 필드 사용
        started_last = r.get("started_at_last") or r.get("started_at")
        finished_last = r.get("finished_at_last") or r.get("finished_at")
        counters_last = r.get("counters_last") or r.get("counters")
        counters_last = _safe_counters(counters_last)

        # top-level errors는 이제 없을 수 있으니, "마지막 attempt" 에러를 대표로 노출
        last_attempt_errors = []
        if attempts:
            last_attempt_errors = (attempts[-1].get("errors") or [])

        # attempts 내부도 시간 포맷 정리 + counters 안전화
        norm_attempts = []
        for a in attempts:
            ac = _safe_counters(a.get("counters"))
            norm_attempts.append({
                **a,
                "started_at": _fmt_dt(a.get("started_at")),
                "finished_at": _fmt_dt(a.get("finished_at")),
                "counters": ac,
                "duration_sec": a.get("duration_sec") if a.get("duration_sec") is not None else "",
                "errors": a.get("errors") or [],
            })

        runs.append({
            **r,

            # 템플릿에서 has_attempts / attempts_cnt 쓰도록
            "attempts": norm_attempts,
            "attempts_cnt": int(attempts_cnt_val),

            # 템플릿이 started/finished/counters를 잡을 수 있도록 last도 같이 내려줌
            "started_at_last": _fmt_dt(r.get("started_at_last")),
            "finished_at_last": _fmt_dt(r.get("finished_at_last")),
            "counters_last": counters_last,

            # 기존 템플릿 호환용(없어도 되는데 안전하게)
            "started_at": _fmt_dt(started_last),
            "finished_at": _fmt_dt(finished_last),
            "counters": counters_last,

            # top-level errors 토글 호환(대표로 마지막 attempt errors 보여줌)
            "errors": last_attempt_errors,
            "errors_cnt": len(last_attempt_errors),
        })

    return templates.TemplateResponse(
        "ingestion.html",
        {
            "request": request,
            "control": {
                **control,
                "last_started_at": _fmt_dt(control.get("last_started_at")),
                "last_finished_at": _fmt_dt(control.get("last_finished_at")),
                "last_heartbeat_at": _fmt_dt(control.get("last_heartbeat_at")),
            },
            "runs": runs,
            "filters": {"status": status or "", "variable": variable or "", "stream": stream or "", "limit": limit},
            "vars": vars_,
            "streams": streams_,
            "statuses": statuses_,
        },
    )


@router.post("/ingestion/pause", include_in_schema=False)
async def ingestion_pause():
    control_col = await get_ingestion_control_collection()
    await control_col.update_one(
        {"_id": CONTROL_DOC_ID},
        {"$set": {"enabled": False, "status": "paused", "updated_at": datetime.now(timezone.utc)}},
        upsert=True,
    )
    return RedirectResponse(url="/ingestion", status_code=303)


@router.post("/ingestion/resume", include_in_schema=False)
async def ingestion_resume():
    control_col = await get_ingestion_control_collection()
    await control_col.update_one(
        {"_id": CONTROL_DOC_ID},
        {"$set": {"enabled": True, "status": "idle", "updated_at": datetime.now(timezone.utc)}},
        upsert=True,
    )
    return RedirectResponse(url="/ingestion", status_code=303)


@router.get("/api/ingestion/control")
async def api_ingestion_control():
    control_col = await get_ingestion_control_collection()
    return await control_col.find_one({"_id": CONTROL_DOC_ID}, {"_id": 0}) or {}


@router.get("/api/ingestion/runs")
async def api_ingestion_runs(
    status: Optional[str] = None,
    variable: Optional[str] = None,
    stream: Optional[str] = None,
    limit: int = 50,
):
    runs_col = await get_ingestion_runs_collection()
    q: Dict[str, Any] = {}
    if status:
        q["status"] = status
    if variable:
        q["variable"] = variable
    if stream:
        q["stream"] = stream

    # API도 새 스키마 기준으로
    proj = {
        "_id": 0,
        "run_time_utc": 1,
        "stream": 1,
        "variable": 1,
        "status": 1,
        "attempts_cnt": 1,
        "started_at_last": 1,
        "finished_at_last": 1,
        "duration_sec_last": 1,
        "trigger_last": 1,
        "counters_last": 1,
        "counters_total": 1,
        "attempts": {"$slice": -5},
        "updated_at": 1,
    }
    return (
        await runs_col.find(q, proj)
        .sort([("run_time_utc", -1)])
        .limit(limit)
        .to_list(length=limit)
    )
