#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ECMWF IFS Open Data "업로드 타이밍" 모니터링 probe (Client 기반)
- 10분마다 실행(크론)
- 최신 run 후보(6시간 단위)들을 대상으로
  (stream, variable, step) 조합에 대해 Client.retrieve()를 "시도"만 함
- 성공하면 found=true / 실패하면 found=false 로 ingestion_when 컬렉션에 기록

중요:
- 실제 파일은 /tmp 로 받아서 즉시 삭제함 (저장 목적 아님)
- 네 ingestion 스크립트와 동일한 방식(Client)으로 "있나/없나"를 확인하는 게 목표
"""

from __future__ import annotations

import argparse
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pymongo import MongoClient, ASCENDING
from ecmwf.opendata import Client

UTC = timezone.utc

SOURCE_REAL = "ecmwf"  # ✅ 핵심: aws가 아니라 ecmwf
SOURCE = "ecmwf"
MODEL = "ifs"
RESOL = "0p25"
TYPE_CODE = "fc"

STREAM_VARS = {
    "oper": ["10u", "10v"],
    "wave": ["swh", "mwp", "mwd"],
}

# ---- Mongo ----
MONGO_URI = os.getenv("MONGO_URI", "")
MONGO_DB = os.getenv("MONGO_DB", "optimal_loads")
MONGO_WHEN_COL = os.getenv("MONGO_WHEN_COL", "ingestion_when")

mongo = MongoClient(MONGO_URI) if MONGO_URI else None
when_col = mongo[MONGO_DB][MONGO_WHEN_COL] if mongo else None

if when_col is not None:
    try:
        when_col.create_index([("run_time_utc", ASCENDING)])
        when_col.create_index([("stream", ASCENDING), ("variable", ASCENDING), ("step_hours", ASCENDING)])
        when_col.create_index([("checked_at", ASCENDING)])
        when_col.create_index([("found", ASCENDING)])
    except Exception:
        pass


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso_z(dt: datetime) -> str:
    return dt.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_utc(s: str) -> datetime:
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def floor_to_6h(dt: datetime) -> datetime:
    h = (dt.hour // 6) * 6
    return dt.replace(hour=h, minute=0, second=0, microsecond=0)


def build_run_candidates(now: datetime, lookback_runs: int) -> List[datetime]:
    base = floor_to_6h(now)
    return [base - timedelta(hours=6 * i) for i in range(lookback_runs)]


def doc_id(run_dt: datetime, stream: str, var: str, step: int) -> str:
    return f"{SOURCE}|{MODEL}|{TYPE_CODE}|{stream}|{var}|run={iso_z(run_dt)}|step={step}"


def upsert_probe_result(
    *,
    run_dt: datetime,
    stream: str,
    var: str,
    step: int,
    found: bool,
    checked_at: datetime,
    latency_ms: int,
    detail: Dict[str, Any],
    error: Optional[str] = None,
) -> None:
    if when_col is None:
        print(f"[probe] run={iso_z(run_dt)} stream={stream} var={var} step={step} found={found} {latency_ms}ms")
        if error:
            print(f"  error={error}")
        return

    _id = doc_id(run_dt, stream, var, step)
    now = checked_at

    prev = when_col.find_one({"_id": _id}, {"found": 1, "first_found_at": 1})
    first_found_at = (prev or {}).get("first_found_at")

    set_doc: Dict[str, Any] = {
        "source": SOURCE,
        "model": MODEL,
        "type_code": TYPE_CODE,
        "stream": stream,
        "variable": var,
        "run_time_utc": iso_z(run_dt),
        "step_hours": int(step),

        "checked_at": now,
        "found": bool(found),

        "probe": {
            "method": "ecmwf.opendata.Client.retrieve",
            "source_real": SOURCE_REAL,
            "latency_ms": int(latency_ms),
            "detail": detail,
        },

        "error": error,
        "updated_at": now,
    }

    if found and not first_found_at and not ((prev or {}).get("found") is True):
        set_doc["first_found_at"] = now

    when_col.update_one(
        {"_id": _id},
        {
            "$setOnInsert": {"_id": _id, "created_at": now},
            "$set": set_doc,
        },
        upsert=True,
    )


def try_retrieve_probe(client: Client, run_dt: datetime, stream: str, var: str, step: int, timeout_sec: int) -> Tuple[bool, int, Dict[str, Any], Optional[str]]:
    """
    실제로 retrieve 시도.
    - target은 /tmp에 저장했다가 즉시 삭제
    - 성공: found=True
    - 실패: found=False + error 메시지 저장
    """
    t0 = time.time()
    tmp = Path(f"/tmp/probe_{var}_{run_dt:%Y%m%d%H}_step{step:03}.grib2")

    detail = {
        "date": str(run_dt.date()),
        "type": TYPE_CODE,
        "stream": stream,
        "time": f"{run_dt:%H}",
        "step": int(step),
        "param": var,
        "resol": RESOL,
    }

    try:
        # Client.retrieve 내부에서 요청을 던짐
        client.retrieve(
            date=run_dt.date(),
            type=TYPE_CODE,
            stream=stream,
            time=f"{run_dt:%H}",
            step=int(step),
            param=var,
            target=str(tmp),
        )

        found = tmp.exists() and tmp.stat().st_size > 0
        return found, int((time.time() - t0) * 1000), detail, None

    except Exception as e:
        # opendata가 없을 때 흔히 404/Not Found류가 여기로 옴
        return False, int((time.time() - t0) * 1000), detail, str(e)[:800]

    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass


def main():
    ap = argparse.ArgumentParser(description="Probe ECMWF IFS availability via ecmwf.opendata.Client and store results.")
    ap.add_argument("--lookback_runs", type=int, default=6, help="최신 후보부터 몇 개 run 확인(기본 6=36시간)")
    ap.add_argument("--steps", default="0", help="확인할 step_hours(콤마). 기본 0")
    ap.add_argument("--timeout", type=int, default=20, help="retrieve에 사용할 timeout 성격(참고용)")
    ap.add_argument("--sleep", type=float, default=0.2, help="요청 간 sleep(초)")
    args = ap.parse_args()

    steps = [int(x.strip()) for x in args.steps.split(",") if x.strip()]
    now = utc_now()
    run_list = build_run_candidates(now, args.lookback_runs)

    print(f"▶ probe_when(Client) now={iso_z(now)} runs={len(run_list)} steps={steps} source_real={SOURCE_REAL}")

    client = Client(source=SOURCE_REAL, model=MODEL, resol=RESOL)

    for run_dt in run_list:
        for stream, vars_ in STREAM_VARS.items():
            for var in vars_:
                for step in steps:
                    found, ms, detail, err = try_retrieve_probe(client, run_dt, stream, var, step, timeout_sec=args.timeout)
                    upsert_probe_result(
                        run_dt=run_dt,
                        stream=stream,
                        var=var,
                        step=step,
                        found=found,
                        checked_at=utc_now(),
                        latency_ms=ms,
                        detail=detail,
                        error=err,
                    )
                    print(f"  - run={iso_z(run_dt)} stream={stream} var={var} step={step} found={found} {ms}ms")
                    time.sleep(args.sleep)

    print("✅ probe_when done.")


if __name__ == "__main__":
    main()
