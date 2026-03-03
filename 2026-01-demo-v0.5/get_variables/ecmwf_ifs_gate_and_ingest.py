#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ecmwf_ifs_gate_and_ingest.py

from __future__ import annotations
import argparse
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Tuple

from ecmwf.opendata import Client

UTC = timezone.utc
MODEL = "ifs"
RESOL = "0p25"
TYPE_CODE = "fc"

# gate로 볼 대표 조합 (최소 1개 or 2개)
GATES = [
    ("oper", "10u", 0),
    ("oper", "10v", 0),
    ("wave", "swh", 0),
    ("wave", "mwp", 0),
    ("wave", "mwd", 0),
]

def utc_now() -> datetime:
    return datetime.now(UTC)

def iso_z(dt: datetime) -> str:
    return dt.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def floor_to_6h(dt: datetime) -> datetime:
    h = (dt.hour // 6) * 6
    return dt.replace(hour=h, minute=0, second=0, microsecond=0)

def build_run_candidates(now: datetime, lookback_runs: int, delay_hours: int) -> list[datetime]:
    base = floor_to_6h(now - timedelta(hours=delay_hours))
    return [base - timedelta(hours=6*i) for i in range(lookback_runs)]

def try_gate(client: Client, run_dt: datetime, stream: str, var: str, step: int, sleep: float = 0.1) -> bool:
    tmp = Path(f"/tmp/gate_{stream}_{var}_{run_dt:%Y%m%d%H}_s{step:03}.grib2")
    try:
        client.retrieve(
            date=run_dt.date(),
            type=TYPE_CODE,
            stream=stream,
            time=f"{run_dt:%H}",
            step=int(step),
            param=var,
            target=str(tmp),
        )
        ok = tmp.exists() and tmp.stat().st_size > 0
        return ok
    except Exception:
        return False
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass
        time.sleep(sleep)

def find_latest_available_run(client: Client, candidates: list[datetime], require_all_gates: bool) -> Optional[datetime]:
    for run_dt in candidates:
        results = []
        for stream, var, step in GATES:
            results.append(try_gate(client, run_dt, stream, var, step))
        if require_all_gates:
            if all(results):
                return run_dt
        else:
            if any(results):
                return run_dt
    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lookback_runs", type=int, default=3)
    ap.add_argument("--delay_hours", type=int, default=6, help="런 생성 대비 공개 지연(대략값)")
    ap.add_argument("--require_all_gates", action="store_true", help="oper/wave 둘 다 열려야 ingest 시작")
    ap.add_argument("--source_real", default="ecmwf", help='ingest와 동일 추천: "aws" 또는 "ecmwf"')
    ap.add_argument("--ingest_script", required=True, help="ingest 스크립트 경로")
    ap.add_argument("--python", default="python3", help="python 실행 파일")
    ap.add_argument("--extra_args", default="", help="ingest에 추가로 넘길 인자 문자열")
    args = ap.parse_args()

    now = utc_now()
    candidates = build_run_candidates(now, args.lookback_runs, args.delay_hours)

    client = Client(source=args.source_real, model=MODEL, resol=RESOL)

    run_dt = find_latest_available_run(client, candidates, args.require_all_gates)
    if not run_dt:
        print(f"⏳ gate not opened yet. now={iso_z(now)} candidates={[iso_z(x) for x in candidates]}")
        return

    run_utc = iso_z(run_dt)
    print(f"✅ latest available run={run_utc} → call ingest")

    cmd = [args.python, args.ingest_script, run_utc]
    if args.extra_args.strip():
        cmd += args.extra_args.strip().split()

    # ingest 내부에서 ingestion_control 락이 걸리므로,
    # 이미 돌고 있으면 ingest가 알아서 스킵하고 종료할 것임.
    subprocess.run(cmd, check=False)

if __name__ == "__main__":
    main()
