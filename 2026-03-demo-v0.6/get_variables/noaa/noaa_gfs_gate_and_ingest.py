#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# noaa_gfs_gate_and_ingest.py

from __future__ import annotations

import argparse
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import requests

UTC = timezone.utc

SOURCE = "noaa"
MODEL = "gfs"
TYPE_CODE = "fc"
DATASET = "original"

# NOMADS g2sub (Grib Filter) endpoint for GFS Wave
FILTER_ENDPOINT = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfswave.pl"

# gate로 확인할 대표 변수/스텝(최소 1개면 충분)
GATE_VAR = "UGRD"
GATE_STEP = 0

DEFAULT_HEADERS = {"User-Agent": "Mozilla/5.0 (gate+ingest)"}


def utc_now() -> datetime:
    return datetime.now(tz=UTC)


def iso_z(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def floor_to_6h(dt: datetime) -> datetime:
    h = (dt.hour // 6) * 6
    return dt.replace(hour=h, minute=0, second=0, microsecond=0)


def build_filter_url(run_dt: datetime, step: int, var: str) -> str:
    """
    NOMADS g2sub query:
      file=gfswave.t{HH}z.global.0p25.f{FFF}.grib2
      var_{VAR}=on
      lev_surface=on (대부분 wave vars 포함)
      dir=%2Fgfs.YYYYMMDD%2FHH%2Fwave%2Fgridded
    """
    yyyymmdd = run_dt.strftime("%Y%m%d")
    hh = run_dt.strftime("%H")
    fff = f"{step:03d}"
    file_ = f"gfswave.t{hh}z.global.0p25.f{fff}.grib2"
    dir_ = f"/gfs.{yyyymmdd}/{hh}/wave/gridded"

    # 전지구 (subregion 비활성)
    params = {
        "file": file_,
        f"var_{var}": "on",
        "lev_surface": "on",
        "subregion": "",
        "leftlon": "0",
        "rightlon": "360",
        "toplat": "90",
        "bottomlat": "-90",
        "dir": dir_,
    }

    # requests가 URL encoding 처리
    req = requests.Request("GET", FILTER_ENDPOINT, params=params).prepare()
    return req.url


def probe_run_available(run_dt: datetime, *, timeout_sec: int = 20) -> bool:
    url = build_filter_url(run_dt, GATE_STEP, GATE_VAR)
    try:
        r = requests.get(url, headers=DEFAULT_HEADERS, stream=True, timeout=timeout_sec)
        if r.status_code != 200:
            return False
        # 빈 응답 방지: 최소 몇 KB라도 내려오는지 확인
        got = 0
        for chunk in r.iter_content(chunk_size=8192):
            if not chunk:
                continue
            got += len(chunk)
            if got >= 16 * 1024:
                break
        return got > 0
    except requests.RequestException:
        return False


def main() -> None:
    ap = argparse.ArgumentParser(
        description="NOAA GFS(WAVE) gate: find latest available run, then call ingest script."
    )
    ap.add_argument("--python", default=sys.executable if "sys" in globals() else "python3")
    ap.add_argument("--ingest_script", required=True, help="Path to noaa_gfs_ingest.py")
    ap.add_argument("--lookback_runs", type=int, default=3, help="Check latest N runs (6h cycles)")
    ap.add_argument("--delay_hours", type=int, default=6, help="How many hours behind 'now' to start searching")
    ap.add_argument("--gate_timeout_sec", type=int, default=20)
    ap.add_argument("--extra_args", default="", help='Extra args passed to ingest script (string)')
    ap.add_argument("--trigger", default="cron")
    ap.add_argument("--polite_wait_sec", type=float, default=1.0, help="Wait between gate probes")

    args = ap.parse_args()

    now = utc_now()
    cursor = floor_to_6h(now - timedelta(hours=args.delay_hours))

    candidates = [cursor - timedelta(hours=6 * i) for i in range(args.lookback_runs)]
    chosen: Optional[datetime] = None

    for dt in candidates:
        ok = probe_run_available(dt, timeout_sec=args.gate_timeout_sec)
        print(f"[gate] probe run={iso_z(dt)} var={GATE_VAR} step={GATE_STEP:03d} -> {ok}")
        if ok:
            chosen = dt
            break
        time.sleep(args.polite_wait_sec)

    if not chosen:
        print("[gate] ❌ no available run found in lookback window.")
        return

    cmd = [
        args.python,
        "-u",
        str(Path(args.ingest_script)),
        iso_z(chosen),
        "--trigger",
        args.trigger,
    ]

    if args.extra_args.strip():
        # 문자열로 들어온 extra_args를 쉘 파싱처럼 분해 (간단 split)
        cmd += args.extra_args.strip().split()

    print("[gate] ✅ chosen run:", iso_z(chosen))
    print("[gate] ▶ calling:", " ".join(cmd))

    subprocess.check_call(cmd)


if __name__ == "__main__":
    import sys
    main()