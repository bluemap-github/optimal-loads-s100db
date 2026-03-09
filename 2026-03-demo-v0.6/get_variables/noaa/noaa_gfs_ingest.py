#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# noaa_gfs_ingest.py
"""
NOAA GFS (Wave / gridded / 0p25) → (var별 GRIB2) + S3 + Mongo assets_metadata + directories
+ ✅ ingestion_control (pause/resume + 중복 실행 방지)
+ ✅ ingestion_runs

- step 규칙 (GFS-Wave): 000~119 hourly, 120~384 3-hourly  :contentReference[oaicite:2]{index=2}
- 다운로드는 NOMADS g2sub Grib Filter(filter_gfswave.pl)로 var 단위 subset 다운로드  :contentReference[oaicite:3]{index=3}
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import boto3
import requests
from pymongo import MongoClient, Return

UTC = timezone.utc

# --------------------- 모델/소스 고정 ---------------------
SOURCE = "noaa"
MODEL = "gfs"
RESOL = "0p25"
TYPE_CODE = "fc"
DATASET_CODE = "original"  # product_code와 동일하게 쓸 예정

# GFS Wave g2sub endpoint
FILTER_ENDPOINT = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfswave.pl"

# 저장할 변수(요청하신 그대로)
PARAMS: Dict[str, Dict[str, str]] = {
    "UGRD": {"unit": "m/s", "name_en": "Eastward Current"},
    "VGRD": {"unit": "m/s", "name_en": "Northward Current"},
    "WIND": {"unit": "m/s", "name_en": "Wind Speed"},
    "WDIR": {"unit": "degree", "name_en": "Wind Direction"},
    "HTSGW": {"unit": "m", "name_en": "Significant Wave Height"},
    "PERPW": {"unit": "s", "name_en": "Peak Wave Period"},
    "DIRPW": {"unit": "degree", "name_en": "Peak Wave Direction"},
}

# stream은 조회 편의상 wave로 고정(원하면 "oper"처럼 바꿔도 됨)
STREAM = "wave"

DEFAULT_HEADERS = {"User-Agent": "Mozilla/5.0 (gfs-ingest)"}

# --------------------- S3 설정 ---------------------
BUCKET = os.getenv("S3_BUCKET", "optimal-loads")
REGION = os.getenv("AWS_REGION", "ap-northeast-2")

# ✅ S3 경로 루트 (요청하신 형태)
# 예: optimal-loads/noaa/gfs/fc/2026/02/02/06Z/original/UGRD/original_UGRD_20260202_06Z_step000.grib2
S3_PREFIX_ROOT = f"{SOURCE}/{MODEL}/{TYPE_CODE}"

DELETE_LOCAL_AFTER_UPLOAD = True

# --------------------- Mongo 설정 ---------------------
MONGO_URI = os.getenv("MONGO_URI", "")
MONGO_DB = os.getenv("MONGO_DB", "optimal_loads")

MONGO_ASSETS_COL = os.getenv("MONGO_COL", "assets_metadata")
MONGO_DIR_COL = os.getenv("MONGO_DIR_COL", "directories")
MONGO_CONTROL_COL = os.getenv("MONGO_CONTROL_COL", "ingestion_control")
MONGO_RUNS_COL = os.getenv("MONGO_RUNS_COL", "ingestion_runs")

CONTROL_DOC_ID = os.getenv("CONTROL_DOC_ID", "noaa_gfs_ingestion")
INGESTION_STALE_MINUTES = int(os.getenv("INGESTION_STALE_MINUTES", "120"))

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_ROOT = SCRIPT_DIR / SOURCE / MODEL / TYPE_CODE  # 로컬 저장 루트


# --------------------- 공통 유틸 ---------------------
def utc_now() -> datetime:
    return datetime.now(tz=UTC)


def iso_z(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_utc(s: str) -> datetime:
    # "2026-02-02T06:00:00Z"
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)


def ensure_dirs(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)


def build_gfs_steps(max_step: int = 384) -> List[int]:
    """
    000~119: 1시간, 120~384: 3시간
    """
    steps = list(range(0, 120))  # 0..119
    steps += list(range(120, max_step + 1, 3))  # 120..384 step=3
    # 혹시 120이 중복되면 set으로 정리
    steps = sorted(set(steps))
    return steps


def get_run_set_dir(run_dt: datetime) -> Path:
    yyyy = run_dt.strftime("%Y")
    mm = run_dt.strftime("%m")
    return DATA_ROOT / yyyy / mm


def get_out_path(run_set_dir: Path, product_code: str, param: str, filename: str) -> Path:
    # 로컬도 S3와 동일한 레이아웃 유지
    yyyy = run_dt.strftime("%Y") if (run_dt := run_set_dir) else ""  # dummy (아래에서 다시)
    return run_set_dir / "tmp" / product_code / param / filename  # run_set_dir 자체가 yyyy/mm라서 단순화


def build_s3_key(run_dt: datetime, product_code: str, param: str, filename: str) -> str:
    yyyy = run_dt.strftime("%Y")
    mm = run_dt.strftime("%m")
    dd = run_dt.strftime("%d")
    hhZ = f"{run_dt:%H}Z"
    return f"{S3_PREFIX_ROOT}/{yyyy}/{mm}/{dd}/{hhZ}/{product_code}/{param}/{filename}"


def upload_to_s3(s3, local_path: Path, s3_key: str) -> None:
    s3.upload_file(str(local_path), BUCKET, s3_key)


def build_keys(
    *,
    source: str,
    dataset_code: str,
    model: str,
    asset_type: str,
    stream: str,
    param: str,
    run_time: datetime,
    step: int,
    valid_time: datetime,
) -> Tuple[str, str]:
    # ECMWF와 동일한 key 철학 유지
    natural_key = (
        f"{dataset_code}|{source}|{model}|{asset_type}|{stream}|{param}|"
        f"run={iso_z(run_time)}|step={step:03d}"
    )
    valid_key = (
        f"{dataset_code}|{source}|{model}|{asset_type}|{stream}|{param}|"
        f"valid={iso_z(valid_time)}"
    )
    return natural_key, valid_key


def _norm_dir(d: str) -> str:
    return d.replace("\\", "/").strip("/").rstrip("/") + "/"


def _split_dir(d: str) -> Tuple[str, str]:
    d = _norm_dir(d)
    if "/" not in d.strip("/"):
        return "", d
    parent = "/".join(d.strip("/").split("/")[:-1]) + "/"
    name = d.strip("/").split("/")[-1] + "/"
    return parent, name


def upsert_directories_from_doc(db, doc: dict) -> None:
    """
    assets_metadata의 inventory_directory를 기준으로 directories 컬렉션에 트리 upsert
    """
    inv_dir = doc.get("inventory_directory")
    if not inv_dir:
        return

    inv_dir = _norm_dir(inv_dir)
    # e.g. noaa/gfs/2026/2026-02/2026-02-02/06Z/original/UGRD/
    parts = inv_dir.strip("/").split("/")
    # 점진적으로 상위부터 생성
    cur = ""
    for i in range(len(parts)):
        cur = "/".join(parts[: i + 1]) + "/"
        parent, name = _split_dir(cur)
        _id = cur

        db[MONGO_DIR_COL].find_one_and_update(
            {"_id": _id},
            {
                "$setOnInsert": {
                    "_id": _id,
                    "dir": cur,
                    "name": name.strip("/"),
                    "parent": parent,
                    "created_at": utc_now(),
                },
                "$set": {
                    "updated_at": utc_now(),
                },
                "$addToSet": {
                    "children": "/".join(parts[: i + 2]) + "/" if i + 1 < len(parts) else None
                },
            },
            upsert=True,
            return_document=Return.DOCUMENT_AFTER,
        )
    # 마지막 addToSet의 None 정리(선택)
    db[MONGO_DIR_COL].update_many({"children": None}, {"$pull": {"children": None}})


def build_raw_doc(
    *,
    source: str,
    dataset_code: str,
    model: str,
    resol: str,
    asset_type: str,
    type_code: str,
    stream: str,
    param: str,
    unit: str,
    name_en: str,
    run_time: datetime,
    step: int,
    valid_time: datetime,
    filename: str,
    local_path: Path,
    s3_key: Optional[str],
) -> dict:
    size_bytes = local_path.stat().st_size
    natural_key, valid_key = build_keys(
        source=source,
        dataset_code=dataset_code,
        model=model,
        asset_type=asset_type,
        stream=stream,
        param=param,
        run_time=run_time,
        step=step,
        valid_time=valid_time,
    )

    doc: dict = {
        "source": source,
        "dataset_code": dataset_code,

        "variable": param,
        "name_en": name_en,
        "unit": unit,

        "model": model,
        "type": asset_type,  # "forecast"
        "stream": stream,

        "resolution": {"lon_deg": 0.25, "lat_deg": 0.25},

        "run_time_utc": iso_z(run_time),
        "step_hours": step,
        "valid_time_utc": iso_z(valid_time),

        "year": int(valid_time.strftime("%Y")),
        "month": int(valid_time.strftime("%m")),

        "name": filename,
        "format": "grib2",
        "content_type": "application/x-grib",
        "size_bytes": size_bytes,

        "created_at": utc_now(),

        "natural_key": natural_key,
        "valid_key": valid_key,

        "source_parameters": {
            "noaa": {
                "type": type_code,
                "stream": stream,
                "time": f"{run_time:%H}",
                "step": step,
                "param": param,
                "resol": resol,
                "endpoint": "nomads-g2sub",
            }
        },

        # ✅ 인벤토리 구조 (ECMWF와 동일 철학)
        "inventory_directory": f"{source}/{model}/{run_time:%Y/%Y-%m/%Y-%m-%d/%H}Z/{dataset_code}/{param}/",
        "inventory_name": f"{dataset_code}_{param}_{run_time:%Y%m%d_%H}Z_step{step:03}",
    }

    if s3_key:
        doc["s3"] = {"bucket": BUCKET, "region": REGION, "key": s3_key}

    return doc


def upsert_assets(db, docs: List[dict]) -> None:
    if not docs:
        return
    col = db[MONGO_ASSETS_COL]
    for d in docs:
        col.find_one_and_update(
            {"natural_key": d["natural_key"]},
            {"$setOnInsert": d, "$set": {"updated_at": utc_now()}},
            upsert=True,
        )


# --------------------- ingestion_control ---------------------
def ensure_control_doc(db) -> dict:
    col = db[MONGO_CONTROL_COL]
    doc = col.find_one({"_id": CONTROL_DOC_ID})
    if doc:
        return doc
    col.insert_one(
        {
            "_id": CONTROL_DOC_ID,
            "paused": False,
            "running": False,
            "running_since": None,
            "heartbeat_at": None,
            "attempt_id": None,
            "updated_at": utc_now(),
            "created_at": utc_now(),
        }
    )
    return col.find_one({"_id": CONTROL_DOC_ID})


def try_begin_ingestion(db, attempt_id: str) -> bool:
    col = db[MONGO_CONTROL_COL]
    now = utc_now()
    doc = col.find_one({"_id": CONTROL_DOC_ID})
    if not doc:
        doc = ensure_control_doc(db)

    if doc.get("paused"):
        print(f"⏸️ paused: control_doc={CONTROL_DOC_ID}")
        return False

    if doc.get("running"):
        # stale 체크
        hb = doc.get("heartbeat_at")
        if hb and isinstance(hb, datetime):
            age = (now - hb).total_seconds() / 60.0
            if age > INGESTION_STALE_MINUTES:
                print(f"⚠️ stale running detected (age={age:.1f}m). recovering...")
            else:
                print("⏳ already running; skip this trigger")
                return False

    updated = col.find_one_and_update(
        {"_id": CONTROL_DOC_ID, "$or": [{"running": False}, {"running": {"$exists": False}}]},
        {
            "$set": {
                "running": True,
                "running_since": now,
                "heartbeat_at": now,
                "attempt_id": attempt_id,
                "updated_at": now,
            }
        },
        upsert=True,
        return_document=Return.DOCUMENT_AFTER,
    )
    return bool(updated)


def heartbeat(db, attempt_id: str) -> None:
    db[MONGO_CONTROL_COL].update_one(
        {"_id": CONTROL_DOC_ID, "attempt_id": attempt_id},
        {"$set": {"heartbeat_at": utc_now(), "updated_at": utc_now()}},
    )


def end_ingestion(db, attempt_id: str) -> None:
    db[MONGO_CONTROL_COL].update_one(
        {"_id": CONTROL_DOC_ID, "attempt_id": attempt_id},
        {"$set": {"running": False, "attempt_id": None, "updated_at": utc_now()}},
    )


# --------------------- ingestion_runs (변수별 run 로그) ---------------------
def run_doc_id(run_time: datetime, stream: str, var: str) -> str:
    return f"{SOURCE}|{MODEL}|{TYPE_CODE}|{stream}|{var}|run={iso_z(run_time)}"


def is_already_success(db, run_time: datetime, stream: str, var: str) -> bool:
    doc = db[MONGO_RUNS_COL].find_one({"_id": run_doc_id(run_time, stream, var)})
    return bool(doc and doc.get("status") == "success")


def start_var_run_log(db, run_time: datetime, stream: str, var: str, attempt_id: str) -> None:
    _id = run_doc_id(run_time, stream, var)
    db[MONGO_RUNS_COL].find_one_and_update(
        {"_id": _id},
        {
            "$setOnInsert": {"_id": _id, "created_at": utc_now()},
            "$set": {
                "source": SOURCE,
                "model": MODEL,
                "type_code": TYPE_CODE,
                "stream": stream,
                "variable": var,
                "run_time_utc": iso_z(run_time),
                "attempt_id": attempt_id,
                "status": "running",
                "updated_at": utc_now(),
            },
        },
        upsert=True,
    )


def finish_var_run_log(db, run_time: datetime, stream: str, var: str, status: str, message: str = "") -> None:
    _id = run_doc_id(run_time, stream, var)
    db[MONGO_RUNS_COL].update_one(
        {"_id": _id},
        {"$set": {"status": status, "message": message, "updated_at": utc_now()}},
        upsert=True,
    )


# --------------------- NOAA 다운로드 ---------------------
def build_filter_url(run_dt: datetime, step: int, var: str) -> str:
    yyyymmdd = run_dt.strftime("%Y%m%d")
    hh = run_dt.strftime("%H")
    fff = f"{step:03d}"
    file_ = f"gfswave.t{hh}z.global.0p25.f{fff}.grib2"
    dir_ = f"/gfs.{yyyymmdd}/{hh}/wave/gridded"

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
    req = requests.Request("GET", FILTER_ENDPOINT, params=params).prepare()
    return req.url


def download_var_step(
    *,
    run_dt: datetime,
    step: int,
    var: str,
    out_path: Path,
    timeout_sec: int,
    polite_wait_sec: float,
) -> None:
    ensure_dirs(out_path)
    url = build_filter_url(run_dt, step, var)

    r = requests.get(url, headers=DEFAULT_HEADERS, stream=True, timeout=timeout_sec)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}: {url} :: {r.text[:120]}")

    with open(out_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

    # g2sub netiquette: 루프 요청시 대기 권장 :contentReference[oaicite:4]{index=4}
    if polite_wait_sec > 0:
        time.sleep(polite_wait_sec)


# --------------------- main ---------------------
def main() -> None:
    ap = argparse.ArgumentParser(description="NOAA GFS(WAVE) ingest → var별 GRIB2 저장 + S3 + Mongo metadata")
    ap.add_argument("RUN_UTC", help="런 시각 예: 2026-02-02T06:00:00Z")
    ap.add_argument("--trigger", default="manual")
    ap.add_argument("--timeout_sec", type=int, default=60)
    ap.add_argument("--polite_wait_sec", type=float, default=1.0, help="requests 사이 대기(서버 보호)")
    ap.add_argument("--max_step", type=int, default=384)
    ap.add_argument("--skip_if_success", action="store_true", default=True)
    ap.add_argument("--no_delete_local", action="store_true", help="업로드 후 로컬 파일 삭제하지 않음")

    args = ap.parse_args()
    run_dt = parse_utc(args.RUN_UTC)

    if not MONGO_URI:
        raise SystemExit("❌ MONGO_URI 가 비어있습니다. .env_runtime 확인 필요")

    attempt_id = f"{CONTROL_DOC_ID}|{args.trigger}|{iso_z(utc_now())}"

    mongo = MongoClient(MONGO_URI)
    db = mongo[MONGO_DB]

    ensure_control_doc(db)
    if not try_begin_ingestion(db, attempt_id):
        return

    s3 = boto3.client("s3", region_name=REGION)

    try:
        steps = build_gfs_steps(max_step=args.max_step)
        print(f"▶ run={iso_z(run_dt)} steps={len(steps)} vars={list(PARAMS.keys())}")

        # 변수별 run 로그는 "var 단위"로 한 번만 시작/종료(중간 실패는 message로 남김)
        for var in PARAMS.keys():
            if args.skip_if_success and is_already_success(db, run_dt, STREAM, var):
                print(f"⏭️ already success: run={iso_z(run_dt)} var={var}")
                continue
            start_var_run_log(db, run_dt, STREAM, var, attempt_id)

        run_set_dir = get_run_set_dir(run_dt)

        # step × var 다운로드 + 업로드 + 메타
        for i, step in enumerate(steps):
            heartbeat(db, attempt_id)

            valid_time = run_dt + timedelta(hours=step)
            yyyy = run_dt.strftime("%Y")
            mm = run_dt.strftime("%m")
            dd = run_dt.strftime("%d")
            hhZ = f"{run_dt:%H}Z"

            for var, meta in PARAMS.items():
                # var가 이미 success라면 스킵(중간 재실행 최적화)
                if args.skip_if_success and is_already_success(db, run_dt, STREAM, var):
                    continue

                filename = f"{DATASET_CODE}_{var}_{run_dt:%Y%m%d_%H}Z_step{step:03}.grib2"

                # 로컬 저장 (프로젝트 내부)
                local_path = run_set_dir / dd / hhZ / DATASET_CODE / var / filename
                if local_path.exists() and local_path.stat().st_size > 0:
                    print(f"⏭️ exists local: {local_path.relative_to(SCRIPT_DIR)}")
                else:
                    print(f"⏬ download var={var} step={step:03} -> {local_path.relative_to(SCRIPT_DIR)}")
                    download_var_step(
                        run_dt=run_dt,
                        step=step,
                        var=var,
                        out_path=local_path,
                        timeout_sec=args.timeout_sec,
                        polite_wait_sec=args.polite_wait_sec,
                    )

                # S3 업로드
                s3_key = build_s3_key(run_dt, DATASET_CODE, var, filename)
                print(f"☁️ upload -> s3://{BUCKET}/{s3_key}")
                upload_to_s3(s3, local_path, s3_key)

                # 메타 생성 + upsert
                doc = build_raw_doc(
                    source=SOURCE,
                    dataset_code=DATASET_CODE,
                    model=MODEL,
                    resol=RESOL,
                    asset_type="forecast",
                    type_code=TYPE_CODE,
                    stream=STREAM,
                    param=var,
                    unit=meta["unit"],
                    name_en=meta["name_en"],
                    run_time=run_dt,
                    step=step,
                    valid_time=valid_time,
                    filename=filename,
                    local_path=local_path,
                    s3_key=s3_key,
                )
                upsert_assets(db, [doc])
                upsert_directories_from_doc(db, doc)

                # 로컬 삭제
                if DELETE_LOCAL_AFTER_UPLOAD and (not args.no_delete_local):
                    try:
                        local_path.unlink(missing_ok=True)
                    except Exception:
                        pass

            if (i + 1) % 10 == 0:
                print(f"… progress step {i+1}/{len(steps)}")

        # var run 로그 성공 처리
        for var in PARAMS.keys():
            if args.skip_if_success and is_already_success(db, run_dt, STREAM, var):
                continue
            finish_var_run_log(db, run_dt, STREAM, var, "success")

        print("✅ done")

    except Exception as e:
        # var run 로그 실패 처리(전부 fail로 찍되 message 남김)
        msg = str(e)[:500]
        for var in PARAMS.keys():
            finish_var_run_log(db, run_dt, STREAM, var, "failed", msg)
        raise

    finally:
        end_ingestion(db, attempt_id)


if __name__ == "__main__":
    main()