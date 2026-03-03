#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# emcwf_ifs_ingest.py
"""
ECMWF IFS → GRIB2 + S3 + Mongo assets_metadata (+ derived planned) + directories
+ ✅ ingestion_control (pause/resume + 중복 실행 방지)
+ ✅ ingestion_runs (변수별 run 로그: run_time_utc + variable 1건)

문서 단위:
- ingestion_control: 단일 문서 1개 (_id 고정)
- ingestion_runs: 변수별 1개 (_id = "ecmwf|ifs|fc|<stream>|<var>|run=<ISOZ>")
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import argparse
import os
import sys
import time

import boto3
from ecmwf.opendata import Client
from pymongo import ASCENDING, MongoClient, ReturnDocument


# ✅ stale/heartbeat 설정
STALE_MINUTES = int(os.getenv("INGESTION_STALE_MINUTES", "30"))  # stale 기준(분)

# --------------------- 사용자 설정 ---------------------
SOURCE_REAL = "ecmwf"
SOURCE = "ecmwf"
MODEL = "ifs"
RESOL = "0p25"

PRODUCT_CODE = "original"  # (= dataset_code for raw)
TYPE_CODE = "fc"           # forecast (ECMWF request value)
ASSET_TYPE = "forecast"    # raw/forecast metadata type

# ✅ 변수별 stream + product_code
PARAMS = {
    "10u":  {"unit": "m/s", "name_en": "Eastward velocity vector component at 10 m", "stream": "oper", "product_code": "original"},
    "10v":  {"unit": "m/s", "name_en": "Northward velocity vector component at 10 m", "stream": "oper", "product_code": "original"},
    "swh":  {"unit": "m",   "name_en": "Significant height of combined wind waves and swell", "stream": "wave", "product_code": "original"},
    "mwp":  {"unit": "s",   "name_en": "Mean wave period", "stream": "wave", "product_code": "original"},
    "mwd":  {"unit": "degree", "name_en": "Mean wave direction", "stream": "wave", "product_code": "original"},
}

# ✅ 파생 변수(바람) planned 메타 생성용
DERIVED_VARS = {
    "wind_speed_10m": {
        "unit": "m/s",
        "name_en": "10 metre wind speed",
        "depends_on": ["10u", "10v"],
        "method": "sqrt(u^2 + v^2)",
        "product_code": "computed",
    },
    "wind_dir_10m": {
        "unit": "degree",
        "name_en": "10 metre wind direction (meteorological)",
        "depends_on": ["10u", "10v"],
        "method": "wind_dir_deg = (atan2(-u, -v) * 180/pi + 360) % 360",
        "convention": "meteorological_from",
        "product_code": "computed",
    },
}

UTC = timezone.utc

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_ROOT = SCRIPT_DIR / "ecmwf" / MODEL / TYPE_CODE  # 로컬 저장 루트

# --------------------- S3 설정 ---------------------
BUCKET = os.getenv("S3_BUCKET", "optimal-loads")
REGION = os.getenv("AWS_REGION", "ap-northeast-2")
S3_PREFIX_ROOT = f"ecmwf/{MODEL}/{TYPE_CODE}"
DELETE_LOCAL_AFTER_UPLOAD = True

# --------------------- Mongo 설정 ---------------------
MONGO_URI = os.getenv("MONGO_URI", "")
MONGO_DB = os.getenv("MONGO_DB", "optimal_loads")

# 기존 메타/디렉토리
MONGO_ASSETS_COL = os.getenv("MONGO_COL", "assets_metadata")
MONGO_DIR_COL = os.getenv("MONGO_DIR_COL", "directories")

# ✅ 제어 + 실행 로그
MONGO_CONTROL_COL = os.getenv("MONGO_CONTROL_COL", "ingestion_control")
MONGO_RUNS_COL = os.getenv("MONGO_RUNS_COL", "ingestion_runs")
CONTROL_DOC_ID = os.getenv("CONTROL_DOC_ID", "ecmwf_ifs_ingestion")

mongo: Optional[MongoClient] = None
assets_col = None
dir_col = None
control_col = None
runs_col = None

# directories 구성 범위(루트)
ROOT_DIR = f"{SOURCE}/{MODEL}/"  # ecmwf/ifs/

if MONGO_URI:
    mongo = MongoClient(MONGO_URI)
    assets_col = mongo[MONGO_DB][MONGO_ASSETS_COL]
    dir_col = mongo[MONGO_DB][MONGO_DIR_COL]
    control_col = mongo[MONGO_DB][MONGO_CONTROL_COL]
    runs_col = mongo[MONGO_DB][MONGO_RUNS_COL]

    try:
        # assets_metadata
        assets_col.create_index([("natural_key", ASCENDING)], unique=True)
        assets_col.create_index([("valid_time_utc", ASCENDING)])
        assets_col.create_index([("run_time_utc", ASCENDING)])
        assets_col.create_index([("valid_key", ASCENDING)])
        assets_col.create_index([("inventory_directory", ASCENDING), ("name", ASCENDING)])

        # directories
        dir_col.create_index([("parent", ASCENDING), ("name", ASCENDING)])
        dir_col.create_index([("last_modified", ASCENDING)])

        # ingestion_runs
        runs_col.create_index([("run_time_utc", ASCENDING)])
        runs_col.create_index([("variable", ASCENDING)])
        runs_col.create_index([("stream", ASCENDING)])
        runs_col.create_index([("status", ASCENDING)])
        runs_col.create_index([("updated_at", ASCENDING)])
    except Exception:
        pass
else:
    print("⚠️ MONGO_URI 환경변수가 비어있어서 Mongo 기능(assets/directories/control/runs)은 비활성화됩니다.")


# --------------------- 공통 유틸 ---------------------
def parse_utc(s: str) -> datetime:
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)

def iso_z(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

def utc_now() -> datetime:
    return datetime.now(UTC)

def parse_run_hours(s: str) -> set[int]:
    out: set[int] = set()
    for x in s.split(","):
        x = x.strip()
        if not x:
            continue
        out.add(int(x))
    return out


# --------------------- step 스케줄 생성 (IFS 규칙) ---------------------
def build_ifs_steps(run_hour: int, max_step: int) -> list[int]:
    """
    IFS Open Data 규칙:
    - 00Z/12Z: 0~144 by 3, 150~360 by 6
    - 06Z/18Z: 0~144 by 3
    """
    steps: list[int] = []

    if run_hour in (0, 12):
        s1_end = min(144, max_step)
        steps.extend(range(0, s1_end + 1, 3))
        if max_step >= 150:
            s2_end = min(360, max_step)
            steps.extend(range(150, s2_end + 1, 6))

    elif run_hour in (6, 18):
        s1_end = min(144, max_step)
        steps.extend(range(0, s1_end + 1, 3))
    else:
        steps.extend(range(0, max_step + 1, 3))

    return sorted(set(steps))


# --------------------- 로컬 폴더 구조 ---------------------
def get_run_set_dir(run_dt: datetime) -> Path:
    yyyy = run_dt.strftime("%Y")
    mm = run_dt.strftime("%m")
    dd = run_dt.strftime("%d")
    hhZ = f"{run_dt:%H}Z"
    return DATA_ROOT / yyyy / mm / dd / hhZ

def get_out_path(run_set_dir: Path, stream: str, param: str, filename: str) -> Path:
    return run_set_dir / stream / param / filename

def ensure_dirs(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


# --------------------- S3 업로드 ---------------------
def build_s3_key(run_dt: datetime, product_code: str, param: str, filename: str) -> str:
    yyyy = run_dt.strftime("%Y")
    mm = run_dt.strftime("%m")
    dd = run_dt.strftime("%d")
    hhZ = f"{run_dt:%H}Z"
    return f"{S3_PREFIX_ROOT}/{yyyy}/{mm}/{dd}/{hhZ}/{product_code}/{param}/{filename}"

def upload_to_s3(s3_client, local_path: Path, s3_key: str) -> None:
    s3_client.upload_file(
        str(local_path),
        BUCKET,
        s3_key,
        ExtraArgs={"ContentType": "application/x-grib"},
    )


# --------------------- Mongo 키 ---------------------
def build_keys(
    source: str,
    dataset_code: str,
    model: str,
    asset_type: str,
    stream: str,
    param: str,
    run_time: datetime,
    step: int,
    valid_time: datetime,
) -> tuple[str, str]:
    natural_key = (
        f"{dataset_code}|{source}|{model}|{asset_type}|{stream}|{param}"
        f"|run={iso_z(run_time)}|step={step}"
    )
    valid_key = (
        f"{dataset_code}|{source}|{model}|{asset_type}|{stream}|{param}"
        f"|valid={iso_z(valid_time)}"
    )
    return natural_key, valid_key


# --------------------- directories 유틸 ---------------------
def _norm_dir(d: str) -> str:
    d = (d or "").strip().lstrip("/")
    if d and not d.endswith("/"):
        d += "/"
    return d

def _split_dir(d: str) -> Tuple[Optional[str], str]:
    d = _norm_dir(d)
    if not d:
        return None, ""
    parts = [p for p in d.strip("/").split("/") if p]
    if not parts:
        return None, ""
    name = parts[-1]
    if len(parts) == 1:
        return None, name
    parent = "/".join(parts[:-1]) + "/"
    return parent, name

def upsert_directories_from_doc(doc: Dict[str, Any]) -> None:
    if dir_col is None:
        return

    inv_dir = _norm_dir(doc.get("inventory_directory", ""))
    if not inv_dir or not inv_dir.startswith(ROOT_DIR):
        return

    lm = doc.get("created_at")
    if not isinstance(lm, datetime):
        try:
            lm = parse_utc(str(lm))
        except Exception:
            lm = utc_now()

    now = utc_now()
    cur = inv_dir

    while cur and cur.startswith(ROOT_DIR):
        parent, name = _split_dir(cur)

        # 1) 현재 디렉토리 upsert
        dir_col.update_one(
            {"_id": cur},
            {
                "$setOnInsert": {
                    "dir": cur,
                    "parent": parent,
                    "name": name,
                    "children_dirs": [],
                    "created_at": now,
                },
                "$max": {"last_modified": lm},
                "$set": {"updated_at": now},
            },
            upsert=True,
        )

        # 2) 부모 upsert → children add
        if parent and parent.startswith(ROOT_DIR):
            p_parent, p_name = _split_dir(parent)

            dir_col.update_one(
                {"_id": parent},
                {
                    "$setOnInsert": {
                        "dir": parent,
                        "parent": p_parent,
                        "name": p_name,
                        "children_dirs": [],
                        "created_at": now,
                    },
                    "$max": {"last_modified": lm},
                    "$set": {"updated_at": now},
                },
                upsert=True,
            )

            child_name = name + "/"
            dir_col.update_one(
                {"_id": parent},
                {
                    "$addToSet": {"children_dirs": child_name},
                    "$max": {"last_modified": lm},
                    "$set": {"updated_at": now},
                },
            )

        cur = parent


# --------------------- 메타 doc 빌더 ---------------------
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

        # 표시/식별용은 문자열
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
            "ecmwf": {
                "type": type_code,
                "stream": stream,
                "time": f"{run_time:%H}",
                "step": step,
                "param": param,
                "resol": resol,
            }
        },

        # ✅ 인벤토리 구조
        "inventory_directory": f"{source}/{model}/{run_time:%Y/%Y-%m/%Y-%m-%d/%H}Z/{dataset_code}/{param}/",
        "inventory_name": f"{dataset_code}_{param}_{run_time:%Y%m%d_%H}Z_step{step:03}",
    }

    if s3_key:
        doc["s3"] = {"bucket": BUCKET, "region": REGION, "key": s3_key}

    return doc

def build_derived_doc(
    *,
    source: str,
    dataset_code: str,  # "computed"
    model: str,
    resol: str,
    type_code: str,
    stream: str,
    derived_var: str,
    derived_meta: dict,
    run_time: datetime,
    step: int,
    valid_time: datetime,
) -> dict:
    asset_type = "derived"

    natural_key, valid_key = build_keys(
        source=source,
        dataset_code=dataset_code,
        model=model,
        asset_type=asset_type,
        stream=stream,
        param=derived_var,
        run_time=run_time,
        step=step,
        valid_time=valid_time,
    )

    doc: dict = {
        "source": source,
        "dataset_code": dataset_code,

        "variable": derived_var,
        "name_en": derived_meta.get("name_en", derived_var),
        "unit": derived_meta.get("unit", ""),

        "model": model,
        "type": asset_type,  # "derived"
        "stream": stream,

        "resolution": {"lon_deg": 0.25, "lat_deg": 0.25},

        "run_time_utc": iso_z(run_time),
        "step_hours": step,
        "valid_time_utc": iso_z(valid_time),

        "year": int(valid_time.strftime("%Y")),
        "month": int(valid_time.strftime("%m")),

        "created_at": utc_now(),

        "natural_key": natural_key,
        "valid_key": valid_key,

        "derivation": {
            "depends_on": derived_meta.get("depends_on", []),
            "method": derived_meta.get("method", ""),
        },

        "source_parameters": {
            "ecmwf": {
                "type": type_code,
                "stream": stream,
                "time": f"{run_time:%H}",
                "step": step,
                "resol": resol,
            }
        },

        "status": "planned",

        "inventory_directory": f"{source}/{model}/{run_time:%Y/%Y-%m/%Y-%m-%d/%H}Z/{dataset_code}/{derived_var}/",
        "inventory_name": f"{dataset_code}_{derived_var}_{run_time:%Y%m%d_%H}Z_step{step:03}",
    }

    conv = derived_meta.get("convention")
    if conv:
        doc["derivation"]["convention"] = conv

    return doc


# --------------------- Mongo upsert (assets + directories) ---------------------
def upsert_assets(doc: dict) -> None:
    if assets_col is None:
        return
    nk = doc["natural_key"]
    assets_col.update_one({"natural_key": nk}, {"$setOnInsert": doc}, upsert=True)
    upsert_directories_from_doc(doc)


# --------------------- ingestion_control ---------------------
def ensure_control_doc() -> None:
    """초기 1회 생성 보장(없으면 생성)."""
    if control_col is None:
        return
    now = utc_now()
    control_col.update_one(
        {"_id": CONTROL_DOC_ID},
        {"$setOnInsert": {
            "_id": CONTROL_DOC_ID,
            "enabled": True,
            "running": False,
            "status": "idle",
            "error": None,
            "created_at": now,
            "updated_at": now,

            # ✅ 추가 (운영 안정성)
            "last_started_at": None,
            "last_finished_at": None,
            "last_heartbeat_at": None,
        }},
        upsert=True,
    )

def try_begin_ingestion() -> bool:
    """
    실행 가능하면:
      enabled=true AND running=false 조건을 만족할 때만 running=true로 바꾸고 시작.
    실행 불가면 False.
    """
    if control_col is None:
        return True  # Mongo 비활성화면 제어도 못하니 그냥 진행(로컬 테스트용)

    ensure_control_doc()
    # ✅ stale 자동 정리 먼저
    recover_stale_running() 
    now = utc_now()

    # 1) enabled 확인
    ctl = control_col.find_one({"_id": CONTROL_DOC_ID}, {"enabled": 1, "running": 1})
    if not ctl:
        return True
    if not ctl.get("enabled", True):
        control_col.update_one(
            {"_id": CONTROL_DOC_ID},
            {"$set": {"status": "paused", "updated_at": now}}
        )
        print("⏸️ ingestion_control.enabled=false → 이번 실행은 스킵합니다.")
        return False

    # 2) running=false일 때만 running=true로 바꾸기(중복 실행 방지)
    updated = control_col.find_one_and_update(
        {"_id": CONTROL_DOC_ID, "enabled": True, "running": False},
        {"$set": {
            "running": True,
            "status": "running",
            "last_started_at": now,
            "last_heartbeat_at": now,  # ✅ 추가
            "updated_at": now,
            "error": None
        }},
        return_document=ReturnDocument.AFTER,
    )
    if not updated:
        print("⏭️ 이미 수집이 진행 중(running=true) → 이번 실행은 스킵합니다.")
        return False

    return True

def end_ingestion(overall: str, err_msg: Optional[str] = None, summary: Optional[dict] = None) -> None:
    """성공/실패 상관없이 running=false로 되돌리고 종료 상태 기록."""
    if control_col is None:
        return
    now = utc_now()
    set_doc: dict = {
        "running": False,
        "status": "idle" if overall in ("success", "partial") else "error",
        "last_finished_at": now,
        "updated_at": now,
    }
    if err_msg:
        set_doc["error"] = err_msg[:1000]
    else:
        set_doc["error"] = None
    if summary:
        set_doc["last_run_summary"] = summary

    control_col.update_one({"_id": CONTROL_DOC_ID}, {"$set": set_doc})

def recover_stale_running() -> None:
    """
    running=true인데 last_heartbeat_at(또는 last_started_at)이 너무 오래됐으면
    비정상 종료로 간주하고 running=false로 정리.
    """
    if control_col is None:
        return

    now = utc_now()
    cutoff = now - timedelta(minutes=STALE_MINUTES)

    q = {
        "_id": CONTROL_DOC_ID,
        "running": True,
        "$or": [
            {"last_heartbeat_at": {"$lt": cutoff}},
            {"last_heartbeat_at": None, "last_started_at": {"$lt": cutoff}},
        ],
    }

    upd = {
        "$set": {
            "running": False,
            "status": "error",
            "error": f"stale reset: no heartbeat for {STALE_MINUTES} minutes",
            "last_finished_at": now,
            "updated_at": now,
        }
    }

    res = control_col.update_one(q, upd)
    if res.modified_count > 0:
        print(f"🧹 stale running detected → forced reset (>{STALE_MINUTES}m)")


def heartbeat(note: Optional[str] = None) -> None:
    """실행 중임을 표시(비정상 종료 감지용)."""
    if control_col is None:
        return
    now = utc_now()
    set_doc = {"last_heartbeat_at": now, "updated_at": now}
    if note:
        set_doc["heartbeat_note"] = note  # optional
    control_col.update_one(
        {"_id": CONTROL_DOC_ID, "running": True},
        {"$set": set_doc},
    )

# --------------------- ingestion_runs (변수별 run 로그) ---------------------
MAX_ERRORS = 20

def run_doc_id(run_dt: datetime, stream: str, var: str) -> str:
    return f"{SOURCE}|{MODEL}|{TYPE_CODE}|{stream}|{var}|run={iso_z(run_dt)}"

def make_attempt_id(now: datetime) -> str:
    # 배열 마지막 원소를 찍을 때 안전하게 쓰려고, attempt_id를 "고유값"으로 만들어둠
    # (uuid 써도 되는데 외부 의존 없애려고 iso+ms 정도로)
    return now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

def is_already_success(run_dt: datetime, stream: str, var: str) -> bool:
    """
    A안: status가 success면 이미 완료로 판단.
    (partial/failed이면 재시도 허용)
    """
    if runs_col is None:
        return False
    _id = run_doc_id(run_dt, stream, var)
    doc = runs_col.find_one({"_id": _id}, {"status": 1})
    return bool(doc and doc.get("status") == "success")

def start_var_run_log(
    *,
    run_dt: datetime,
    stream: str,
    var: str,
    dataset_code: str,
    max_step: int,
    steps: list[int],
    trigger: str,
) -> tuple[str, str, datetime]:
    """
    변수별 run 로그 attempt 시작.
    반환: (_id, attempt_id, started_at)
    """
    if runs_col is None:
        now = utc_now()
        return run_doc_id(run_dt, stream, var), make_attempt_id(now), now

    now = utc_now()
    _id = run_doc_id(run_dt, stream, var)
    attempt_id = make_attempt_id(now)

    zero_counters = {"downloaded": 0, "existed": 0, "uploaded": 0, "mongo_written": 0, "failed": 0}
    zero_notes = {"derived_planned_written": 0}

    attempt_doc = {
        "attempt_no": None,  # 나중에 attempts_cnt 기반으로 셋(원하면 아래에서 넣어도 됨)
        "attempt_id": attempt_id,
        "trigger": trigger,
        "status": "running",
        "started_at": now,
        "finished_at": None,
        "duration_sec": None,
        "counters": dict(zero_counters),
        "errors": [],
        "notes": dict(zero_notes),
    }

    # attempts_cnt를 안전하게 증가시키고, 증가된 값으로 attempt_no 부여하려면
    # find_one_and_update로 attempts_cnt를 먼저 올리고 attempt_no 계산하는 방식이 깔끔함.
    # (문서 수 적으니 간단히 2-step 해도 되지만, 원자적으로 하려고 여기서 처리)
    doc_after = runs_col.find_one_and_update(
        {"_id": _id},
        {
            "$setOnInsert": {
                "_id": _id,
                "source": SOURCE,
                "model": MODEL,
                "type_code": TYPE_CODE,
                "dataset_code": dataset_code,
                "stream": stream,
                "variable": var,
                "run_time_utc": iso_z(run_dt),
                "created_at": now,
                "attempts": [],
                # ❌ "attempts_cnt": 0  <-- 이 줄 제거
                "counters_total": {"downloaded": 0, "existed": 0, "uploaded": 0, "mongo_written": 0, "failed": 0},
            },
            "$inc": {"attempts_cnt": 1},   # ✅ 이거 하나로 충분
            "$set": {
                "running": True,
                "status": "running",
                "trigger_last": trigger,
                "schedule": {"max_step": max_step, "steps_cnt": len(steps)},
                "started_at_last": now,
                "finished_at_last": None,
                "duration_sec_last": None,
                "counters_last": dict(zero_counters),
                "notes_last": dict(zero_notes),
                "updated_at": now,
            },
        },
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    attempt_no = int((doc_after or {}).get("attempts_cnt") or 1)

    attempt_doc["attempt_no"] = attempt_no

    # 이제 attempt push
    runs_col.update_one(
        {"_id": _id},
        {
            "$push": {"attempts": attempt_doc},
            "$set": {"updated_at": now},
        },
    )

    return _id, attempt_id, now

def update_var_run_log(
    _id: str,
    attempt_id: str,
    *,
    counters: Optional[dict] = None,
    add_error: Optional[dict] = None,
    inc_notes: Optional[dict] = None,
) -> None:
    """
    진행 중 카운터/에러/노트 업데이트.
    - counters_last 갱신
    - attempts 내 attempt_id 매칭 원소도 갱신
    """
    if runs_col is None:
        return

    now = utc_now()
    upd: dict = {"$set": {"updated_at": now}}

    array_updates: dict = {}
    if counters is not None:
        upd["$set"]["counters_last"] = counters
        array_updates["attempts.$[a].counters"] = counters

    if inc_notes is not None:
        # last는 누적 업데이트(inc)
        upd.setdefault("$inc", {}).update({f"notes_last.{k}": v for k, v in inc_notes.items()})
        # attempt notes도 누적
        upd.setdefault("$inc", {}).update({f"attempts.$[a].notes.{k}": v for k, v in inc_notes.items()})

    if add_error is not None:
        # 문서 상단엔 최근 에러만 남길 수도 있지만,
        # 일단 last_errors 같은 건 안 두고 attempt.errors만 유지하자.
        upd.setdefault("$push", {})["attempts.$[a].errors"] = {"$each": [add_error], "$slice": -MAX_ERRORS}

    if array_updates:
        upd["$set"].update(array_updates)

    runs_col.update_one(
        {"_id": _id},
        upd,
        array_filters=[{"a.attempt_id": attempt_id}],
    )

def finish_var_run_log(
    _id: str,
    attempt_id: str,
    *,
    started_at: datetime,
    status: str,
    counters: dict,
    errors: list[dict],
    notes: dict,
) -> None:
    """
    attempt 종료 + 문서 top-level last_* 갱신.
    - counters_total은 "시도 누적"으로 더함(원하면 나중에 정책 바꿔도 됨)
    """
    if runs_col is None:
        return

    now = utc_now()
    dur = int((now - started_at).total_seconds())

    # status가 success/partial/failed로 최종 확정
    # 문서 top-level status는 "마지막 실행 결과"로 두되,
    # success면 앞으로 is_already_success에서 스킵되도록 유지.
    upd = {
        "$set": {
            "running": False,
            "status": status,
            "updated_at": now,

            "finished_at_last": now,
            "duration_sec_last": dur,
            "counters_last": counters,
            "notes_last": notes,

            # 참고용: 마지막 시도 트리거, started는 start에서 이미 set
        },
        "$inc": {
            "counters_total.downloaded": counters.get("downloaded", 0),
            "counters_total.existed": counters.get("existed", 0),
            "counters_total.uploaded": counters.get("uploaded", 0),
            "counters_total.mongo_written": counters.get("mongo_written", 0),
            "counters_total.failed": counters.get("failed", 0),
        },
        "$setOnInsert": {
            "created_at": now,
        },
    }

    # attempts 내 해당 attempt 마무리
    upd["$set"].update({
        "attempts.$[a].status": status,
        "attempts.$[a].finished_at": now,
        "attempts.$[a].duration_sec": dur,
        "attempts.$[a].counters": counters,
        "attempts.$[a].notes": notes,
        "attempts.$[a].errors": errors[-MAX_ERRORS:],
    })

    runs_col.update_one(
        {"_id": _id},
        upd,
        array_filters=[{"a.attempt_id": attempt_id}],
        upsert=True,
    )



# --------------------- run 리스트 생성 ---------------------
def build_run_list(
    *,
    run_utc: Optional[str],
    start_utc: Optional[str],
    end_utc: Optional[str],
    run_hours: set[int],
) -> List[datetime]:
    if run_utc:
        return [parse_utc(run_utc)]

    if not start_utc or not end_utc:
        raise SystemExit("❌ 기간 모드면 --start_utc, --end_utc 둘 다 필요합니다. 또는 RUN_UTC 하나를 주면 됩니다.")

    start_dt = parse_utc(start_utc)
    end_dt = parse_utc(end_utc)

    runs: List[datetime] = []
    t = start_dt
    while t <= end_dt:
        if t.hour in run_hours:
            runs.append(t)
        t += timedelta(hours=6)
    return runs


def main():
    ap = argparse.ArgumentParser(
        description="ECMWF IFS → GRIB2 + S3 + Mongo metadata (+ derived planned) + directories + ingestion_control + ingestion_runs"
    )

    ap.add_argument("--test_mode", action="store_true", help="테스트 모드: 커서 기반으로 RUN_UTC 1개만 실행")
    ap.add_argument("--test_cursor_start", default="2025-01-01T00:00:00Z", help="테스트 시작 RUN_UTC")
    ap.add_argument("--test_max_runs", type=int, default=1, help="한 번 실행에서 처리할 run 개수(기본 1)")


    ap.add_argument("RUN_UTC", nargs="?", help="런 시각 예: 2025-12-16T00:00:00Z")
    ap.add_argument("--start_utc", help="기간 시작(UTC) 예: 2025-07-01T00:00:00Z")
    ap.add_argument("--end_utc", help="기간 끝(UTC) 예: 2025-11-30T18:00:00Z")
    ap.add_argument("--run_hours", default="0,6,12,18",
                    help="실행할 런 시각(UTC hour) 콤마구분. 예: 0,12 또는 6,18 또는 0,6,12,18")

    ap.add_argument("--max_step_long", type=int, default=360, help="00/12 런 max_step (기본 360)")
    ap.add_argument("--max_step_short", type=int, default=144, help="06/18 런 max_step (기본 144)")
    ap.add_argument("--max_step", type=int, default=None, help="단일 RUN에서 max_step 강제 (기존 호환)")

    ap.add_argument("--sleep", type=float, default=0.2, help="요청 간 sleep(초)")
    ap.add_argument("--no_s3", action="store_true", help="S3 업로드 비활성화")
    ap.add_argument("--no_mongo", action="store_true", help="Mongo 메타 저장 비활성화(assets+directories+control+runs 모두)")
    ap.add_argument("--trigger", default="cron", help="trigger 값(기본 cron)")

    args = ap.parse_args()

    global assets_col, dir_col, control_col, runs_col
    if args.no_mongo:
        assets_col = None
        dir_col = None
        control_col = None
        runs_col = None

    # ✅ ingestion_control로 실행 가능 여부 판정
    if not args.no_mongo:
        can_run = try_begin_ingestion()
        if not can_run:
            return

    # 아래부터는 “실제로 수집을 수행하는 구간”
    overall_err: Optional[str] = None
    overall_status: str = "success"

    # 요약(ingestion_control.last_run_summary에 기록용)
    summary_vars_total = len(PARAMS)
    summary_vars_success = 0
    summary_vars_failed = 0
    summary_run_time_utc: Optional[str] = None
    summary_overall: str = "success"

    try:
        if args.test_mode:
            # step=0만
            args.max_step = 0

            # 커서 읽기(없으면 start로 초기화)
            if control_col is not None:
                ctl = control_col.find_one({"_id": CONTROL_DOC_ID}, {"test_cursor_run_utc": 1})
                cur = (ctl or {}).get("test_cursor_run_utc") or args.test_cursor_start
            else:
                cur = args.test_cursor_start

            run_list = []
            cur_dt = parse_utc(cur)
            for _ in range(args.test_max_runs):
                run_list.append(cur_dt)
                cur_dt = cur_dt + timedelta(hours=6)

            # 커서 저장(다음 실행을 위해)
            if control_col is not None:
                control_col.update_one(
                    {"_id": CONTROL_DOC_ID},
                    {"$set": {"test_cursor_run_utc": iso_z(cur_dt), "updated_at": utc_now()}},
                    upsert=True,
                )

        else:
            run_hours = parse_run_hours(args.run_hours)
            run_list = build_run_list(
                run_utc=args.RUN_UTC,
                start_utc=args.start_utc,
                end_utc=args.end_utc,
                run_hours=run_hours,
            )

        if not run_list:
            print("⚠️ 실행할 RUN이 0개입니다.  (--run_hours / start~end 범위 확인)")
            return

        client = Client(source=SOURCE_REAL, model=MODEL, resol=RESOL)

        s3 = None
        if not args.no_s3:
            s3 = boto3.client("s3", region_name=REGION)

        total_failed = 0

        for run_dt in run_list:
            summary_run_time_utc = iso_z(run_dt)

            # 런별 max_step 결정
            if args.max_step is not None:
                max_step = args.max_step
            else:
                max_step = args.max_step_long if run_dt.hour in (0, 12) else args.max_step_short

            steps = build_ifs_steps(run_dt.hour, max_step)

            run_set_dir = get_run_set_dir(run_dt)
            ensure_dirs(run_set_dir)

            print(f"▶ ECMWF RUN : {run_dt.isoformat()}")
            print(f"▶ run_hour  : {run_dt.hour}Z")
            print(f"▶ max_step  : {max_step}")
            print(f"▶ steps_cnt : {len(steps)}")
            print(f"▶ Params    : {', '.join(PARAMS.keys())}")
            print(f"▶ Run set   : {run_set_dir}")
            print(f"▶ S3        : {'OFF' if args.no_s3 else f's3://{BUCKET}/{S3_PREFIX_ROOT}/...'}")
            print(f"▶ Mongo     : {'OFF' if args.no_mongo else ('ON' if (MONGO_URI and assets_col is not None) else 'OFF (no MONGO_URI)')}")
            print("")

            # derived 메타는 (run, step)당 1번만 생성 가드
            derived_written_steps: set[tuple[str, int]] = set()

            # 변수별 실행
            for param, meta in PARAMS.items():
                unit = meta["unit"]
                name_en = meta["name_en"]
                stream = meta.get("stream", "oper")
                product_code = meta.get("product_code", PRODUCT_CODE)

                # ✅ (추가) 이미 성공한 변수-run이면 스킵
                now = utc_now()
                if is_already_success(run_dt, stream, param):
                    print(f"⏭️ SKIP (already success): {param} ({stream}) run={iso_z(run_dt)} at {now.isoformat()}")
                    summary_vars_success += 1
                    if not args.no_mongo:
                        heartbeat(f"skip var={param} already_success run={iso_z(run_dt)}")
                    continue

                # ✅ (추가) 변수 시작 heartbeat
                if not args.no_mongo:
                    heartbeat(f"start var={param} run={iso_z(run_dt)}")

                # ✅ 변수별 로그 시작
                log_id, attempt_id, started_at = start_var_run_log(
                    run_dt=run_dt,
                    stream=stream,
                    var=param,
                    dataset_code=product_code,
                    max_step=max_step,
                    steps=steps,
                    trigger=args.trigger,
                )


                counters = {"downloaded": 0, "existed": 0, "uploaded": 0, "mongo_written": 0, "failed": 0}
                errors: list[dict] = []
                notes = {"derived_planned_written": 0}

                for step in steps:
                    valid_time = run_dt + timedelta(hours=step)
                    filename = f"{PRODUCT_CODE}_{param}_{run_dt:%Y%m%d_%H}Z_step{step:03}.grib2"
                    out_path = get_out_path(run_set_dir, stream, param, filename)
                    ensure_dirs(out_path.parent)

                    # 1) 다운로드
                    if out_path.exists():
                        counters["existed"] += 1
                    else:
                        try:
                            client.retrieve(
                                date=run_dt.date(),
                                type=TYPE_CODE,
                                stream=stream,
                                time=f"{run_dt:%H}",
                                step=step,
                                param=param,
                                target=str(out_path),
                            )
                            counters["downloaded"] += 1
                        except Exception as e:
                            counters["failed"] += 1
                            if len(errors) < MAX_ERRORS:
                                errors.append({"phase": "retrieve", "step": step, "message": str(e)[:500]})
                            time.sleep(args.sleep)
                            continue

                    # 2) S3 업로드
                    s3_key = None
                    if s3 is not None:
                        try:
                            s3_key = build_s3_key(run_dt, product_code, param, filename)
                            upload_to_s3(s3, out_path, s3_key)
                            counters["uploaded"] += 1
                        except Exception as e:
                            counters["failed"] += 1
                            if len(errors) < MAX_ERRORS:
                                errors.append({"phase": "s3", "step": step, "message": str(e)[:500]})
                            time.sleep(args.sleep)
                            continue

                    # 3) raw 메타 doc 생성 + upsert
                    raw_doc = build_raw_doc(
                        source=SOURCE,
                        dataset_code=PRODUCT_CODE,
                        model=MODEL,
                        resol=RESOL,
                        asset_type=ASSET_TYPE,
                        type_code=TYPE_CODE,
                        stream=stream,
                        param=param,
                        unit=unit,
                        name_en=name_en,
                        run_time=run_dt,
                        step=step,
                        valid_time=valid_time,
                        filename=filename,
                        local_path=out_path,
                        s3_key=s3_key,
                    )

                    if assets_col is not None:
                        try:
                            upsert_assets(raw_doc)
                            counters["mongo_written"] += 1
                        except Exception as e:
                            counters["failed"] += 1
                            if len(errors) < MAX_ERRORS:
                                errors.append({"phase": "mongo", "step": step, "message": str(e)[:500]})

                    # 3.5) 파생 planned 메타 (run+step 당 1번)
                    if stream == "oper" and param in ("10u", "10v"):
                        step_key = (iso_z(run_dt), step)
                        if step_key not in derived_written_steps:
                            for dvar, dmeta in DERIVED_VARS.items():
                                ddoc = build_derived_doc(
                                    source=SOURCE,
                                    dataset_code="computed",
                                    model=MODEL,
                                    resol=RESOL,
                                    type_code=TYPE_CODE,
                                    stream="oper",
                                    derived_var=dvar,
                                    derived_meta=dmeta,
                                    run_time=run_dt,
                                    step=step,
                                    valid_time=valid_time,
                                )
                                if assets_col is not None:
                                    try:
                                        upsert_assets(ddoc)
                                        counters["mongo_written"] += 1
                                        notes["derived_planned_written"] += 1
                                    except Exception as e:
                                        counters["failed"] += 1
                                        if len(errors) < MAX_ERRORS:
                                            errors.append({"phase": "mongo", "step": step, "message": f"derived:{dvar} {str(e)[:480]}"})

                            derived_written_steps.add(step_key)

                    # 5) 업로드 후 로컬 삭제
                    if DELETE_LOCAL_AFTER_UPLOAD and s3_key:
                        try:
                            out_path.unlink(missing_ok=True)
                        except Exception:
                            pass

                    time.sleep(args.sleep)

                # ✅ 변수별 로그 종료 status 결정
                succeeded_any = (counters["downloaded"] + counters["existed"] + counters["uploaded"] + counters["mongo_written"]) > 0
                if counters["failed"] == 0:
                    var_status = "success"
                else:
                    var_status = "partial" if succeeded_any else "failed"

                finish_var_run_log(
                    log_id,
                    attempt_id,
                    started_at=started_at,
                    status=var_status,
                    counters=counters,
                    errors=errors,
                    notes=notes,
                )

                if not args.no_mongo:
                    heartbeat(f"done var={param} status={var_status} run={iso_z(run_dt)}")  

                # 요약 집계
                if var_status == "success":
                    summary_vars_success += 1
                else:
                    summary_vars_failed += 1
                    total_failed += counters["failed"]

                print(f"  ✅ VAR Done {param} ({stream}) status={var_status} counters={counters}")

            print("")

        # overall 결정(간단 규칙)
        if summary_vars_failed == 0:
            summary_overall = "success"
        elif summary_vars_success > 0:
            summary_overall = "partial"
        else:
            summary_overall = "failed"

        overall_status = summary_overall

        if total_failed > 0 and overall_status == "failed":
            # 스크립트 exit code를 실패로 주고 싶으면 아래 주석 해제
            sys.exit(2)
            pass

    except Exception as e:
        overall_status = "failed"
        overall_err = str(e)
        raise
    finally:
        if not args.no_mongo:
            summary = {
                "run_time_utc": summary_run_time_utc,
                "overall": overall_status,
                "vars_total": summary_vars_total,
                "vars_success": summary_vars_success,
                "vars_failed": summary_vars_failed,
            }
            end_ingestion(overall=overall_status, err_msg=overall_err, summary=summary)


if __name__ == "__main__":
    main()
