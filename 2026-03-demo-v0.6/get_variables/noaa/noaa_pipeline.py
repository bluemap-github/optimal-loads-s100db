from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import argparse
import os
import sys
import time

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from pymongo import ASCENDING, MongoClient


# --------------------- 사용자 설정 ---------------------
SOURCE = "noaa"
MODEL = "gfs"
RESOL = "0p25"

PRODUCT_CODE = "original"   # dataset_code for raw
TYPE_CODE = "fc"            # forecast
ASSET_TYPE = "forecast"     # metadata type

# ✅ 저장할 변수
PARAMS = {
    "UGRD": {"unit": "m/s", "name_en": "Eastward Current",  "stream": "wave", "product_code": "original"},
    "VGRD": {"unit": "m/s", "name_en": "Northward Current", "stream": "wave", "product_code": "original"},
    "WIND": {"unit": "m/s", "name_en": "Wind Speed",       "stream": "wave", "product_code": "original"},
    "WDIR": {"unit": "degree", "name_en": "Wind Direction","stream": "wave", "product_code": "original"},
    "HTSGW": {"unit": "m", "name_en": "Significant Wave Height", "stream": "wave", "product_code": "original"},
    "PERPW": {"unit": "s", "name_en": "Peak Wave Period", "stream": "wave", "product_code": "original"},
    "DIRPW": {"unit": "degree", "name_en": "Peak Wave Direction", "stream": "wave", "product_code": "original"}
}

UTC = timezone.utc

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_ROOT = SCRIPT_DIR / "noaa" / MODEL / TYPE_CODE  # 로컬 저장 루트

# --------------------- 목적지 S3 설정 ---------------------
DST_BUCKET = os.getenv("S3_BUCKET", "optimal-loads")
DST_REGION = os.getenv("AWS_REGION", "ap-northeast-2")
S3_PREFIX_ROOT = f"{SOURCE}/{MODEL}/{TYPE_CODE}"
DELETE_LOCAL_AFTER_UPLOAD = True

# --------------------- 원천 NOAA 공개 버킷 (익명 접근) ---------------------
SRC_BUCKET = os.getenv("NOAA_GFS_BUCKET", "noaa-gfs-bdp-pds")
SRC_REGION = os.getenv("NOAA_GFS_REGION", "us-east-1")

# wave 파일명 도메인: 스샷 기준 global
WAVE_DOMAIN = os.getenv("NOAA_GFS_WAVE_DOMAIN", "global")

# idx 매칭 레벨
IDX_LEVEL_HINT = os.getenv("NOAA_GFS_LEVEL_HINT", "surface")

DEFAULT_HEADERS = {"User-Agent": "Mozilla/5.0 (noaa-pipeline)"}

# --------------------- Mongo 설정 ---------------------
MONGO_URI = os.getenv("MONGO_URI", "")
MONGO_DB = os.getenv("MONGO_DB", "optimal_loads")
MONGO_COL = os.getenv("MONGO_COL", "assets_metadata")
DIR_COL_NAME = os.getenv("MONGO_DIR_COL", "directories")

mongo: Optional[MongoClient] = None
col = None
dir_col = None

ROOT_DIR = f"{SOURCE}/{MODEL}/"  # noaa/gfs/

if MONGO_URI:
    mongo = MongoClient(MONGO_URI)
    col = mongo[MONGO_DB][MONGO_COL]
    dir_col = mongo[MONGO_DB][DIR_COL_NAME]
    try:
        # assets_metadata
        col.create_index([("natural_key", ASCENDING)], unique=True)
        col.create_index([("valid_time_utc", ASCENDING)])
        col.create_index([("run_time_utc", ASCENDING)])
        col.create_index([("valid_key", ASCENDING)])
        col.create_index([("inventory_directory", ASCENDING), ("name", ASCENDING)])

        # directories
        dir_col.create_index([("parent", ASCENDING), ("name", ASCENDING)])
        dir_col.create_index([("last_modified", ASCENDING)])
    except Exception:
        pass
else:
    print("⚠️ MONGO_URI 환경변수가 비어있어서 Mongo 메타 저장은 비활성화됩니다.")


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
        if x:
            out.add(int(x))
    return out


# --------------------- step 스케줄 생성 (GFS 규칙) ---------------------
def build_gfs_steps(max_step: int) -> list[int]:
    # 000~119: 1시간, 120~384: 3시간
    steps = list(range(0, min(119, max_step) + 1, 1))
    if max_step >= 120:
        steps += list(range(120, min(384, max_step) + 1, 3))
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


# --------------------- 목적지 S3 업로드 ---------------------
def build_s3_key(run_dt: datetime, product_code: str, param: str, filename: str) -> str:
    yyyy = run_dt.strftime("%Y")
    mm = run_dt.strftime("%m")
    dd = run_dt.strftime("%d")
    hhZ = f"{run_dt:%H}Z"
    return f"{S3_PREFIX_ROOT}/{yyyy}/{mm}/{dd}/{hhZ}/{product_code}/{param}/{filename}"


def upload_to_s3(s3_client, local_path: Path, s3_key: str) -> None:
    s3_client.upload_file(
        str(local_path),
        DST_BUCKET,
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
        "type": asset_type,
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
                "access": "aws-s3-range-idx",
                "bucket": SRC_BUCKET,
                "domain": WAVE_DOMAIN,
            }
        },

        "inventory_directory": f"{source}/{model}/{run_time:%Y/%Y-%m/%Y-%m-%d/%H}Z/{dataset_code}/{param}/",
        "inventory_name": f"{dataset_code}_{param}_{run_time:%Y%m%d_%H}Z_step{step:03}",
    }

    if s3_key:
        doc["s3"] = {"bucket": DST_BUCKET, "region": DST_REGION, "key": s3_key}

    return doc


def upsert_mongo(doc: dict) -> None:
    if col is None:
        return
    nk = doc["natural_key"]
    col.update_one({"natural_key": nk}, {"$setOnInsert": doc}, upsert=True)
    upsert_directories_from_doc(doc)


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


def build_run_step_window(
    *,
    start_run_utc: str,
    start_step: int,
    end_run_utc: str,
    end_step: int,
    run_hours: set[int],
    max_step: int,
) -> List[Tuple[datetime, List[int]]]:
    """
    (run_time, steps_for_that_run) 목록 생성.
    - run_time은 6시간 간격으로 증가
    - 첫 run은 start_step부터
    - 마지막 run은 end_step까지
    - 중간 run은 전체 steps(0~max_step 규칙) 사용
    """
    s_run = parse_utc(start_run_utc)
    e_run = parse_utc(end_run_utc)

    if s_run > e_run:
        raise SystemExit("❌ start_run_utc 가 end_run_utc 보다 늦습니다.")

    if start_step < 0 or end_step < 0:
        raise SystemExit("❌ step은 0 이상이어야 합니다.")
    if start_step > max_step or end_step > max_step:
        raise SystemExit(f"❌ step은 max_step({max_step}) 이하여야 합니다.")

    out: List[Tuple[datetime, List[int]]] = []

    t = s_run
    while t <= e_run:
        if run_hours and (t.hour not in run_hours):
            t += timedelta(hours=6)
            continue

        steps = build_gfs_steps(max_step)

        if t == s_run:
            steps = [x for x in steps if x >= start_step]
        if t == e_run:
            steps = [x for x in steps if x <= end_step]

        if steps:
            out.append((t, steps))

        t += timedelta(hours=6)

    return out


# --------------------- 원천 AWS S3 (익명) ---------------------
def make_src_s3_unsigned():
    return boto3.client("s3", region_name=SRC_REGION, config=Config(signature_version=UNSIGNED))


def build_wave_object_key(run_dt: datetime, step: int) -> str:
    yyyymmdd = run_dt.strftime("%Y%m%d")
    hh = run_dt.strftime("%H")
    fff = f"{step:03d}"
    fname = f"gfswave.t{hh}z.{WAVE_DOMAIN}.0p25.f{fff}.grib2"
    return f"gfs.{yyyymmdd}/{hh}/wave/gridded/{fname}"


def build_wave_idx_key(run_dt: datetime, step: int) -> str:
    return build_wave_object_key(run_dt, step) + ".idx"


def s3_get_text(s3_client, bucket: str, key: str) -> str:
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read().decode("utf-8", errors="replace")


def s3_download_range_to_bytes(
    s3_client,
    bucket: str,
    key: str,
    start: int,
    end: Optional[int] = None,
) -> bytes:
    if end is None:
        range_header = f"bytes={start}-"
    else:
        range_header = f"bytes={start}-{end}"

    obj = s3_client.get_object(Bucket=bucket, Key=key, Range=range_header)
    return obj["Body"].read()


def list_src_prefix_exists(s3_client, prefix: str) -> bool:
    resp = s3_client.list_objects_v2(Bucket=SRC_BUCKET, Prefix=prefix, MaxKeys=1)
    return bool(resp.get("KeyCount", 0))


# --------------------- idx 파싱 / range 추출 ---------------------
def parse_idx_records(idx_text: str) -> List[Dict[str, Any]]:
    """
    NOAA idx 예시 라인 대략:
    1:0:d=2025070100:UGRD:surface:...
    2:12345:d=2025070100:VGRD:surface:...

    앞의 두 필드:
    - record no
    - byte offset

    이후:
    - d=...
    - variable
    - level
    - 기타 설명...
    """
    rows: List[Dict[str, Any]] = []

    for line in idx_text.splitlines():
        line = line.strip()
        if not line:
            continue

        parts = line.split(":")
        if len(parts) < 5:
            continue

        try:
            rec_no = int(parts[0])
            start = int(parts[1])
        except ValueError:
            continue

        ref_time = parts[2]
        var = parts[3].strip()
        level = parts[4].strip()
        tail = parts[5:] if len(parts) > 5 else []

        rows.append(
            {
                "rec_no": rec_no,
                "start": start,
                "ref_time": ref_time,
                "var": var,
                "level": level,
                "tail": tail,
                "raw": line,
            }
        )

    rows.sort(key=lambda x: x["start"])

    for i in range(len(rows)):
        if i < len(rows) - 1:
            rows[i]["end"] = rows[i + 1]["start"] - 1
        else:
            rows[i]["end"] = None

    return rows


def choose_matching_idx_records(
    records: List[Dict[str, Any]],
    var: str,
    level_hint: str = "surface",
) -> List[Dict[str, Any]]:
    # 1차: var exact + level contains surface
    picked = [
        r for r in records
        if r["var"] == var and level_hint.lower() in (r["level"] or "").lower()
    ]
    if picked:
        return picked

    # 2차: var exact only
    picked = [r for r in records if r["var"] == var]
    if picked:
        return picked

    return []


def download_var_from_s3_idx_range(
    s3_client,
    run_dt: datetime,
    step: int,
    var: str,
    out_path: Path,
) -> None:
    ensure_dirs(out_path.parent)

    grib_key = build_wave_object_key(run_dt, step)
    idx_key = build_wave_idx_key(run_dt, step)

    idx_text = s3_get_text(s3_client, SRC_BUCKET, idx_key)
    records = parse_idx_records(idx_text)
    picked = choose_matching_idx_records(records, var, IDX_LEVEL_HINT)

    if not picked:
        available = sorted({r["var"] for r in records})
        raise RuntimeError(
            f"idx에서 var={var!r} 를 찾지 못했습니다. "
            f"key=s3://{SRC_BUCKET}/{idx_key} "
            f"available_vars={available}"
        )

    with open(out_path, "wb") as f:
        for rec in picked:
            chunk = s3_download_range_to_bytes(
                s3_client=s3_client,
                bucket=SRC_BUCKET,
                key=grib_key,
                start=rec["start"],
                end=rec["end"],
            )
            f.write(chunk)


# --------------------- 메인 ---------------------
def main():
    ap = argparse.ArgumentParser(
        description="NOAA GFS wave → (var별 GRIB2) + S3 + Mongo metadata + directories"
    )
    ap.add_argument("RUN_UTC", nargs="?", help="런 시각 예: 2026-02-02T06:00:00Z")
    ap.add_argument("--start_utc", help="기간 시작(UTC)")
    ap.add_argument("--end_utc", help="기간 끝(UTC)")
    ap.add_argument("--run_hours", default="0,6,12,18")

    ap.add_argument("--max_step", type=int, default=384, help="GFS max_step (기본 384)")
    ap.add_argument("--sleep", type=float, default=0.5, help="요청 간 sleep(초)")
    ap.add_argument("--timeout_sec", type=int, default=60, help="현재 미사용(호환용 유지)")

    ap.add_argument("--no_s3", action="store_true", help="목적지 S3 업로드 비활성화")
    ap.add_argument("--no_mongo", action="store_true", help="Mongo 메타 저장 비활성화")
    ap.add_argument(
        "--use_src_s3_api_check",
        action="store_true",
        help="원천 AWS S3에 object prefix가 있는지 list로 체크",
    )

    # ✅ run+step window 모드
    ap.add_argument("--start_run_utc", help="시작 RUN(UTC) 예: 2025-07-01T00:00:00Z")
    ap.add_argument("--start_step", type=int, help="시작 step (예: 0)")
    ap.add_argument("--end_run_utc", help="끝 RUN(UTC) 예: 2025-07-02T06:00:00Z")
    ap.add_argument("--end_step", type=int, help="끝 step (예: 15)")

    args = ap.parse_args()

    global col, dir_col
    if args.no_mongo:
        col = None
        dir_col = None

    run_hours = parse_run_hours(args.run_hours)

    # (1) run+step window 모드 우선
    if args.start_run_utc and args.end_run_utc and (args.start_step is not None) and (args.end_step is not None):
        run_step_plan = build_run_step_window(
            start_run_utc=args.start_run_utc,
            start_step=args.start_step,
            end_run_utc=args.end_run_utc,
            end_step=args.end_step,
            run_hours=run_hours,
            max_step=args.max_step,
        )
        if not run_step_plan:
            print("⚠️ 실행할 (RUN, STEP) 조합이 0개입니다.")
            return
    else:
        # (2) 기존 run 범위 모드
        run_list = build_run_list(
            run_utc=args.RUN_UTC,
            start_utc=args.start_utc,
            end_utc=args.end_utc,
            run_hours=run_hours,
        )
        if not run_list:
            print("⚠️ 실행할 RUN이 0개입니다.")
            return

        run_step_plan = [(run_dt, build_gfs_steps(args.max_step)) for run_dt in run_list]

    dst_s3 = None
    if not args.no_s3:
        dst_s3 = boto3.client("s3", region_name=DST_REGION)

    src_s3 = make_src_s3_unsigned()

    total_failed = 0

    for run_dt, steps in run_step_plan:
        run_set_dir = get_run_set_dir(run_dt)
        ensure_dirs(run_set_dir)

        print(f"▶ NOAA RUN : {run_dt.isoformat()}")
        print(f"▶ steps_cnt : {len(steps)} (max_step={args.max_step})")
        print(f"▶ Params    : {', '.join(PARAMS.keys())}")
        print(f"▶ Run set   : {run_set_dir}")
        print(f"▶ DST S3    : {'OFF' if args.no_s3 else f's3://{DST_BUCKET}/{S3_PREFIX_ROOT}/...'}")
        print(f"▶ Mongo     : {'OFF' if args.no_mongo else ('ON' if (MONGO_URI and col is not None) else 'OFF (no MONGO_URI)')}")
        print(f"▶ Source    : AWS public S3 + idx range ({WAVE_DOMAIN})")
        print("")

        if args.use_src_s3_api_check:
            yyyymmdd = run_dt.strftime("%Y%m%d")
            hh = run_dt.strftime("%H")
            prefix = f"gfs.{yyyymmdd}/{hh}/wave/gridded/"
            try:
                ok = list_src_prefix_exists(src_s3, prefix)
                print(f"  🔎 SRC S3 check: s3://{SRC_BUCKET}/{prefix} -> {ok}")
            except Exception as e:
                print(f"  ⚠️ SRC S3 check failed: {e}")

        for param, meta in PARAMS.items():
            unit = meta["unit"]
            name_en = meta["name_en"]
            stream = meta.get("stream", "wave")
            product_code = meta.get("product_code", PRODUCT_CODE)

            for step in steps:
                valid_time = run_dt + timedelta(hours=step)
                filename = f"{PRODUCT_CODE}_{param}_{run_dt:%Y%m%d_%H}Z_step{step:03}.grib2"
                out_path = get_out_path(run_set_dir, stream, param, filename)
                ensure_dirs(out_path.parent)

                # 1) 다운로드 (AWS S3 .idx + Range GET)
                if out_path.exists():
                    print(f"⏭️ exists local: {out_path.relative_to(run_set_dir)}")
                else:
                    print(f"⏬ download {param} step={step:03} -> {out_path.relative_to(run_set_dir)}")
                    try:
                        download_var_from_s3_idx_range(
                            s3_client=src_s3,
                            run_dt=run_dt,
                            step=step,
                            var=param,
                            out_path=out_path,
                        )
                    except Exception as e:
                        print(f"  ❌ download failed: {e}")
                        total_failed += 1
                        time.sleep(args.sleep)
                        continue

                # 2) 목적지 S3 업로드
                s3_key = None
                if dst_s3 is not None:
                    try:
                        s3_key = build_s3_key(run_dt, product_code, param, filename)
                        upload_to_s3(dst_s3, out_path, s3_key)
                    except Exception as e:
                        print(f"  ❌ dst s3 upload failed: {e}")
                        total_failed += 1
                        time.sleep(args.sleep)
                        continue

                # 3) 메타 upsert
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

                if col is not None:
                    try:
                        upsert_mongo(raw_doc)
                    except Exception as e:
                        print(f"  ❌ mongo upsert failed: {e}")
                        total_failed += 1

                # 4) 업로드 후 로컬 삭제
                if DELETE_LOCAL_AFTER_UPLOAD and s3_key:
                    try:
                        out_path.unlink(missing_ok=True)
                    except Exception:
                        pass

                time.sleep(args.sleep)

        print("")

    print("========================================")
    print(f"✅ TOTAL failed={total_failed}")
    if total_failed > 0:
        sys.exit(2)


if __name__ == "__main__":
    main()