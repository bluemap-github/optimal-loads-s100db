# api.py
from __future__ import annotations

from fastapi import APIRouter, Query, HTTPException
from typing import Optional, List
from datetime import datetime, timezone, timedelta

from app.models import GridDataResponse
from app.db import get_assets_collection, get_directories_collection


import numpy as np
import xarray as xr
import tempfile, os, contextlib, boto3

# ---- env / S3 ----
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-2")
S3_BUCKET  = os.getenv("S3_BUCKET", "optimal-loads")
s3 = boto3.client("s3", region_name=AWS_REGION)


# =============================================================================
# 소스별 변수명 ALIASES
# =============================================================================

# ECMWF 변수명 매핑
ALIASES_ECMWF = {
    "10u": ["10u", "u10"],
    "10v": ["10v", "v10"],
    "swh": ["swh"],
}

# NOAA 변수명 매핑
ALIASES_NOAA = {
    "UGRD": ["u"],
    "VGRD": ["v"],
    "WIND": ["ws"],
    "WDIR": ["wdir"],
    "HTSGW": ["swh"],
    "PERPW": ["perpw"],
    "DIRPW": ["dirpw"],
}


def get_aliases_by_source(source: str) -> dict:
    """소스에 따른 ALIASES 딕셔너리 반환"""
    source_lower = source.lower()
    if source_lower == "noaa":
        return ALIASES_NOAA
    elif source_lower == "ecmwf":
        return ALIASES_ECMWF
    else:
        return ALIASES_ECMWF  # 기본값


# =============================================================================
# bbox limits & cell budget
# =============================================================================
BBOX_LIMITS = {
    "min_lon": -118.8389887,
    "min_lat":  -52.3232408,
    "max_lon":  194.4699022,
    "max_lat":   69.7861536,
}
MAX_CELLS = 5_250_000
ASSUMED_DLON = 0.083
ASSUMED_DLAT = 0.083


# =============================================================================
# Router
# =============================================================================
# router (griddata용)
router = APIRouter(prefix="/api", tags=["grid"])

# meta_router (sources, variables용)
meta_router = APIRouter(prefix="/api", tags=["meta"])

@router.get(
    "/griddata",
    response_model=GridDataResponse,
    summary="Get gridded variable data",
    description="Reads a GRIB2/NetCDF from S3 (by forecast run+step) and returns encoded grid values."
)
async def get_griddata(
    # ---- forecast identity (필수) ----
    source: str = Query(..., example="ecmwf"),
    dataset_code: str = Query(..., example="original"),
    model: str = Query(..., example="ifs"),
    variable: str = Query(..., example="swh"),
    run_time_utc: str = Query(..., example="2025-07-16T00:00:00Z"),
    step_hours: int = Query(..., ge=0, le=360, example=24),

    # ---- spatial (방식 1: 중심점 + 버퍼) ----
    lat: Optional[float] = Query(default=None, example=35.0, description="중심점 위도"),
    lon: Optional[float] = Query(default=None, example=129.0, description="중심점 경도"),
    buffer_km: Optional[float] = Query(default=None, ge=0.0, le=500.0, example=50.0, description="버퍼 반경(km)"),
    
    # ---- spatial (방식 2: 북서-남동 모서리) ----
    nw_lon: Optional[float] = Query(default=None, example=128.0, description="북서(좌상단) 경도"),
    nw_lat: Optional[float] = Query(default=None, example=36.0, description="북서(좌상단) 위도"),
    se_lon: Optional[float] = Query(default=None, example=130.0, description="남동(우하단) 경도"),
    se_lat: Optional[float] = Query(default=None, example=34.0, description="남동(우하단) 위도"),
    ) -> GridDataResponse:
    
    type_ = "forecast"
    
    # ---- bbox 생성 로직 ----
    # 우선순위: 1) nw/se 모서리 → 2) 중심점+버퍼 → 3) 전체 영역
    
    # 방식 1: 북서-남동 모서리로 bbox 생성
    nw_se_params = [nw_lon, nw_lat, se_lon, se_lat]
    center_buffer_params = [lat, lon, buffer_km]
    
    if all(p is not None for p in nw_se_params):
        # nw/se 파라미터가 모두 제공됨
        if any(p is not None for p in center_buffer_params):
            raise HTTPException(
                status_code=422,
                detail={
                    "error": "Cannot use both nw/se and lat/lon/buffer parameters simultaneously.",
                    "hint": "Use either (nw_lon, nw_lat, se_lon, se_lat) OR (lat, lon, buffer_km)"
                }
            )
        
        # nw = (min_lon, max_lat), se = (max_lon, min_lat)
        effective_bbox = [
            nw_lon,  # min_lon
            se_lat,  # min_lat
            se_lon,  # max_lon
            nw_lat   # max_lat
        ]
    
    # 방식 2: 중심점 + 버퍼로 bbox 생성 (기존 로직)
    elif lat is not None and lon is not None:
        # buffer_km이 None이면 기본값 50.0 사용
        buffer = buffer_km if buffer_km is not None else 50.0
        
        if buffer == 0:
            # ✅ 단일 포인트: 가장 가까운 격자점 1개만
            buffer_deg_lat = 0.001
            buffer_deg_lon = 0.001
        else:
            # km → degree 변환
            buffer_deg_lat = buffer / 111.0
            buffer_deg_lon = buffer / (111.0 * np.cos(np.radians(lat)))
            
            # 최소 버퍼 보장 (buffer_km > 0일 때만)
            min_buffer_deg = 0.125  # 0.25° / 2
            buffer_deg_lat = max(buffer_deg_lat, min_buffer_deg)
            buffer_deg_lon = max(buffer_deg_lon, min_buffer_deg / np.cos(np.radians(lat)))
        
        effective_bbox = [
            lon - buffer_deg_lon,  # min_lon
            lat - buffer_deg_lat,  # min_lat
            lon + buffer_deg_lon,  # max_lon
            lat + buffer_deg_lat   # max_lat
        ]
    
    # 방식 3: 전체 영역 (파라미터 없음)
    else:
        effective_bbox = [
            BBOX_LIMITS["min_lon"], BBOX_LIMITS["min_lat"],
            BBOX_LIMITS["max_lon"], BBOX_LIMITS["max_lat"],
        ]

    _validate_bbox_limits_raw(effective_bbox)
    
    # ---- 변수 정규화 (소스별) ----
    norm_var = _norm_var(variable, source)
    
    # ---- valid_time 계산 (응답용) ----
    run_dt = _parse_utc(run_time_utc)
    valid_dt = run_dt + timedelta(hours=int(step_hours))
    valid_time_utc = _to_z(valid_dt)
    
    # ========== computed wind: U/V 성분 계산 ==========
    if norm_var in ("wind_speed_10m", "wind_dir_10m"):
        coll = await get_assets_collection()
        
        # ✅ 소스별로 U/V 변수명이 다름
        if source.lower() == "noaa":
            u_var = "UGRD"
            v_var = "VGRD"
        elif source.lower() == "ecmwf":
            u_var = "10u"
            v_var = "10v"
        else:
            u_var = "10u"
            v_var = "10v"
        
        doc_u = await _find_by_natural_key(
            coll,
            source=source,
            dataset_code="original",
            model=model,
            type_=type_,
            variable=u_var,
            run_time_utc=_to_z(run_dt),
            step_hours=step_hours
        )
        doc_v = await _find_by_natural_key(
            coll,
            source=source,
            dataset_code="original",
            model=model,
            type_=type_,
            variable=v_var,
            run_time_utc=_to_z(run_dt),
            step_hours=step_hours
        )
        
        if not (doc_u and doc_v):
            raise HTTPException(
                status_code=404,
                detail={
                    "error": "Required U/V components not found for computed wind at this run+step.",
                    "needed": [u_var, v_var],
                    "source": source,
                    "run_time_utc": _to_z(run_dt),
                    "step_hours": int(step_hours)
                }
            )
        
        # bbox cell budget 체크
        est = _estimate_cells_from_doc_or_assume(effective_bbox, doc_u)
        if est > MAX_CELLS:
            raise HTTPException(
                status_code=413,
                detail={
                    "error": "Requested bbox is too large for this dataset resolution.",
                    "requested_bbox": effective_bbox,
                    "estimated_cells": est,
                    "limit": MAX_CELLS,
                }
            )

        # s3 key 검증
        if "s3" not in doc_u or "key" not in doc_u.get("s3", {}):
            raise HTTPException(status_code=409, detail={"error": f"{u_var} metadata has no s3.key"})
        if "s3" not in doc_v or "key" not in doc_v.get("s3", {}):
            raise HTTPException(status_code=409, detail={"error": f"{v_var} metadata has no s3.key"})

        tmpu = tempfile.NamedTemporaryFile(delete=False, suffix=_tmp_suffix_from_doc(doc_u))
        tmpv = tempfile.NamedTemporaryFile(delete=False, suffix=_tmp_suffix_from_doc(doc_v))
        ds_u = ds_v = None
        try:
            # S3에서 다운로드
            s3.download_file(S3_BUCKET, doc_u["s3"]["key"], tmpu.name)
            s3.download_file(S3_BUCKET, doc_v["s3"]["key"], tmpv.name)
            
            # Dataset 열기
            ds_u = _open_dataset_safely(tmpu.name)
            ds_v = _open_dataset_safely(tmpv.name)
            
            # 소스별 정규화 (좌표 체계 통일)
            ds_u = _normalize_grib_coordinates(ds_u, source)
            ds_v = _normalize_grib_coordinates(ds_v, source)
            
            # 소스별 변수명으로 선택
            da_u, lat_inc_u, _ = _select_da(ds_u, u_var, effective_bbox, source=source)
            da_v, lat_inc_v, _ = _select_da(ds_v, v_var, effective_bbox, source=source)
            
            da_u = _ensure_lat_lon_names(da_u)
            da_v = _ensure_lat_lon_names(da_v)
            
            # 좌표 정합 (격자 불일치 해결)
            da_u, da_v = xr.align(da_u, da_v, join="inner")
            
            # 바람 속도/방향 계산
            speed = np.hypot(da_u.values, da_v.values)
            direc = (270.0 - np.degrees(np.arctan2(da_v.values, da_u.values))) % 360.0

            target = speed if norm_var == "wind_speed_10m" else direc
            da_like = da_u  # 좌표/차원 템플릿

            arr2, dlon, dlat, width, height = _prepare_array_for_response(
                xr.DataArray(target, coords=da_like.coords, dims=da_like.dims),
                lat_inc_u
            )

            if norm_var == "wind_speed_10m":
                unit_meta, name_en_meta, std_name_meta = "m s-1", "10 m wind speed", "wind_speed_10m"
            else:
                unit_meta, name_en_meta, std_name_meta = "degree", "10 m wind direction (from)", "wind_from_direction"

            arr_flat = arr2.astype(np.float32).flatten()
            data_list = [None if np.isnan(x) else float(x) for x in arr_flat]

            return {
                "timestamp": valid_time_utc,
                "run_time_utc": _to_z(run_dt),
                "step_hours": int(step_hours),
                "valid_time_utc": valid_time_utc,

                "variable": norm_var,
                "unit": unit_meta,
                "name_en": name_en_meta,
                "standard_name": std_name_meta,

                "bbox": [
                    float(da_like["lon"].values.min()),
                    float(da_like["lat"].values.min()),
                    float(da_like["lon"].values.max()),
                    float(da_like["lat"].values.max()),
                ],
                "resolution": [dlon, dlat],
                "shape": [width, height],
                "indexOrder": "row-major-bottom-up",
                "valueEncoding": {"type": "float32", "scale": 1.0, "offset": 0.0, "nodata": None},
                "data": data_list,
            }
        finally:
            with contextlib.suppress(Exception):
                tmpu.close(); os.unlink(tmpu.name)
                tmpv.close(); os.unlink(tmpv.name)
                ds_u and ds_u.close()
                ds_v and ds_v.close()

    # ========== original variable (S3 GRIB2/NC) ==========
    coll = await get_assets_collection()
    
    doc = await _find_by_natural_key(
        coll,
        source=source,
        dataset_code=dataset_code,
        model=model,
        type_=type_,
        variable=norm_var,
        run_time_utc=_to_z(run_dt),
        step_hours=step_hours
    )
    if not doc:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "No matching dataset found for this run+step.",
                "source": source,
                "dataset_code": dataset_code,
                "model": model,
                "type": type_,
                "variable": norm_var,
                "run_time_utc": _to_z(run_dt),
                "step_hours": int(step_hours),
            }
        )

    est = _estimate_cells_from_doc_or_assume(effective_bbox, doc)
    if est > MAX_CELLS:
        raise HTTPException(
            status_code=413,
            detail={
                "error": "Requested bbox is too large for this dataset resolution.",
                "requested_bbox": effective_bbox,
                "estimated_cells": est,
                "limit": MAX_CELLS,
            }
        )

    if "s3" not in doc or "key" not in doc.get("s3", {}):
        raise HTTPException(
            status_code=409,
            detail={"error": "This metadata record has no s3.key", "doc_keys": list(doc.keys())}
        )

    s3_key = doc["s3"]["key"]
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=_tmp_suffix_from_doc(doc))
    ds = None
    try:
        s3.download_file(S3_BUCKET, s3_key, tmp.name)
        ds = _open_dataset_safely(tmp.name)
        
        # 소스별 좌표 정규화
        ds = _normalize_grib_coordinates(ds, source)


        # 소스 정보 전달
        da, lat_inc, _ = _select_da(ds, norm_var, effective_bbox, source=source)
        da = _ensure_lat_lon_names(da)

        arr2, dlon, dlat, width, height = _prepare_array_for_response(da, lat_inc)

        unit_meta = doc.get("unit")
        name_en_meta = doc.get("name_en")
        std_name_meta = doc.get("standard_name")

        arr_flat = arr2.astype(np.float32).flatten()
        data_list = [None if np.isnan(x) else float(x) for x in arr_flat]

        return {
            "timestamp": valid_time_utc,
            "run_time_utc": _to_z(run_dt),
            "step_hours": int(step_hours),
            "valid_time_utc": valid_time_utc,

            "variable": norm_var,
            "unit": unit_meta,
            "name_en": name_en_meta,
            "standard_name": std_name_meta,

            "bbox": [
                float(da["lon"].values.min()), 
                float(da["lat"].values.min()),
                float(da["lon"].values.max()),
                float(da["lat"].values.max()),
            ],
            "resolution": [dlon, dlat],
            "shape": [width, height],
            "indexOrder": "row-major-bottom-up",
            "valueEncoding": {"type": "float32", "scale": 1.0, "offset": 0.0, "nodata": None},
            "data": data_list
        }
    finally:
        with contextlib.suppress(Exception):
            tmp.close(); os.unlink(tmp.name)
            ds and ds.close()


# ----------------- Web-ECDIS 응답용 ----------------- 






# =============================================================================
# GET /api/sources
# STEP 2 진입 시 — ecmwf, noaa 등 source 목록 반환
# directories 컬렉션에서 parent가 None인 최상위 폴더들 조회
# =============================================================================

@meta_router.get("/sources", summary="사용 가능한 데이터 소스 목록")
async def get_sources():
    """
    directories 컬렉션의 최상위 폴더(_id 예: "ecmwf/", "noaa/")를 반환.
    parent가 None인 문서들 = 루트 바로 아래 source 폴더들.
    """
    coll = await get_directories_collection()

    docs = await coll.find(
        {"parent": None},
        {"_id": 1, "name": 1}
    ).to_list(length=100)

    if not docs:
        raise HTTPException(status_code=404, detail="No sources found in directories.")

    # 변경
    MODEL_MAP = {
        "ecmwf": "ifs",
        "noaa": "gfs",
    }

    sources = [
        {
            "source": (name := doc.get("name") or doc["_id"].strip("/")),
            "model": MODEL_MAP.get(name)
        }
        for doc in docs
    ]

    return {"sources": sources}
    # 응답 예시:
    # { "sources": [{"source": "ecmwf"}, {"source": "noaa"}] }


# =============================================================================
# GET /api/variables?source=ecmwf
# STEP 3 진입 시 — 해당 source의 변수 목록 반환
# directories에서 xxZ/ 하위의 original/, computed/ 문서들을 찾아
# children_dirs를 union해서 변수 목록 구성
# original-all-steps/, s100/ 는 regex에서 자동 제외됨
# =============================================================================

@meta_router.get("/variables", summary="source별 사용 가능한 변수 목록")
async def get_variables(source: str):
    """
    경로 패턴: {source}/{model}/yyyy/yyyy-mm/yyyy-mm-dd/xxZ/(original|computed)/
    해당 패턴에 매칭되는 디렉토리 문서들의 children_dirs를 모아서 변수 목록 반환.

    - original/   → dataset_code: "original"
    - computed/   → dataset_code: "computed"
    - original-all-steps/, s100/ 등은 regex 패턴에서 자동 제외
    """
    source_lower = source.strip().lower()

    # source에 따라 model 결정
    # ecmwf → ifs,  noaa → gfs  (추후 다른 모델 생기면 여기서 확장)
    MODEL_MAP = {
        "ecmwf": "ifs",
        "noaa": "gfs",
    }
    model = MODEL_MAP.get(source_lower)
    if not model:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown source '{source}'. Available: {list(MODEL_MAP.keys())}"
        )

    coll = await get_directories_collection()

    # 패턴: ecmwf/ifs/2026/2026-03/2026-03-08/00Z/original/
    #        ecmwf/ifs/2026/2026-03/2026-03-08/00Z/computed/
    # → original-all-steps, s100 등은 매칭 안 됨
    pattern = (
        rf"^{source_lower}/{model}/"       # ecmwf/ifs/
        r"\d{4}/"                          # 2026/
        r"\d{4}-\d{2}/"                    # 2026-03/
        r"\d{4}-\d{2}-\d{2}/"             # 2026-03-08/
        r"\d{2}Z/"                         # 00Z/
        r"(original|computed)/$"           # original/ 또는 computed/ 만
    )

    docs = await coll.find(
        {"_id": {"$regex": pattern}},
        {"_id": 1, "children_dirs": 1}
    ).to_list(length=10000)

    if not docs:
        raise HTTPException(
            status_code=404,
            detail=f"No variable directories found for source='{source}'."
        )

    # dataset_code별로 변수 수집
    original_vars: set[str] = set()
    computed_vars: set[str] = set()

    for doc in docs:
        path: str = doc["_id"]          # ex) "ecmwf/ifs/2026/.../original/"
        children: list = doc.get("children_dirs", [])

        if "/original/" in path:
            for child in children:
                original_vars.add(child.rstrip("/"))
        elif "/computed/" in path:
            for child in children:
                computed_vars.add(child.rstrip("/"))

    # 응답 구성 — original 먼저, computed 뒤에
    variables = (
        [{"variable": v, "dataset_code": "original"} for v in sorted(original_vars)]
        + [{"variable": v, "dataset_code": "computed"} for v in sorted(computed_vars)]
    )

    return {
        "source": source_lower,
        "model": model,
        "variables": variables,
    }
    # 응답 예시:
    # {
    #   "source": "ecmwf",
    #   "model": "ifs",
    #   "variables": [
    #     {"variable": "10u", "dataset_code": "original"},
    #     {"variable": "10v", "dataset_code": "original"},
    #     {"variable": "swh", "dataset_code": "original"},
    #     {"variable": "wind_speed_10m", "dataset_code": "computed"}
    #   ]
    # }

# =============================================================================
# Helpers - 변수명 처리
# =============================================================================

def resolve_var(ds: xr.Dataset, var: str, source: str = "ecmwf") -> str:
    """
    소스별 변수명 별칭을 실제 데이터셋에 있는 변수명으로 해석
    """
    aliases = get_aliases_by_source(source)
    candidates = aliases.get(var, [var])
    
    for name in candidates:
        if name in ds.data_vars:
            return name
    
    if var in ds.data_vars:
        return var
    
    raise KeyError(
        f"Variable '{var}' not found in {source} dataset. "
        f"Tried: {candidates}. Available: {list(ds.data_vars)}"
    )


def _norm_var(v: str, source: str = "ecmwf") -> str:
    low = v.strip().lower()
    source_lower = source.lower()

    # ✅ NOAA 변수명을 먼저 처리 (WDIR이 wind_dir_10m으로 오변환되는 것 방지)
    if source_lower == "noaa":
        for key in ALIASES_NOAA.keys():
            if low == key.lower():
                return key
        for key, aliases in ALIASES_NOAA.items():
            if low in [a.lower() for a in aliases]:
                return key

    # Computed 바람 (NOAA 원본 변수명 처리 후에만 도달)
    if low in ("wind_speed_10m", "ws", "ws10", "spd", "wind"):
        return "wind_speed_10m"
    if low in ("wind_dir_10m", "wdir", "wdir10", "dir", "wd"):
        return "wind_dir_10m"

    # ECMWF 변수명 정규화
    if source_lower == "ecmwf":
        if low in ("u10", "10u"):
            return "10u"
        if low in ("v10", "10v"):
            return "10v"
        if low in ("swh", "significant_wave_height", "hs"):
            return "swh"

    return v.strip()


# =============================================================================
# Helpers - 좌표 정규화
# =============================================================================

def _normalize_grib_coordinates(ds: xr.Dataset, source: str) -> xr.Dataset:
    """
    소스별로 다른 GRIB 좌표 체계를 ECMWF 스타일(-180~180)로 통일
    """
    ds = _normalize_lonlat(ds)  # longitude/latitude → lon/lat
    
    if "lon" not in ds.coords:
        return ds
    
    lon_vals = ds["lon"].values
    
    # 1. 경도 체계 확인 및 변환 (0~360 → -180~180)
    if lon_vals.max() > 180:
        ds = ds.assign_coords(lon=(ds["lon"] + 180) % 360 - 180)
        ds = ds.sortby("lon")
    
    # 2. NOAA: 부동소수점 오차 보정
    if source.lower() == "noaa":
        lon_corrected = np.round(ds["lon"].values / 0.25) * 0.25
        lat_corrected = np.round(ds["lat"].values / 0.25) * 0.25
        
        ds = ds.assign_coords({
            "lon": lon_corrected,
            "lat": lat_corrected
        })
    
    return ds


def _normalize_lonlat(ds: xr.Dataset) -> xr.Dataset:
    """longitude/latitude → lon/lat 이름 변경"""
    rename_map = {}
    if "longitude" in ds.coords and "lon" not in ds.coords:
        rename_map["longitude"] = "lon"
    if "latitude" in ds.coords and "lat" not in ds.coords:
        rename_map["latitude"] = "lat"
    return ds.rename(rename_map) if rename_map else ds


# =============================================================================
# Helpers - 마스킹 처리
# =============================================================================

def _handle_grib_masking(da: xr.DataArray, source: str, fill_strategy: str = "nan") -> xr.DataArray:
    """
    소스별 마스킹 처리 (numpy.ma.MaskedArray → 일반 배열)
    """
    if not hasattr(da.values, 'mask'):
        return da
    
    if fill_strategy == "nan":
        filled_values = np.where(da.values.mask, np.nan, da.values.data)
    elif fill_strategy == "zero":
        filled_values = np.where(da.values.mask, 0, da.values.data)
    else:
        filled_values = da.values.filled(np.nan)
    
    return xr.DataArray(
        filled_values,
        coords=da.coords,
        dims=da.dims,
        attrs=da.attrs
    )


# =============================================================================
# Helpers - 데이터 선택
# =============================================================================

def _select_da(ds: xr.Dataset, var: str, bbox: Optional[List[float]], source: str = "ecmwf"):
    """
    소스별 처리를 추가한 DataArray 선택
    """
    # 변수명 해석 (소스별 ALIASES 적용)
    var2 = resolve_var(ds, var, source)
    da = ds[var2]
    
    # 마스킹 처리
    da = _handle_grib_masking(da, source, fill_strategy="nan")
    
    if "time" in da.dims and da.sizes.get("time", 1) == 1:
        da = da.isel(time=0)
    
    # ✅ 기본값 설정 (전체 데이터셋)
    lon_vals = ds["lon"].values
    lat_vals = ds["lat"].values
    lon_inc = bool(lon_vals[1] > lon_vals[0]) if lon_vals.size > 1 else True
    lat_inc = bool(lat_vals[1] > lat_vals[0]) if lat_vals.size > 1 else True
    
    if bbox and len(bbox) == 4:
        min_lon, min_lat, max_lon, max_lat = map(float, bbox)
        
        # ✅ bbox가 매우 작으면 (단일 포인트) nearest 사용
        bbox_width = max_lon - min_lon
        bbox_height = max_lat - min_lat
        
        if bbox_width < 0.01 and bbox_height < 0.01:
            # 단일 포인트: 중심점의 가장 가까운 격자 선택
            center_lon = (min_lon + max_lon) / 2
            center_lat = (min_lat + max_lat) / 2
            da = da.sel(lon=center_lon, lat=center_lat, method='nearest')
            
            # ✅ 단일 포인트 후처리: 차원 유지
            if 'lon' not in da.dims and 'lat' not in da.dims:
                # 0차원 스칼라 → 1x1 배열로 변환
                selected_lon = float(da['lon'].values)
                selected_lat = float(da['lat'].values)
                da = da.expand_dims({'lon': [selected_lon], 'lat': [selected_lat]})
        else:
            # 범위 선택
            lon_slice = slice(min_lon, max_lon) if lon_inc else slice(max_lon, min_lon)
            lat_slice = slice(min_lat, max_lat) if lat_inc else slice(max_lat, min_lat)
            da = da.sel(lon=lon_slice, lat=lat_slice)
    
    return da, lat_inc, lon_inc

def _ensure_lat_lon_names(da: xr.DataArray) -> xr.DataArray:
    """latitude/longitude → lat/lon 이름 변경"""
    rename_map = {}
    if "latitude" in da.dims:
        rename_map["latitude"] = "lat"
    if "longitude" in da.dims:
        rename_map["longitude"] = "lon"
    return da.rename(rename_map) if rename_map else da


def _prepare_array_for_response(da: xr.DataArray, lat_inc: bool):
    """
    응답용 배열 준비 (row-major-bottom-up 형식)
    """
    da2 = da.transpose("lat", "lon")
    lat_vals = da2["lat"].values
    lon_vals = da2["lon"].values
    arr2 = da2.values

    # 위도가 감소하는 경우 뒤집기 (bottom-up)
    if not lat_inc:
        arr2 = arr2[::-1, :]
        lat_vals = lat_vals[::-1]

    # 해상도 계산 (단일 포인트 안전 처리)
    if lon_vals.size > 1:
        dlon = float(abs(np.mean(np.diff(lon_vals))))
    else:
        dlon = 0.25  # 기본 해상도 (ECMWF 0.25°)
    
    if lat_vals.size > 1:
        dlat = float(abs(np.mean(np.diff(lat_vals))))
    else:
        dlat = 0.25
    
    h, w = arr2.shape
    return arr2, dlon, dlat, w, h

# =============================================================================
# Helpers - 파일 및 시간
# =============================================================================

def _parse_utc(dt_str: str) -> datetime:
    """ISO 8601 문자열을 UTC datetime으로 파싱"""
    s = dt_str.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s).astimezone(timezone.utc)


def _to_z(dt: datetime) -> str:
    """datetime을 ISO 8601 Z 형식 문자열로 변환"""
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


async def _find_by_natural_key(
    coll,
    *,
    source: str,
    dataset_code: str,
    model: str,
    type_: str,
    variable: str,
    run_time_utc: str,
    step_hours: int,
):
    """MongoDB에서 자연키로 문서 찾기"""
    return await coll.find_one({
        "source": source,
        "dataset_code": dataset_code,
        "model": model,
        "type": type_,
        "variable": variable,
        "run_time_utc": run_time_utc,
        "step_hours": int(step_hours),
    })


def _tmp_suffix_from_doc(doc: dict) -> str:
    """문서 메타데이터에서 파일 확장자 추정"""
    fmt = (doc.get("format") or "").lower()
    ctype = (doc.get("content_type") or "").lower()
    name = (doc.get("name") or "").lower()

    if "grib" in fmt or "grib" in ctype or name.endswith((".grib2", ".grib")):
        return ".grib2"
    if "netcdf" in fmt or name.endswith((".nc", ".netcdf")):
        return ".nc"
    return ".bin"


def _open_dataset_safely(path: str) -> xr.Dataset:
    """cfgrib 또는 h5netcdf로 Dataset 열기"""
    last_err = None
    for engine in ("cfgrib", "h5netcdf"):
        try:
            return xr.open_dataset(path, engine=engine)
        except Exception as e:
            last_err = e
    raise RuntimeError(f"Failed to open dataset: {last_err}")


# ============================================================================= 
# Helpers - bbox 검증 및 셀 추정
# =============================================================================

def _validate_bbox_limits_raw(bbox: Optional[List[float]]):
    """bbox가 허용 범위 내에 있는지 검증"""
    if not bbox:
        return
    if len(bbox) != 4:
        raise HTTPException(status_code=422, detail="bbox must be [minLon, minLat, maxLon, maxLat]")
    min_lon, min_lat, max_lon, max_lat = map(float, bbox)
    lim = BBOX_LIMITS
    if (
        min_lon < lim["min_lon"] or max_lon > lim["max_lon"] or
        min_lat < lim["min_lat"] or max_lat > lim["max_lat"]
    ):
        raise HTTPException(
            status_code=422,
            detail={
                "error": "Requested bbox exceeds allowed range.",
                "allowed_bbox": [lim["min_lon"], lim["min_lat"], lim["max_lon"], lim["max_lat"]],
                "hint": "Reduce bbox (minLon,minLat,maxLon,maxLat). Example: bbox=115&bbox=25&bbox=142&bbox=43"
            }
        )


def _estimate_cells_from_doc_or_assume(bbox: List[float], doc: dict) -> int:
    """문서 메타데이터 또는 가정된 해상도로 셀 수 추정"""
    res = (doc or {}).get("resolution") or {}
    dlon = float(res.get("lon_deg") or ASSUMED_DLON)
    dlat = float(res.get("lat_deg") or ASSUMED_DLAT)
    return _estimate_cells_assuming_resolution(bbox, dlon=dlon, dlat=dlat)


def _estimate_cells_assuming_resolution(bbox: List[float], dlon=ASSUMED_DLON, dlat=ASSUMED_DLAT) -> int:
    """주어진 해상도로 bbox 내 셀 수 추정"""
    min_lon, min_lat, max_lon, max_lat = map(float, bbox)

    def to0360(x: float) -> float:
        return x + 360.0 if x < 0 else x

    a, b = to0360(min_lon), to0360(max_lon)
    lon_span = (360.0 - a) + b if a > b else (b - a)
    lat_span = abs(max_lat - min_lat)

    width = int(np.floor(lon_span / dlon)) + 1
    height = int(np.floor(lat_span / dlat)) + 1
    return max(0, width) * max(0, height)