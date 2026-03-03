# api.py
from __future__ import annotations

from fastapi import APIRouter, Query, HTTPException
from typing import Optional, List, Union
from datetime import datetime, timezone, timedelta

from app.models import GridDataResponse
from app.db import get_assets_collection

import numpy as np
import xarray as xr
import tempfile, os, contextlib, boto3


# ---- env / S3 ----
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-2")
S3_BUCKET  = os.getenv("S3_BUCKET", "optimal-loads")
s3 = boto3.client("s3", region_name=AWS_REGION)

ALIASES = {
    "10u": ["10u", "u10"],
    "10v": ["10v", "v10"],
}

router = APIRouter(prefix="/api", tags=["grid"])


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

    # ---- spatial ----
    bbox: Optional[List[float]] = Query(
        default=None,
        description="[minLon, minLat, maxLon, maxLat] (optional)",
        example=[128.0, 34.0, 130.0, 36.0]
    ),

    # ---- optional vertical ----
    # depth: Optional[Union[float, str]] = Query(
    #     default=None,
    #     description="(optional) depth in meters or 'surface', e.g., 0.5, 10, 'surface'"
    # )
) -> GridDataResponse:
    # ✅ Mongo 문서에는 forecast만 있다고 했으니 고정
    type_ = "forecast"

    # ---- 변수 정규화 (별칭 허용) ----
    norm_var = _norm_var(variable)

    # ---- bbox 선검증 (없으면 전역 허용범위 적용) ----
    effective_bbox = bbox or [
        BBOX_LIMITS["min_lon"], BBOX_LIMITS["min_lat"],
        BBOX_LIMITS["max_lon"], BBOX_LIMITS["max_lat"],
    ]
    _validate_bbox_limits_raw(effective_bbox)

    # ---- valid_time 계산 (응답용) ----
    run_dt = _parse_utc(run_time_utc)
    valid_dt = run_dt + timedelta(hours=int(step_hours))
    valid_time_utc = _to_z(valid_dt)

    # ---- computed wind: U/V(10u/10v) 원본을 같은 run+step으로 찾고 계산 ----
    if norm_var in ("wind_speed_10m", "wind_dir_10m"):
        coll = await get_assets_collection()

        # ✅ 재료는 original에서 찾는 게 일반적 (computed 요청이어도)
        doc_u = await _find_by_natural_key(
            coll,
            source=source,
            dataset_code="original",
            model=model,
            type_=type_,
            variable="10u",
            run_time_utc=_to_z(run_dt),
            step_hours=step_hours
        )
        doc_v = await _find_by_natural_key(
            coll,
            source=source,
            dataset_code="original",
            model=model,
            type_=type_,
            variable="10v",
            run_time_utc=_to_z(run_dt),
            step_hours=step_hours
        )

        if not (doc_u and doc_v):
            raise HTTPException(
                status_code=404,
                detail={
                    "error": "Required U/V components not found for computed wind at this run+step.",
                    "needed": ["10u", "10v"],
                    "run_time_utc": _to_z(run_dt),
                    "step_hours": int(step_hours)
                }
            )

        # bbox cell budget (doc 해상도 우선)
        est = _estimate_cells_from_doc_or_assume(effective_bbox, doc_u)  # U 기준(보통 동일)
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

        # ✅ s3 레퍼런스 방어(혹시라도 누락되면 500 대신 명확히)
        if "s3" not in doc_u or "key" not in doc_u.get("s3", {}):
            raise HTTPException(status_code=409, detail={"error": "10u metadata has no s3.key", "doc_keys": list(doc_u.keys())})
        if "s3" not in doc_v or "key" not in doc_v.get("s3", {}):
            raise HTTPException(status_code=409, detail={"error": "10v metadata has no s3.key", "doc_keys": list(doc_v.keys())})

        tmpu = tempfile.NamedTemporaryFile(delete=False, suffix=_tmp_suffix_from_doc(doc_u))
        tmpv = tempfile.NamedTemporaryFile(delete=False, suffix=_tmp_suffix_from_doc(doc_v))
        ds_u = ds_v = None
        try:
            s3.download_file(S3_BUCKET, doc_u["s3"]["key"], tmpu.name)
            s3.download_file(S3_BUCKET, doc_v["s3"]["key"], tmpv.name)
            ds_u = _open_dataset_safely(tmpu.name)
            ds_v = _open_dataset_safely(tmpv.name)

            ds_u = _normalize_lonlat(ds_u)
            ds_v = _normalize_lonlat(ds_v)

            # ✅ 여기서 eastward_wind/northward_wind 사용 금지 → 10u/10v로 통일
            da_u, lat_inc_u, _ = _select_da(ds_u, "10u", bbox)
            da_v, lat_inc_v, _ = _select_da(ds_v, "10v", bbox)
            da_u = _ensure_lat_lon_names(da_u)
            da_v = _ensure_lat_lon_names(da_v)

            # # depth (있으면 선택)
            # da_u, _ = _select_depth_if_present(da_u, depth)
            # da_v, _ = _select_depth_if_present(da_v, depth)

            # 좌표 정합
            da_u, da_v = xr.align(da_u, da_v, join="inner")

            # 계산 (meteorological FROM-direction)
            speed = np.hypot(da_u.values, da_v.values)
            direc = (90.0 - np.degrees(np.arctan2(da_v.values, da_u.values))) % 360.0

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

                "bbox": bbox if bbox else [
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
        ds = _normalize_lonlat(ds)

        if bbox is not None and len(bbox) != 4:
            raise HTTPException(status_code=422, detail="bbox must have 4 numbers: [minLon, minLat, maxLon, maxLat]")

        da, lat_inc, _ = _select_da(ds, norm_var, bbox)
        da = _ensure_lat_lon_names(da)
        # da, _chosen_depth = _select_depth_if_present(da, depth)

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

            "bbox": bbox if bbox else [
                float(da["lon"].values.min()),
                float(da["lat"].values.min()),
                float(da["lon"].values.max()),
                float(da["lat"].values.max())
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


# =============================================================================
# Helpers
# =============================================================================
def resolve_var(ds, var: str) -> str:
    candidates = ALIASES.get(var, [var])
    for name in candidates:
        if name in ds.data_vars:
            return name
    raise KeyError(f"Variable '{var}' not found. Available: {list(ds.data_vars)}")


def _parse_utc(dt_str: str) -> datetime:
    s = dt_str.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s).astimezone(timezone.utc)


def _to_z(dt: datetime) -> str:
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
    fmt = (doc.get("format") or "").lower()
    ctype = (doc.get("content_type") or "").lower()
    name = (doc.get("name") or "").lower()

    if "grib" in fmt or "grib" in ctype or name.endswith((".grib2", ".grib")):
        return ".grib2"
    if "netcdf" in fmt or name.endswith((".nc", ".netcdf")):
        return ".nc"
    return ".bin"


def _open_dataset_safely(path: str) -> xr.Dataset:
    last_err = None
    for engine in ("cfgrib", "h5netcdf"):
        try:
            return xr.open_dataset(path, engine=engine)
        except Exception as e:
            last_err = e
    raise RuntimeError(f"Failed to open dataset: {last_err}")


def _norm_var(v: str) -> str:
    low = v.strip().lower()

    # computed(요청 alias들)
    if low in ("wind_speed_10m", "ws", "ws10", "spd"):
        return "wind_speed_10m"
    if low in ("wind_dir_10m", "wdir", "wdir10", "dir", "wd"):
        return "wind_dir_10m"

    # U/V 성분
    if low in ("u10", "10u"):
        return "10u"
    if low in ("v10", "10v"):
        return "10v"

    return v.strip()


def _normalize_lonlat(ds: xr.Dataset) -> xr.Dataset:
    rename_map = {}
    if "longitude" in ds.coords and "lon" not in ds.coords:
        rename_map["longitude"] = "lon"
    if "latitude" in ds.coords and "lat" not in ds.coords:
        rename_map["latitude"] = "lat"
    return ds.rename(rename_map) if rename_map else ds


def _ensure_lon_range(lon_vals: np.ndarray, x: float) -> float:
    if lon_vals.max() > 180 and x < 0:
        return x + 360.0
    return x


def _select_da(ds: xr.Dataset, var: str, bbox: Optional[List[float]]):
    ds = _normalize_lonlat(ds)

    var2 = resolve_var(ds, var)
    da = ds[var2]

    if "time" in da.dims and da.sizes.get("time", 1) == 1:
        da = da.isel(time=0)

    if bbox and len(bbox) == 4:
        min_lon, min_lat, max_lon, max_lat = map(float, bbox)
        lon_vals = ds["lon"].values
        lat_vals = ds["lat"].values

        min_lon = _ensure_lon_range(lon_vals, min_lon)
        max_lon = _ensure_lon_range(lon_vals, max_lon)

        lon_inc = bool(lon_vals[1] > lon_vals[0]) if lon_vals.size > 1 else True
        lat_inc = bool(lat_vals[1] > lat_vals[0]) if lat_vals.size > 1 else True

        lon_slice = slice(min_lon, max_lon) if lon_inc else slice(max_lon, min_lon)
        lat_slice = slice(min_lat, max_lat) if lat_inc else slice(max_lat, min_lat)

        da = da.sel(lon=lon_slice, lat=lat_slice)
    else:
        lon_vals = ds["lon"].values
        lat_vals = ds["lat"].values
        lon_inc = bool(lon_vals[1] > lon_vals[0]) if lon_vals.size > 1 else True
        lat_inc = bool(lat_vals[1] > lat_vals[0]) if lat_vals.size > 1 else True

    return da, lat_inc, lon_inc


def _ensure_lat_lon_names(da: xr.DataArray) -> xr.DataArray:
    rename_map = {}
    if "latitude" in da.dims:
        rename_map["latitude"] = "lat"
    if "longitude" in da.dims:
        rename_map["longitude"] = "lon"
    return da.rename(rename_map) if rename_map else da


# def _select_depth_if_present(da: xr.DataArray, depth_param):
#     if "depth" not in da.dims:
#         return da, None
#     zvals = da["depth"].values
#     if depth_param is None or (isinstance(depth_param, str) and depth_param.lower() == "surface"):
#         return da.isel(depth=0), float(zvals[0])
#     try:
#         target = float(depth_param)
#         da2 = da.sel(depth=target, method="nearest")
#         if "depth" in da2.dims and da2.sizes["depth"] == 1:
#             da2 = da2.isel(depth=0)
#         return da2, float(da2.coords["depth"].values)
#     except Exception:
#         raise HTTPException(status_code=422, detail="Invalid depth parameter. Use a number (meters) or 'surface'.")


def _prepare_array_for_response(da: xr.DataArray, lat_inc: bool):
    da2 = da.transpose("lat", "lon")
    lat_vals = da2["lat"].values
    lon_vals = da2["lon"].values
    arr2 = da2.values

    if not lat_inc:
        arr2 = arr2[::-1, :]
        lat_vals = lat_vals[::-1]

    dlon = float(abs(np.mean(np.diff(lon_vals)))) if lon_vals.size > 1 else np.nan
    dlat = float(abs(np.mean(np.diff(lat_vals)))) if lat_vals.size > 1 else np.nan
    h, w = arr2.shape
    return arr2, dlon, dlat, w, h


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


def _validate_bbox_limits_raw(bbox: Optional[List[float]]):
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
    res = (doc or {}).get("resolution") or {}
    dlon = float(res.get("lon_deg") or ASSUMED_DLON)
    dlat = float(res.get("lat_deg") or ASSUMED_DLAT)
    return _estimate_cells_assuming_resolution(bbox, dlon=dlon, dlat=dlat)


def _estimate_cells_assuming_resolution(bbox: List[float], dlon=ASSUMED_DLON, dlat=ASSUMED_DLAT) -> int:
    min_lon, min_lat, max_lon, max_lat = map(float, bbox)

    def to0360(x: float) -> float:
        return x + 360.0 if x < 0 else x

    a, b = to0360(min_lon), to0360(max_lon)
    lon_span = (360.0 - a) + b if a > b else (b - a)
    lat_span = abs(max_lat - min_lat)

    width = int(np.floor(lon_span / dlon)) + 1
    height = int(np.floor(lat_span / dlat)) + 1
    return max(0, width) * max(0, height)
