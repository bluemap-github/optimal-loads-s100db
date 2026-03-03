# main.py
from __future__ import annotations

import os
import json
from datetime import datetime, timezone
from typing import Optional, Any, Dict, List

from dotenv import load_dotenv
from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.cors import CORSMiddleware
from app.ingestion import router as ingestion_router

from app.db import get_assets_collection, get_directories_collection
from app.api import router as api_router

from urllib.parse import quote

load_dotenv()

APP_TITLE = os.getenv("APP_TITLE", "Inventory")
templates = Jinja2Templates(directory="app/templates")


templates.env.filters["urlencode"] = lambda v: quote(v or "")

# (선택) /inventory 루트에서 보여줄 시작 경로
ROOT_DIR = os.getenv("INVENTORY_ROOT_DIR", "ecmwf/ifs/")  # trailing "/" 권장

app = FastAPI(
    title=APP_TITLE,
    version="0.1.0",
    description="""
It reads ocean gridded data (e.g., CMEMS wave data) from S3 and provides it as JSON.

- Indexing: row-major, bottom-up (south → north)
""",
    contact={"name": "BlueMap", "email": "hjk@bluemap.dev"},
    license_info={"name": "MIT"},
    openapi_tags=[{"name": "grid", "description": "grid data API"}],
)

app.mount("/guide", StaticFiles(directory="app/templates/static/guide"), name="guide")


@app.get("/ko", response_class=HTMLResponse, include_in_schema=False)
async def root_ko():
    return FileResponse("app/templates/static/guide/index_ko.html")


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def root_en():
    return FileResponse("app/templates/static/guide/index_en.html")


app.include_router(api_router)
app.include_router(ingestion_router)

# ---- CORS ----
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================================================
# NOAA Index 스타일 helper
# =========================================================

def _norm_path(p: str) -> str:
    p = (p or "/").strip()
    if not p.startswith("/"):
        p = "/" + p
    if p != "/" and p.endswith("/"):
        p = p[:-1]
    return p


def _parent_path(p: str) -> Optional[str]:
    p = _norm_path(p)
    if p == "/":
        return None
    parts = p.strip("/").split("/")
    if len(parts) <= 1:
        return "/"
    return "/" + "/".join(parts[:-1])


def _human_size(n: Optional[int]) -> Optional[str]:
    if n is None:
        return None
    size = float(n)
    for unit in ["B", "K", "M", "G", "T"]:
        if size < 1024 or unit == "T":
            if unit == "B":
                return f"{int(size)}"
            return f"{size:.1f}{unit}"
        size /= 1024.0
    return str(n)


def _to_iso_z(v: Any) -> str:
    """
    datetime -> "YYYY-MM-DDTHH:MM:SSZ"
    str이면 그대로 반환(이미 Z 포맷이라고 가정)
    """
    if v is None:
        return ""
    if isinstance(v, str):
        return v
    if isinstance(v, datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return str(v)


def _fmt_lm(v: Any) -> Optional[str]:
    """created_at/last_modified가 datetime 또는 ISO Z string일 수 있으니 안전 변환"""
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.strftime("%d-%b-%Y %H:%M")
    if isinstance(v, str):
        # "2025-12-29T03:51:25.349+00:00" 또는 "...Z" 모두 대응
        try:
            s = v.replace("Z", "+00:00")
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).strftime("%d-%b-%Y %H:%M")
        except Exception:
            return None
    return None


def _build_api_example(doc: Dict[str, Any], bbox: List[str]) -> str:
    """
    Final griddata API example
    - stream, type 제거
    - datetime은 RFC3339 그대로 (NO URL encoding)
    """
    source = doc.get("source", "ecmwf")
    dataset_code = doc.get("dataset_code", "original")
    model = doc.get("model", "ifs")
    variable = doc.get("variable", "")

    # ✅ assets_metadata 필드명: run_time_utc
    run_time_utc = _to_iso_z(doc.get("run_time_utc"))
    step_hours = int(doc.get("step_hours", 0))

    bbox_q = "&".join(f"bbox={v}" for v in bbox)

    return (
        "http://52.78.244.211/api/griddata"
        f"?source={source}"
        f"&dataset_code={dataset_code}"
        f"&model={model}"
        f"&variable={variable}"
        f"&run_time_utc={run_time_utc}"
        f"&step_hours={step_hours}"
        f"&{bbox_q}"
    )


# =========================================================
# ✅ /inventory (Index of …) - directories + assets_metadata
# =========================================================

@app.get("/inventory", response_class=HTMLResponse, include_in_schema=False)
async def inventory_index(
    request: Request,
    path: str = Query("/", description="directory-like path. e.g. /ecmwf/ifs/2025/2025-07/2025-07-16/06Z/"),
    bbox: List[str] = Query(default=["128", "34", "130", "36"], description="bbox repeated 4 times: minLon,minLat,maxLon,maxLat"),
):
    assets = await get_assets_collection()
    dirs = await get_directories_collection()

    current_path = _norm_path(path)
    if current_path != "/" and not current_path.endswith("/"):
        current_path += "/"

    parent_path = _parent_path(current_path.rstrip("/"))
    if parent_path and not parent_path.endswith("/"):
        parent_path += "/"

    # path -> prefix (앞 "/" 제거)  ※ directories._id와 동일한 포맷 (trailing "/" 포함)
    prefix_dir = current_path.lstrip("/")  # e.g. "ecmwf/ifs/.../original/"
    # 루트면 prefix_dir == ""

    entries: List[Dict[str, Any]] = []

    # -----------------------------
    # 1) ✅ 하위 폴더 목록: directories에서 즉시 조회
    # -----------------------------
    if prefix_dir:
        ddoc = await dirs.find_one({"_id": prefix_dir}, {"children_dirs": 1})
        children = (ddoc or {}).get("children_dirs", [])
    else:
        # 루트(/)에서 정책: ROOT_DIR 한 개만 보여주기
        root = (ROOT_DIR or "").lstrip("/")
        if root and not root.endswith("/"):
            root += "/"
        children = [root] if root else []

    for child in sorted(children):
        name = child.rstrip("/")
        child_path = (current_path + child).replace("//", "/")
        entries.append({
            "name": name,
            "is_dir": True,
            "path": child_path,
            "file_id": None,
            "last_modified": None,  # 필요하면 child들의 last_modified를 한번에 조회해서 넣기
            "size_human": None,
        })

    # -----------------------------
    # 2) ✅ 현재 디렉토리의 파일 목록: assets_metadata에서 조회
    # -----------------------------
    file_docs = await assets.find(
        {"inventory_directory": prefix_dir},
        {
            "inventory_name": 1,
            "name": 1,
            "natural_key": 1,
            "created_at": 1,
            "size_bytes": 1,
            "source": 1,
            "dataset_code": 1,
            "model": 1,
            "variable": 1,
            "run_time_utc": 1,
            "step_hours": 1,
        }
    ).sort([("name", 1), ("inventory_name", 1)]).to_list(length=5000)

    for d in file_docs:
        display_name = d.get("name") or d.get("inventory_name") or "(unnamed)"
        api_example = _build_api_example(d, bbox=bbox)

        entries.append({
            "name": display_name,
            "is_dir": False,
            "path": None,
            "api_url": api_example,
            "file_id": d.get("natural_key"),
            "last_modified": _fmt_lm(d.get("created_at")),
            "size_human": _human_size(d.get("size_bytes")),
        })

    entries.sort(key=lambda x: (0 if x["is_dir"] else 1, x["name"]))

    return templates.TemplateResponse(
        "inventory.html",
        {
            "request": request,
            "current_path": current_path,
            "parent_path": parent_path,
            "entries": entries,
            "description": None,
        },
    )


# =========================================================
# 파일 상세 보기: s3.key 기반 (assets_metadata)
# =========================================================

@app.get("/inventory/file", response_class=HTMLResponse, include_in_schema=False)
async def inventory_file(request: Request, key: str = Query(..., description="S3 key")):
    assets = await get_assets_collection()

    doc = await assets.find_one({"s3.key": key}, {"_id": 0})
    if not doc:
        raise HTTPException(status_code=404, detail=f"Not found: {key}")

    preferred_order = [
        ("s3.key", None),
        ("source", None),
        ("dataset_code", None),
        ("model", None),
        ("type", None),
        ("stream", None),
        ("variable", None),
        ("name", None),
        ("name_en", None),
        ("unit", None),
        ("format", None),
        ("content_type", None),
        ("size_bytes", None),
        ("resolution", None),
        ("run_time_utc", None),
        ("step_hours", None),
        ("valid_time_utc", None),
        ("natural_key", None),
        ("valid_key", None),
        ("created_at", None),
        ("s3", None),
    ]

    def get_by_path(d: Dict[str, Any], path: str):
        cur: Any = d
        for part in path.split("."):
            if not isinstance(cur, dict) or part not in cur:
                return None
            cur = cur[part]
        return cur

    def fmt(v: Any) -> str:
        if v is None:
            return ""
        if isinstance(v, datetime):
            return v.isoformat()
        if isinstance(v, (dict, list)):
            return json.dumps(v, ensure_ascii=False, indent=2)
        return str(v)

    rows = []
    used = set()

    for field, _ in preferred_order:
        val = get_by_path(doc, field) if "." in field else doc.get(field)
        if val is not None:
            rows.append({"k": field, "v": fmt(val), "is_json": isinstance(val, (dict, list))})
            used.add(field.split(".")[0])

    other = []
    for k, v in doc.items():
        if k in used:
            continue
        other.append({"k": k, "v": fmt(v), "is_json": isinstance(v, (dict, list))})
        
    nk = doc.get("natural_key", "")
    display_name = doc.get("name") or doc.get("inventory_name") or ""
    inventory_directory = doc.get("inventory_directory", "")
    s3_key = (doc.get("s3") or {}).get("key", "")

    return templates.TemplateResponse(
        "inventory_file.html",
        {
            "request": request,
            "key": key,
            "nk": nk,
            "display_name": display_name,
            "inventory_directory": inventory_directory,
            "s3_key": s3_key,
            "rows": rows,
            "other": other,
        },
    )
