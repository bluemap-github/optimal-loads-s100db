# app/db.py
import os
from motor.motor_asyncio import AsyncIOMotorClient

_MONGO_URI = os.getenv("MONGO_URI")
_MONGO_DB  = os.getenv("MONGO_DB", "optimal_loads")
_ASSET_COL = os.getenv("MONGO_COL", "assets_metadata")
_DIR_COL   = os.getenv("MONGO_DIR_COL", "directories")

_CONTROL_COL = os.getenv("MONGO_CONTROL_COL", "ingestion_control")
_RUNS_COL    = os.getenv("MONGO_RUNS_COL", "ingestion_runs")
_WHEN_COL    = os.getenv("MONGO_WHEN_COL", "ingestion_when")   # ✅ 추가

_client = AsyncIOMotorClient(_MONGO_URI)
_db = _client[_MONGO_DB]

async def get_assets_collection():
    return _db[_ASSET_COL]

async def get_directories_collection():
    return _db[_DIR_COL]

async def get_ingestion_control_collection():
    return _db[_CONTROL_COL]

async def get_ingestion_runs_collection():
    return _db[_RUNS_COL]

async def get_ingestion_when_collection():   # ✅ 추가
    return _db[_WHEN_COL]
