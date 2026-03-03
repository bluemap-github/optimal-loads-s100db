from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional, Union

class InventoryItem(BaseModel):
    source: Optional[str] = None
    dataset_code: Optional[str] = None
    variable: Optional[str] = None
    unit: Optional[str] = Field(None, description="변수 단위 (예: m, degree, s, m s-1)")
    name_en: Optional[str] = Field(None, description="사람이 읽는 변수명(영문)")
    standard_name: Optional[str] = Field(None, description="CF 표준 변수명(가능한 경우)")
    year: Optional[int] = None
    month: Optional[int] = None
    valid_time_utc: Optional[str] = None
    name: Optional[str] = None
    size_bytes: Optional[int] = None
    s3: Optional[dict] = None     # { bucket, region, key }


class ValueEncoding(BaseModel):
    type: str = Field("uint16", description="인코딩 타입: uint16 또는 float32")
    scale: Optional[float] = Field(None, description="스케일 팩터 (uint16일 때)")
    offset: Optional[float] = Field(None, description="오프셋 (uint16일 때)")
    nodata: Optional[Union[int, float]] = Field(None, description="결측값 마커 (uint16일 때는 int, float32일 때는 None 또는 NaN)")

class GridDataResponse(BaseModel):
    timestamp: str = Field(..., description="ISO8601 UTC")
    variable: str
    unit: Optional[str] = Field(None, description="변수 단위 (예: m, degree, s, m s-1)")
    name_en: Optional[str] = Field(None, description="사람이 읽는 변수명(영문)")
    standard_name: Optional[str] = Field(None, description="CF 표준 변수명(가능한 경우)")
    bbox: List[float] = Field(..., description="[minLon, minLat, maxLon, maxLat]")
    resolution: List[float] = Field(..., description="[dLon, dLat] (deg)")
    shape: List[int] = Field(..., description="[width(lon), height(lat)]")
    indexOrder: str = Field("row-major-bottom-up")
    valueEncoding: ValueEncoding
    data: List[Optional[Union[int, float]]] = Field(..., description="데이터 배열 (uint16일 때는 int, float32일 때는 float, null은 결측값)")

        # ---- OpenAPI 예시 (Ellipsis 절대 금지) ----
    model_config = ConfigDict(json_schema_extra={
        "example": {
            "timestamp": "2024-03-31T00:00:00Z",
            "variable": "VHM0",
            "bbox": [128.0, 34.0, 130.0, 36.0],
            "resolution": [0.0833333333333, 0.0833333333333],
            "shape": [25, 25],
            "indexOrder": "row-major-bottom-up",
            "valueEncoding": {
                "type": "uint16",
                "scale": 100,
                "offset": 0,
                "nodata": 65535
            },
            "data": [94, 96, 97, "…", 86]  # 긴 배열은 줄이고 "…"는 문자열로
        }
    })


class ErrorResponse(BaseModel):
    error: str