# 🌊 Optimal-LOADS Ocean Gridded Data Demo  
**최적물류 해양 격자 데이터 데모 시스템**

> 🧭 **(2025-11) Optimal-LOADS Demo v0.2 is now launching 🚀**  
> CMEMS 파랑(`VHM0`, `VMDR`, `VTPK`) + GFS 바람(`u/v`, `wind speed`, `wind direction`)  
> 자동 처리 기능이 포함된 버전입니다.

[![Status](https://img.shields.io/badge/status-active-success?style=flat-square)]()
[![Version](https://img.shields.io/badge/version-v0.2-blue?style=flat-square)]()
[![Date](https://img.shields.io/badge/release-2025--11-lightgrey?style=flat-square)]()
[![License](https://img.shields.io/badge/license-research-green?style=flat-square)]()

---

## 🧭 1. Overview / 개요  

**EN**  
This demo system automatically retrieves, processes, and serves global **ocean and atmospheric gridded datasets** (e.g., NOAA GFS, CMEMS).  
All datasets are stored in **AWS S3** and **MongoDB**, and exposed via a **FastAPI backend** for visualization and analysis.  
The pipeline handles **GRIB2 / NetCDF** data formats and returns **JSON-encoded grid arrays** for use in front-end applications.  

It is designed as a prototype for future **IHO S-100-based marine data services** (e.g., S-102 Bathymetry, S-111 Currents, S-104 Water Level).  

**KO**  
이 데모는 **전지구 해양·대기 격자 데이터(GFS, CMEMS 등)**를 자동으로 수집하고,  
S3 및 MongoDB에 저장한 후 **FastAPI 기반 API 서비스**를 통해  
격자 데이터를 조회·시각화할 수 있도록 구성된 프로토타입입니다.  

데이터는 GRIB2 및 NetCDF 형식을 자동 처리하며,  
JSON 인코딩된 격자 배열을 API로 제공합니다.  
향후 **S-100 기반 해양 데이터 서비스(S-102, S-111 등)**와 연계를 목표로 설계되었습니다.

---

## 🌐 2. Data Sources / 데이터 소스  

| Category / 구분 | Source / 소스 | Variables / 변수 | Interval / 주기 | Format / 형식 | Notes |
|-----------------|----------------|------------------|-----------------|----------------|--------|
| Atmosphere / 대기 | NOAA GFS | `eastward_wind (u)`, `northward_wind (v)`, `wind_speed`, `wind_direction` | 3h | GRIB2 | `wind_speed = sqrt(u² + v²)`, `wind_direction = atan2(u, v)` |
| Ocean / 해양 | Copernicus Marine (CMEMS) | `VHM0`, `VMDR`, `VTPK` | 3h | NetCDF | Wave height, direction, period |
| Bathymetry / 수심 | GEBCO / IHO S-102 | – | – | – | 🕓 Planned for future release |

> Reference / 참조: *IHO S-102 Bathymetric Surface Product Specification (Edition 2.0.0)*

---

## ⚙️ 3. Data Processing Pipeline / 데이터 처리 파이프라인  

1. **Download** – Retrieve timeseries datasets using `copernicusmarine.subset()` or `wget`.  
   Supports dateline-crossing (±180°).  
2. **Preprocess** – Read with `xarray`, normalize variable names, units, coordinates.  
3. **Metadata Storage** – Store metadata to `MongoDB Atlas` (`assets_metadata`).  
4. **Upload** – Push to S3 via `boto3.upload_file()` with `ACL: private`.  
5. **Encoding** – Convert to `uint16` with `scale=100`, `nodata=65535`.  
   Index order: *row-major-bottom-up* (South → North).  

---

## 🧩 4. API Service / API 서비스  

| Path | Description / 설명 |
|------|-------------------|
| `/inventory` | View dataset metadata as HTML / 저장된 메타데이터 HTML 조회 |
| `/api/griddata` | Returns JSON-encoded grid values / S3에서 읽어 격자 데이터 반환 |

### Example Request / 예시 요청
```bash
GET /api/griddata?variable=wind_dir&forecast_datetime=2024-08-05T00:00:00Z&source=gfs&bbox=128,34,130,36
```

### Example Response / 예시 응답
```json
{
  "timestamp": "2024-08-05T00:00:00Z",
  "variable": "wind_dir",
  "unit": "degree",
  "name_en": "10 m wind direction (from)",
  "standard_name": "wind_from_direction",
  "bbox": [128, 34, 130, 36],
  "resolution": [0.125, 0.125],
  "shape": [16, 16],
  "indexOrder": "row-major-bottom-up",
  "valueEncoding": {
    "type": "float32",
    "scale": 1,
    "offset": 0,
    "nodata": null
  },
  "data": [247.972061157227, 248.148880004883, ...]
}
```

---

## 🧮 5. Encoding & Index Order / 인코딩 및 인덱스 순서  

| Property / 항목 | Description / 설명 |
|------------------|--------------------|
| Value Encoding | `uint16 + scale = 100`, `nodata = 65535` |
| Index Order | `row-major-bottom-up (South → North)` |
| Lon/Lat Range | lon: −180 ~ 180°, lat: −90 ~ 90° |
| CRS | EPSG:4326 (WGS84) |

### 📘 Index Order Explained (3×3 Example)

S-100 계열 격자 데이터와 동일하게, **남쪽에서 북쪽으로** 값이 저장됩니다.  
즉, 첫 번째 행(`row[0]`)이 남단(최소 위도), 마지막 행(`row[-1]`)이 북단(최대 위도)을 의미합니다.

```
North ↑
[7, 8, 9]
[4, 5, 6]
[1, 2, 3] ← South
```

이 데이터는 직렬화될 때 다음과 같이 표현됩니다:
```json
"data": [1, 2, 3, 4, 5, 6, 7, 8, 9],
"indexOrder": "row-major-bottom-up"
```

즉 `(0,0)`은 남서(SW) 모서리이며, 열(column)과 행(row)이 증가할수록 북동(NE) 방향으로 확장됩니다.

---

## 🚀 6. Deployment / 배포  

### Docker Build & Run / 도커 빌드 및 실행
```bash
# Build image / 이미지 빌드
sudo docker build --no-cache -t fastapi-inventory:latest .

# Run container / 컨테이너 실행
sudo docker run -d --name fastapi-inventory   --restart=always   --env-file .env   -p 80:8000   fastapi-inventory:latest
```

### .env Example / 환경변수 예시
```
APP_TITLE=Optimal-LOADS Inventory
MONGO_URI=mongodb+srv://...
MONGO_DB=optimal_loads
MONGO_COLL=assets_metadata
AWS_REGION=ap-northeast-2
S3_BUCKET=optimal-loads
```

---

## 🌊 7. Demo Examples / 데모 예시  

| Dataset | Variables / 변수 | Description / 설명 |
|----------|------------------|--------------------|
| **Waves (cmems/027)** | `VHM0`, `VMDR`, `VTPK` | Significant wave height, mean direction, peak period / 유의파고, 평균 파향, 피크주기 |
| **Wind (gfs/012)** | `eastward_wind`, `northward_wind`, `wind_speed`, `wind_direction` | 10 m wind components and derived values / 10m 바람 성분 및 계산 변수 |
| **Bathymetry (S-102)** | – | 🕓 Planned for integration (수심 격자 제공 예정) |

---

## 📚 8. References / 참고 문헌  

- **IHO (2019).** *S-102 Bathymetric Surface Product Specification, Edition 2.0.0*  
- **Copernicus Marine User Manual**  
- **NOAA GFS GRIB2 Key Reference**  
- **ISO 19115-2:2009 / ISO 19123:2005** — Coverage schema & metadata standards  

---

## 🔭 9. Future Roadmap / 향후 계획  

- Add `depth` parameter for 3D spatiotemporal grids  
- Integrate S-102 bathymetry layer (planned)  
- Improve encoding performance for large CMEMS datasets  
- Expand metadata structure for uncertainty and QC flags  

---

## 🧑‍💻 Author / 작성자  

**BlueMap – Optimal-LOADS Project**  
Lead Developer: *Jay Kim (김현주)*  
📧 Contact: [hjk@bluemap.dev](mailto:hjk@bluemap.dev)

---

> © 2025 BlueMap / Optimal-LOADS Consortium.  
> Designed for research and demonstration purposes under the **Optimal Logistics Data Space (최적물류 데이터 스페이스)** project.
