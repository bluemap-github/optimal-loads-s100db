optimal-loads-gfs-viewer    
├── .github
│   └── workflows
│       └──deploy-demo-v0-4.yml
├── 2025-07-demo-v0.1
├── 2025-11-demo-v0.2
├── 2025-12-demo-v0.3
├── 2026-01-demo-v0.4
│    │
│    ├── services
│    │   └── fastapi-inventory
│    │       └── app
│    │
│    ├── orchestration
│    │   ├── collector
│    │   │   ├── ecmwf
│    │   │   │   ├── fetch.py        
│    │   │   │   ├── metadata.py     
│    │   │   │   ├── storage.py      
│    │   │   │   ├── directories.py     
│    │   │   │   └── __init__.py
│    │   │   └── __init__.py
│    │   │
│    │   ├── airflow
│    │   │   └── dags
│    │   │       └── ecmwf_ifs_demo.py 
│    │   │   └── Dockerfile 
│    │   │   └── requirements.txt  
│    │   │
│    │   ├── kafka
│    │   │   ├── producer.py 
│    │   │   ├── topics.py
│    │   │   └── schemas.py
│    │   │
│    │   └── runners
│    │       └── ecmwf_task.py 
│    │
│    └── README.md
│