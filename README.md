# 🌍 Geospatial Data Lakehouse

A full-stack geospatial data platform built with Apache Spark, Delta Lake, Apache Sedona, and React.

![Project Screenshot](screenshot.png)

## 🚀 Features

- **Geospatial Query Engine**: Radius search, K-nearest neighbors
- **Delta Lake Storage**: ACID transactions on Parquet files
- **Spatial Indexing**: GeoHash partitioning for fast queries
- **Interactive Map**: Real-time visualization with Leaflet
- **REST API**: FastAPI backend with automatic documentation

## 🛠️ Tech Stack

**Backend:**
- Python 3.10+
- Apache Spark 3.5.0
- Apache Sedona 1.5.1
- Delta Lake 3.0.0
- FastAPI

**Frontend:**
- React 18
- TypeScript
- Leaflet
- Vite

## 📦 Installation

### Prerequisites

- Python 3.10+
- Node.js 18+
- Java 11+

### Backend Setup
```bash
cd backend
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Frontend Setup
```bash
cd frontend
npm install
```

## 🚀 Running the Application

### Start Backend (Terminal 1)
```bash
cd backend
source venv/bin/activate
python main.py
```

Backend will run at: http://localhost:8000

### Start Frontend (Terminal 2)
```bash
cd frontend
npm run dev
```

Frontend will run at: http://localhost:3000

## 📊 Usage

1. Open http://localhost:3000 in your browser
2. Enter latitude, longitude, and radius
3. Click "Search" to query photos
4. View results on the interactive map
5. Click markers to see photo details

### Example Queries

- **NYC (Times Square)**: 40.7580, -73.9855, 50km
- **London**: 51.5074, -0.1278, 30km
- **Paris**: 48.8566, 2.3522, 40km

## 🏗️ Architecture
```
Frontend (React + Leaflet)
    ↓
REST API (FastAPI)
    ↓
Query Engine (Spark + Sedona)
    ↓
Delta Lake (Parquet + Transaction Log)
```

### Key Components

- **spark_config.py**: Spark session with Delta Lake & Sedona
- **data_ingestion.py**: Data pipeline with spatial indexing
- **geo_query.py**: Geospatial query engine
- **main.py**: FastAPI REST API
- **App.tsx**: React frontend with map

## 📚 API Documentation

Interactive API docs available at: http://localhost:8000/docs

### Endpoints

- `GET /health` - Health check
- `POST /api/query/radius` - Radius search
- `GET /api/query/nearest` - K-nearest neighbors
- `GET /api/statistics` - Dataset statistics

## 🎯 How It Works

1. **Data Ingestion**: Photos are stored in Delta Lake with GPS coordinates
2. **Spatial Indexing**: GeoHash partitioning groups nearby locations
3. **Query Optimization**: Only relevant partitions are read
4. **Fast Queries**: Sub-second response times for typical queries

## 📝 License

MIT License

## 👤 Author

Built as a learning project for geospatial data engineering.
