# 🚀 Setup Guide

Complete setup instructions for the Geospatial Data Lakehouse.

## Prerequisites

### 1. Install Java 11

**macOS (using Homebrew):**
```bash
brew install openjdk@11
echo 'export PATH="/opt/homebrew/opt/openjdk@11/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
java -version
```

### 2. Install Python 3.9+
```bash
python3 --version
```

### 3. Install Node.js 18+

**macOS:**
```bash
brew install node
node --version
```

## Installation Steps

### Backend Setup
```bash
# 1. Navigate to backend
cd backend

# 2. Create virtual environment
python3 -m venv venv

# 3. Activate virtual environment
source venv/bin/activate  # macOS/Linux
# OR
venv\Scripts\activate  # Windows

# 4. Install dependencies
pip install -r requirements.txt

# 5. Generate sample data
python data_ingestion.py

# 6. Test Spark configuration
python spark_config.py
```

### Frontend Setup
```bash
# 1. Navigate to frontend
cd frontend

# 2. Install dependencies
npm install

# 3. Test build
npm run dev
```

## Running the Application

### Terminal 1: Backend
```bash
cd backend
source venv/bin/activate
python main.py
```

**Backend runs at:** http://localhost:8000

**API Docs at:** http://localhost:8000/docs

### Terminal 2: Frontend
```bash
cd frontend
npm run dev
```

**Frontend runs at:** http://localhost:3000

## Testing

### Test Backend API
```bash
# Health check
curl http://localhost:8000/health

# Radius query
curl -X POST http://localhost:8000/api/query/radius \
  -H "Content-Type: application/json" \
  -d '{"latitude": 40.7580, "longitude": -73.9855, "radius_km": 10, "limit": 5}'
```

### Test Frontend

1. Open http://localhost:3000
2. Click "NYC (Times Square)"
3. Click "Search"
4. Verify markers appear on map

## Troubleshooting

### Issue: "Java Runtime not found"

**Solution:** Install Java 11 (see Prerequisites)

### Issue: "Port 8000 already in use"

**Solution:** 
```bash
# Kill process on port 8000
lsof -ti:8000 | xargs kill -9
```

### Issue: "Module not found" in Python

**Solution:**
```bash
# Make sure venv is activated
source venv/bin/activate
pip install -r requirements.txt
```

### Issue: Spark memory errors

**Solution:** Reduce memory in `spark_config.py`:
```python
conf.set("spark.driver.memory", "1g")
conf.set("spark.executor.memory", "1g")
```

## Project Structure
```
geospatial-lakehouse/
├── backend/
│   ├── spark_config.py       # Spark + Sedona setup
│   ├── data_ingestion.py     # Data pipeline
│   ├── geo_query.py          # Query engine
│   ├── main.py               # FastAPI server
│   └── requirements.txt      # Python deps
├── frontend/
│   ├── src/
│   │   ├── App.tsx           # Main React app
│   │   ├── services/api.ts   # API client
│   │   └── types/index.ts    # TypeScript types
│   ├── package.json          # Node deps
│   └── vite.config.ts        # Vite config
├── data/
│   └── delta-lake/           # Delta Lake storage
└── README.md
```

## What Gets Created

After running `data_ingestion.py`:
- 100 sample photos with GPS coordinates
- Delta Lake files in `data/delta-lake/`
- Parquet files partitioned by GeoHash
- Transaction log in `data/delta-lake/_delta_log/`

## Performance

- **Query Time**: ~300-500ms for typical queries
- **Data Size**: ~100 photos (demo), scalable to millions
- **Storage**: Delta Lake with ACID transactions
- **Indexing**: GeoHash spatial partitioning

## Next Steps

1. Add more sample data (increase from 100 to 1000+)
2. Implement tag-based filtering
3. Add export functionality (CSV, GeoJSON)
4. Deploy to cloud (AWS, Azure, GCP)

## Learning Resources

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Apache Sedona](https://sedona.apache.org/)
- [Delta Lake](https://delta.io/)
- [FastAPI](https://fastapi.tiangolo.com/)
- [React Leaflet](https://react-leaflet.js.org/)
