"""
FastAPI Backend for Geospatial Data Lakehouse
==============================================

This creates a REST API that serves geospatial queries.
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any
from datetime import datetime

from spark_config import create_spark_session
from geo_query import GeoQueryEngine

# Initialize Spark (this takes a moment)
print("🚀 Initializing Spark...")
spark = create_spark_session("GeoAPI")

# Initialize Query Engine
delta_path = "../data/delta-lake"
query_engine = GeoQueryEngine(spark, delta_path)

# Create FastAPI app
app = FastAPI(
    title="Geospatial Data Lakehouse API",
    description="Query geospatial image data",
    version="1.0.0"
)

# Enable CORS (so frontend can access this)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# === Request/Response Models ===

class RadiusQuery(BaseModel):
    """Request model for radius queries"""
    latitude: float
    longitude: float
    radius_km: float
    limit: int = 100


class QueryResponse(BaseModel):
    """Response model for queries"""
    count: int
    results: List[Dict[str, Any]]
    query_time_ms: float


# === API Endpoints ===

@app.get("/")
def root():
    """Root endpoint with API info"""
    return {
        "name": "Geospatial Data Lakehouse API",
        "version": "1.0.0",
        "endpoints": {
            "health": "/health",
            "query_radius": "/api/query/radius",
            "query_nearest": "/api/query/nearest",
            "statistics": "/api/statistics",
        }
    }


@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "spark": "connected"}


@app.post("/api/query/radius")
def query_by_radius(query: RadiusQuery):
    """
    Query images within a radius of a point.
    
    Example:
```
    POST /api/query/radius
    {
        "latitude": 40.7580,
        "longitude": -73.9855,
        "radius_km": 10,
        "limit": 100
    }
```
    """
    try:
        start_time = datetime.now()
        
        results = query_engine.query_radius(
            latitude=query.latitude,
            longitude=query.longitude,
            radius_km=query.radius_km
        )
        
        # Limit results
        results = results[:query.limit]
        
        end_time = datetime.now()
        query_time_ms = (end_time - start_time).total_seconds() * 1000
        
        return QueryResponse(
            count=len(results),
            results=results,
            query_time_ms=query_time_ms
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/query/nearest")
def query_nearest_neighbors(
    latitude: float,
    longitude: float,
    k: int = 10
):
    """
    Query k nearest neighbor images.
    
    Example: /api/query/nearest?latitude=51.5079&longitude=-0.0877&k=5
    """
    try:
        start_time = datetime.now()
        
        results = query_engine.query_nearest(
            latitude=latitude,
            longitude=longitude,
            k=k
        )
        
        end_time = datetime.now()
        query_time_ms = (end_time - start_time).total_seconds() * 1000
        
        return QueryResponse(
            count=len(results),
            results=results,
            query_time_ms=query_time_ms
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/statistics")
def get_statistics():
    """Get dataset statistics"""
    try:
        stats = query_engine.get_statistics()
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Run with: uvicorn main:app --reload
if __name__ == "__main__":
    import uvicorn
    print("\n🌐 Starting API server...")
    print("📍 API will be at: http://localhost:8000")
    print("📚 Docs will be at: http://localhost:8000/docs")
    uvicorn.run(app, host="0.0.0.0", port=8000)
