"""
Geospatial Query Engine
=======================

This handles spatial queries on our Delta Lake data:
- Find photos within X km of a point (Radius search)
- Find K nearest photos to a point (K-NN search)
- Bounding box queries
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from sedona.sql import st_constructors as st
from sedona.sql import st_functions as stf


class GeoQueryEngine:
    """Execute geospatial queries on Delta Lake data"""
    
    def __init__(self, spark: SparkSession, delta_path: str):
        self.spark = spark
        self.delta_path = delta_path
        self._load_table()
    
    def _load_table(self):
        """Load Delta table and create a SQL view"""
        self.df = self.spark.read.format("delta").load(self.delta_path)
        self.df.createOrReplaceTempView("geo_images")
        print(f"✅ Loaded {self.df.count()} photos from Delta Lake")
    
    def query_radius(self, latitude: float, longitude: float, radius_km: float):
        """
        Find all photos within a radius of a point.
        
        HOW IT WORKS:
        1. Create a point from lat/lon
        2. Calculate distance to each photo using ST_Distance
        3. Filter photos within radius
        4. Sort by distance
        
        Args:
            latitude: Center point latitude
            longitude: Center point longitude
            radius_km: Search radius in kilometers
            
        Returns:
            List of photos with distances
        """
        print(f"\n🔍 Searching for photos within {radius_km}km of ({latitude}, {longitude})")
        
        # Convert km to meters (Sedona uses meters)
        radius_m = radius_km * 1000
        
        # Create SQL query
        query = f"""
        SELECT 
            image_id,
            title,
            latitude,
            longitude,
            tags,
            CAST(ST_Distance(geometry, ST_GeomFromText('POINT({longitude} {latitude})')) as DOUBLE) as distance_m
        FROM geo_images
        WHERE ST_Distance(geometry, ST_GeomFromText('POINT({longitude} {latitude})')) <= {radius_m}
        ORDER BY distance_m
        """
        
        result_df = self.spark.sql(query)
        
        # Convert to Python list with distance in km
        results = []
        for row in result_df.collect():
            photo = row.asDict()
            if photo['distance_m'] is not None:
                photo['distance_km'] = photo['distance_m'] / 1000
            else:
                photo['distance_km'] = 0.0
            results.append(photo)
        
        print(f"   ✅ Found {len(results)} photos")
        return results
    
    def query_nearest(self, latitude: float, longitude: float, k: int = 10):
        """
        Find K nearest photos to a point.
        
        Args:
            latitude: Query point latitude
            longitude: Query point longitude
            k: Number of nearest photos to find
            
        Returns:
            List of k nearest photos with distances
        """
        print(f"\n🔍 Finding {k} nearest photos to ({latitude}, {longitude})")
        
        query = f"""
        SELECT 
            image_id,
            title,
            latitude,
            longitude,
            tags,
            CAST(ST_Distance(geometry, ST_GeomFromText('POINT({longitude} {latitude})')) as DOUBLE) as distance_m
        FROM geo_images
        ORDER BY distance_m
        LIMIT {k}
        """
        
        result_df = self.spark.sql(query)
        
        results = []
        for row in result_df.collect():
            photo = row.asDict()
            if photo['distance_m'] is not None:
                photo['distance_km'] = photo['distance_m'] / 1000
            else:
                photo['distance_km'] = 0.0
            results.append(photo)
        
        print(f"   ✅ Found {len(results)} nearest photos")
        return results
    
    def get_statistics(self):
        """Get dataset statistics"""
        stats = {
            "total_photos": self.df.count(),
            "unique_locations": self.df.select("spatial_index").distinct().count(),
            "cities": self.df.select("title").rdd.map(lambda x: x[0].split()[0]).distinct().collect()
        }
        return stats


# === Demo the queries ===
if __name__ == "__main__":
    from spark_config import create_spark_session, stop_spark
    
    print("="*60)
    print("GEOSPATIAL QUERY DEMO")
    print("="*60)
    
    # Start Spark
    spark = create_spark_session("QueryDemo")
    
    # Initialize query engine
    delta_path = "../data/delta-lake"
    query_engine = GeoQueryEngine(spark, delta_path)
    
    # Get statistics
    print("\n📊 Dataset Statistics:")
    stats = query_engine.get_statistics()
    for key, value in stats.items():
        print(f"   {key}: {value}")
    
    # DEMO 1: Radius search around Times Square, NYC
    print("\n" + "="*60)
    print("DEMO 1: Radius Search")
    print("="*60)
    print("Query: Photos within 50km of Times Square, NYC")
    
    results = query_engine.query_radius(
        latitude=40.7580,   # Times Square
        longitude=-73.9855,
        radius_km=50
    )
    
    print("\n📸 Results:")
    for photo in results[:5]:  # Show first 5
        print(f"   • {photo['title']:30} - {photo['distance_km']:.2f} km away")
    
    # DEMO 2: Find nearest photos to London Bridge
    print("\n" + "="*60)
    print("DEMO 2: Nearest Neighbors")
    print("="*60)
    print("Query: 5 nearest photos to London Bridge")
    
    results = query_engine.query_nearest(
        latitude=51.5079,   # London Bridge
        longitude=-0.0877,
        k=5
    )
    
    print("\n📸 Results:")
    for i, photo in enumerate(results, 1):
        print(f"   {i}. {photo['title']:30} - {photo['distance_km']:.2f} km away")
    
    print("\n✅ Query engine working perfectly!")
    
    # Clean up
    stop_spark(spark)
