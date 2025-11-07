"""
Data Ingestion Pipeline
=======================

This creates sample geospatial data and stores it in Delta Lake.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from sedona.sql import st_constructors as st
from sedona.sql import st_functions as stf
from datetime import datetime
import random


class GeoDataIngestion:
    """Handles ingesting geospatial data into Delta Lake"""
    
    def __init__(self, spark: SparkSession, delta_path: str):
        self.spark = spark
        self.delta_path = delta_path
        print(f"\n📂 Delta Lake location: {delta_path}")
    
    def create_sample_data(self, num_photos: int = 100):
        """
        Generate sample photo data with GPS coordinates.
        
        In production, this would come from:
        - Flickr API
        - User uploads
        - External datasets
        """
        print(f"\n🎨 Generating {num_photos} sample photos...")
        
        # Famous cities with real GPS coordinates
        cities = [
            ("New York", 40.7128, -74.0060),
            ("London", 51.5074, -0.1278),
            ("Paris", 48.8566, 2.3522),
            ("Tokyo", 35.6762, 139.6503),
            ("Sydney", -33.8688, 151.2093),
        ]
        
        tags_pool = ["sunset", "architecture", "nature", "urban", "portrait"]
        
        photos = []
        for i in range(num_photos):
            # Pick a random city
            city, base_lat, base_lon = random.choice(cities)
            
            # Add randomness (within ~5km of city center)
            lat = base_lat + random.uniform(-0.05, 0.05)
            lon = base_lon + random.uniform(-0.05, 0.05)
            
            photo = {
                "image_id": f"img_{i:05d}",
                "title": f"{city} Photo {i}",
                "latitude": lat,
                "longitude": lon,
                "tags": ",".join(random.sample(tags_pool, k=2)),
                "date_taken": datetime.now(),
            }
            photos.append(photo)
        
        print(f"✅ Generated {len(photos)} photos")
        return photos
    
    def ingest_to_delta_lake(self, photos: list):
        """
        Ingest data into Delta Lake with geospatial indexing.
        
        Steps:
        1. Create DataFrame from photos
        2. Add geometric Point column
        3. Calculate GeoHash for spatial partitioning
        4. Save to Delta Lake
        """
        print("\n" + "="*60)
        print("🚀 INGESTION PROCESS")
        print("="*60)
        
        # Step 1: Create DataFrame
        print("\n📊 Step 1: Creating Spark DataFrame...")
        df = self.spark.createDataFrame(photos)
        print(f"   ✅ Created DataFrame with {df.count()} rows")
        
        # Step 2: Add Geometric Point
        print("\n🌍 Step 2: Adding geometric points...")
        # NOTE: ST_Point takes (longitude, latitude) - longitude first!
        df_with_geometry = df.withColumn(
            "geometry",
            st.ST_Point(col("longitude"), col("latitude"))
        )
        print("   ✅ Added geometry column")
        
        # Step 3: Calculate GeoHash for spatial indexing
        print("\n🔢 Step 3: Calculating GeoHash (precision 6 = ~1km)...")
        df_with_index = df_with_geometry.withColumn(
            "spatial_index",
            stf.ST_GeoHash(col("geometry"), lit(6))
        )
        print("   ✅ Added spatial_index column")
        
        # Show sample
        print("\n   Sample data with GeoHash:")
        df_with_index.select(
            "image_id", "title", "latitude", "longitude", "spatial_index"
        ).show(5, truncate=False)
        
        # Step 4: Save to Delta Lake with partitioning
        print("\n💾 Step 4: Writing to Delta Lake...")
        print("   (Partitioning by spatial_index for fast queries)")
        
        (df_with_index
         .write
         .format("delta")
         .mode("overwrite")  # Replace if exists
         .partitionBy("spatial_index")  # Key to fast queries!
         .save(self.delta_path))
        
        print(f"   ✅ Saved to {self.delta_path}")
        
        # Verify
        saved_df = self.spark.read.format("delta").load(self.delta_path)
        
        print("\n" + "="*60)
        print("✅ INGESTION COMPLETE")
        print("="*60)
        print(f"Total photos: {saved_df.count()}")
        print(f"Unique locations: {saved_df.select('spatial_index').distinct().count()}")
        
        return saved_df


# === Run the ingestion ===
if __name__ == "__main__":
    from spark_config import create_spark_session, stop_spark
    
    print("="*60)
    print("GEOSPATIAL DATA INGESTION DEMO")
    print("="*60)
    
    # Start Spark
    spark = create_spark_session("IngestionDemo")
    
    # Set up ingestion
    delta_path = "../data/delta-lake"
    ingestion = GeoDataIngestion(spark, delta_path)
    
    # Generate and ingest 100 sample photos
    photos = ingestion.create_sample_data(num_photos=100)
    result_df = ingestion.ingest_to_delta_lake(photos)
    
    # Show results grouped by city
    print("\n📊 Photos by location (GeoHash):")
    result_df.groupBy("spatial_index").count().orderBy("count", ascending=False).show(10)
    
    print("\n✅ Data is ready for geospatial queries!")
    
    # Clean up
    stop_spark(spark)
