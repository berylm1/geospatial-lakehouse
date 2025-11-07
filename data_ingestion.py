"""
Data Ingestion Pipeline - How We Store Geospatial Data
=======================================================

THE PROBLEM:
-----------
You have 1 million photos with GPS coordinates.
How do you find "all photos within 10km of Times Square" quickly?

THE SOLUTION:
------------
1. Store photos in Delta Lake (database features)
2. Use Parquet format (columnar = fast queries)
3. Add GeoHash spatial index (only search nearby grid squares)

WHAT HAPPENS:
------------
Photo → Add GPS Point → Calculate GeoHash → Store in Delta Lake
                         (for indexing)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    DoubleType, TimestampType
)
from sedona.sql import st_constructors as st
from sedona.sql import st_functions as stf
from datetime import datetime
import random


class GeoDataIngestion:
    """Handles ingesting geospatial data into our Delta Lake"""
    
    def __init__(self, spark: SparkSession, delta_path: str):
        """
        Initialize the ingestion pipeline.
        
        Args:
            spark: Our Spark session (from spark_config.py)
            delta_path: Where to save Delta Lake files (like a database location)
        """
        self.spark = spark
        self.delta_path = delta_path
        print(f"📂 Delta Lake location: {delta_path}")
    
    def create_sample_data(self, num_photos: int = 100):
        """
        Create sample photo data for learning.
        In production, this would come from Flickr API or file uploads.
        
        Args:
            num_photos: How many sample photos to generate
            
        Returns:
            List of photo dictionaries
        """
        print(f"\n🎨 Generating {num_photos} sample photos...")
        
        # Famous cities with their real GPS coordinates
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
            
            # Add randomness to coordinates (within ~5km of city center)
            # 0.05 degrees ≈ 5km
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
        
        print(f"✅ Generated {len(photos)} photos across {len(cities)} cities")
        return photos
    
    def ingest_to_delta_lake(self, photos: list):
        """
        THE MAGIC HAPPENS HERE! 🪄
        
        This function:
        1. Takes raw photo data
        2. Adds geospatial Point geometry
        3. Calculates GeoHash for spatial indexing
        4. Saves to Delta Lake with partitioning
        
        Args:
            photos: List of photo dictionaries
        """
        print("\n" + "="*60)
        print("🚀 INGESTION PROCESS - Follow Along!")
        print("="*60)
        
        # === STEP 1: Create DataFrame ===
        print("\n📊 Step 1: Converting Python data to Spark DataFrame...")
        print("   (Think: Excel spreadsheet → Database table)")
        
        df = self.spark.createDataFrame(photos)
        print(f"   ✅ Created DataFrame with {df.count()} rows")
        print("\n   Sample of data:")
        df.select("image_id", "title", "latitude", "longitude").show(3)
        
        # === STEP 2: Add Geometric Point ===
        print("\n🌍 Step 2: Creating geometric points from lat/lon...")
        print("   (Converting GPS numbers → Geometric objects Sedona understands)")
        
        # ST_Point creates a geometric point that Sedona can use for spatial queries
        # Format: ST_Point(longitude, latitude) - NOTE: longitude comes FIRST!
        df_with_geometry = df.withColumn(
            "geometry",
            st.ST_Point(col("longitude"), col("latitude"))
        )
        
        print("   ✅ Added 'geometry' column with Point objects")
        print("\n   Now our data has geometric shapes Sedona can query!")
        
        # === STEP 3: Calculate GeoHash for Spatial Indexing ===
        print("\n🔢 Step 3: Calculating GeoHash for spatial indexing...")
        print("   (Dividing Earth into grid squares for fast lookups)")
        
        # GeoHash precision levels:
        # - Precision 4: ~20km grid squares
        # - Precision 5: ~5km grid squares  
        # - Precision 6: ~1km grid squares ← We use this
        # - Precision 7: ~150m grid squares
        
        df_with_index = df_with_geometry.withColumn(
            "spatial_index",
            stf.ST_GeoHash(col("geometry"), lit(6))  # 6 = ~1km precision
        )
        
        print("   ✅ Added 'spatial_index' column (GeoHash)")
        print("\n   Example GeoHash codes:")
        df_with_index.select("title", "latitude", "longitude", "spatial_index").show(5)
        
        print("\n   📝 NOTICE: Photos near each other have similar GeoHash codes!")
        print("      This lets us partition data by location.")
        
        # === STEP 4: Save to Delta Lake ===
        print("\n💾 Step 4: Writing to Delta Lake with partitioning...")
        print("   (Organizing files by spatial_index for fast queries)")
        
        # PARTITION BY spatial_index:
        # This creates separate folders for each GeoHash
        # Example structure:
        #   delta-lake/
        #   ├── spatial_index=dr5ru/  (Times Square area)
        #   │   └── part-00001.parquet
        #   ├── spatial_index=gcpvj/  (London area)
        #   │   └── part-00002.parquet
        
        (df_with_index
         .write
         .format("delta")           # Use Delta Lake format
         .mode("overwrite")         # Replace if exists (use "append" in production)
         .partitionBy("spatial_index")  # THE KEY TO FAST QUERIES!
         .save(self.delta_path))
        
        print(f"   ✅ Saved {df_with_index.count()} photos to Delta Lake")
        print(f"   📂 Location: {self.delta_path}")
        
        # === STEP 5: Show What We Created ===
        print("\n" + "="*60)
        print("🎉 INGESTION COMPLETE!")
        print("="*60)
        
        # Read back to verify
        saved_df = self.spark.read.format("delta").load(self.delta_path)
        
        print(f"\n📊 Final dataset statistics:")
        print(f"   Total photos: {saved_df.count()}")
        print(f"   Unique spatial indexes: {saved_df.select('spatial_index').distinct().count()}")
        print(f"   Columns: {len(saved_df.columns)}")
        
        print("\n📋 Schema (what columns we have):")
        saved_df.printSchema()
        
        return saved_df


# === DEMONSTRATION ===
if __name__ == "__main__":
    """
    Let's see it in action! 🎬
    """
    from spark_config import create_spark_session
    
    print("="*60)
    print("GEOSPATIAL DATA INGESTION DEMO")
    print("="*60)
    
    # Start Spark
    spark = create_spark_session("IngestionDemo")
    
    # Create ingestion pipeline
    delta_path = "/mnt/user-data/outputs/learning-geospatial/data/delta-lake"
    ingestion = GeoDataIngestion(spark, delta_path)
    
    # Generate and ingest sample data
    photos = ingestion.create_sample_data(num_photos=50)
    result_df = ingestion.ingest_to_delta_lake(photos)
    
    # Show some results
    print("\n" + "="*60)
    print("🔍 LET'S EXPLORE THE DATA")
    print("="*60)
    
    print("\n1️⃣ Photos grouped by city/region (spatial_index):")
    result_df.groupBy("spatial_index").count().show()
    
    print("\n2️⃣ Sample of stored data:")
    result_df.select("image_id", "title", "spatial_index").show(10)
    
    print("\n✅ Data is now ready for fast geospatial queries!")
    print("   Next: We'll learn how to query this data efficiently.")
    
    spark.stop()
