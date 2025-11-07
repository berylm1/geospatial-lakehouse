"""
Spark Configuration for Geospatial Data Lakehouse
==================================================
"""

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from sedona.spark import SedonaContext


def create_spark_session(app_name="GeoLakehouse"):
    """
    Create and configure a Spark session with Delta Lake and Sedona.
    """
    
    print("\n🚀 Starting Spark session...")
    
    # Create configuration
    conf = SparkConf()
    
    # === Delta Lake JARs ===
    # This tells Spark to download Delta Lake libraries
    conf.set("spark.jars.packages", 
             "io.delta:delta-spark_2.12:3.0.0,"
             "org.apache.sedona:sedona-spark-3.4_2.12:1.5.1,"
             "org.datasyslab:geotools-wrapper:1.5.1-28.2")
    
    # === Delta Lake Configuration ===
    conf.set("spark.sql.extensions", 
             "io.delta.sql.DeltaSparkSessionExtension")
    conf.set("spark.sql.catalog.spark_catalog", 
             "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    # === Memory Settings ===
    conf.set("spark.driver.memory", "2g")
    conf.set("spark.executor.memory", "2g")
    
    # Create base Spark session
    spark = (SparkSession.builder
            .appName(app_name)
            .config(conf=conf)
            .getOrCreate())
    
    # Create Sedona context (replaces the deprecated registerAll)
    sedona = SedonaContext.create(spark)
    
    print(f"✅ Spark {sedona.version} ready!")
    print(f"✅ Delta Lake enabled")
    print(f"✅ Sedona geospatial functions registered")
    
    return sedona


def stop_spark(spark):
    """Stop the Spark session cleanly"""
    if spark:
        spark.stop()
        print("\n🛑 Spark session stopped")


# === Test the configuration ===
if __name__ == "__main__":
    print("="*60)
    print("Testing Spark Configuration")
    print("="*60)
    
    # Start Spark (this will take a minute as it downloads JARs)
    print("\n📦 Downloading required libraries (first time only)...")
    spark = create_spark_session("ConfigTest")
    
    # Test that Sedona works
    print("\n🧪 Testing Sedona geospatial functions...")
    test_df = spark.sql("SELECT ST_Point(1.0, 2.0) as point")
    test_df.show()
    
    print("✅ Success! Spark is configured correctly.")
    
    # Clean up
    stop_spark(spark)
