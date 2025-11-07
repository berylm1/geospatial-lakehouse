"""
Spark Configuration for Geospatial Data Lakehouse
==================================================

WHAT IS SPARK?
--------------
Apache Spark is like having multiple workers process data in parallel.
Instead of one computer reading 1 million records, you can have 10 computers
each reading 100k records simultaneously.

WHAT IS SEDONA?
---------------
Apache Sedona adds geospatial superpowers to Spark. It understands:
- GPS coordinates (latitude/longitude)
- Distances between points
- Shapes (circles, polygons)

WHY DELTA LAKE?
---------------
Delta Lake turns simple Parquet files into a reliable database:
- ACID transactions (all-or-nothing writes)
- Time travel (see data as it was yesterday)
- Schema enforcement (prevents bad data)
"""

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer


def create_spark_session(app_name="GeoLakehouse"):
    """
    Create a Spark session with Delta Lake and Sedona enabled.
    
    Think of this like opening a connection to a database, but for big data.
    
    Args:
        app_name: Name for your Spark application (shows in monitoring)
    
    Returns:
        SparkSession: Your configured Spark instance
    """
    
    # Step 1: Configure Spark settings
    conf = SparkConf()
    
    # === DELTA LAKE CONFIGURATION ===
    # These two lines enable Delta Lake's ACID transaction features
    
    # This line adds Delta Lake's SQL extensions
    conf.set("spark.sql.extensions", 
             "io.delta.sql.DeltaSparkSessionExtension")
    
    # This line tells Spark to use Delta Lake as the catalog
    # (catalog = the system that tracks what tables exist)
    conf.set("spark.sql.catalog.spark_catalog", 
             "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    # === SEDONA (GEOSPATIAL) CONFIGURATION ===
    # Kryo is a faster way to serialize (save/load) objects
    # Sedona needs this to efficiently handle geometric shapes
    conf.set("spark.serializer", KryoSerializer.getName)
    conf.set("spark.kryo.registrator", SedonaKryoRegistrator.getName)
    
    # === PERFORMANCE TUNING ===
    # Adaptive Query Execution: Spark optimizes queries while they run
    conf.set("spark.sql.adaptive.enabled", "true")
    
    # Coalesce partitions: Combines small chunks of data for efficiency
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    # === MEMORY CONFIGURATION ===
    # Driver = the main coordinator process
    # Executor = worker processes that do the actual work
    # 
    # For your laptop: 4GB each is reasonable
    # For production: scale up to 16GB+ depending on your data size
    conf.set("spark.driver.memory", "4g")
    conf.set("spark.executor.memory", "4g")
    
    # Step 2: Create the Spark session
    print("🚀 Starting Spark with Delta Lake and Sedona...")
    
    spark = (SparkSession.builder
            .appName(app_name)
            .config(conf=conf)
            .getOrCreate())
    
    # Step 3: Register Sedona's geospatial functions
    # This enables SQL functions like ST_Distance, ST_Within, etc.
    SedonaRegistrator.registerAll(spark)
    
    print(f"✅ Spark {spark.version} ready with geospatial capabilities!")
    
    return spark


# === EXAMPLE USAGE ===
if __name__ == "__main__":
    # Try it out!
    spark = create_spark_session()
    
    # Test that Sedona works by creating a point
    test_query = "SELECT ST_Point(1.0, 2.0) as point"
    result = spark.sql(test_query)
    
    print("\n🧪 Testing Sedona functions:")
    result.show()
    
    print("\n✅ Everything working! Sedona can create geometric points.")
    
    spark.stop()
