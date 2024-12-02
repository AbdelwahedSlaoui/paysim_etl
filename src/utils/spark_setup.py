from pyspark.sql import SparkSession

def create_spark_session(app_name: str = "LocalETL") -> SparkSession:
    """Create a Spark session optimized for local system"""
    return (SparkSession.builder
            .appName(app_name)
            .master("local[2]")
            # Memory Management
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "2g")
            .config("spark.memory.fraction", "0.6")
            .config("spark.memory.storageFraction", "0.5")
            # Shuffle Management
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.default.parallelism", "4")
            # Disk Management
            .config("spark.local.dir", "/tmp/spark-temp")
            .config("spark.sql.files.maxPartitionBytes", "64mb")
            .config("spark.sql.files.ignoreMissingFiles", "true")
            .config("spark.sql.files.ignoreCorruptFiles", "true")
            # File System
            .config("spark.hadoop.fs.defaultFS", "file:///")
            .getOrCreate())

def stop_spark_session(spark: SparkSession) -> None:
    """Safely stop the Spark session and clean up resources."""
    if spark is not None:
        spark.stop()