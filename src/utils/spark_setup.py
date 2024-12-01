from pyspark.sql import SparkSession

def create_spark_session(app_name: str = "LocalETL") -> SparkSession:
    """Create a Spark session configured for local file system operations."""
    return (SparkSession.builder
            .appName(app_name)
            .master("local[*]")
            .config("spark.sql.files.ignoreMissingFiles", "true")
            .config("spark.sql.files.ignoreCorruptFiles", "true")
            .config("spark.hadoop.fs.defaultFS", "file:///")
            .getOrCreate())

def stop_spark_session(spark: SparkSession) -> None:
    """Safely stop the Spark session."""
    if spark is not None:
        spark.stop()