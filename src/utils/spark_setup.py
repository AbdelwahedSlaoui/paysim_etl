from pyspark.sql import SparkSession
from typing import Optional
import logging
from pathlib import Path

def create_spark_session(
    app_name: str = "local-etl",
    log_level: str = "WARN"
) -> SparkSession:
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        # Create builder with optimized local configs
        builder = (
            SparkSession.builder.appName(app_name)
            .master("local[2]")
            .config("spark.driver.memory", "4g")  # Half of available RAM
            .config("spark.memory.fraction", "0.6")  # Memory fraction
            .config("spark.memory.storageFraction", "0.5")  # Balanced storage/execution
            .config("spark.sql.shuffle.partitions", "8")  # Reduced from default 200
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC -XX:+UseG1GC") # MacOS specific optimizations
            .config("spark.local.dir", str(Path.home() / ".spark-temp")) # Temporary directory management
        )

        # Create and configure session
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel(log_level)

        logger.info("Spark session created successfully")
        return spark

    except Exception as e:
        logger.error(f"Failed to create Spark session: {str(e)}")
        raise

def stop_spark_session(spark: Optional[SparkSession] = None) -> None:
    if spark:
        try:
            spark.stop()
            logging.info("Spark session stopped successfully")
        except Exception as e:
            logging.error(f"Error stopping Spark session: {str(e)}")