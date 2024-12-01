from pyspark.sql import SparkSession
import logging
from pathlib import Path

def create_spark_session(app_name: str = "local-etl") -> SparkSession:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        spark = (SparkSession.builder
            .appName(app_name)
            .master("local[2]")
            .config("spark.driver.memory", "4g")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.local.dir", str(Path.home() / ".spark-temp"))
            .getOrCreate())

        # Set log level
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session created successfully")
        return spark

    except Exception as e:
        logger.error(f"Failed to create Spark session: {str(e)}")
        raise

def stop_spark_session(spark: SparkSession) -> None:
    try:
        spark.stop()
        logging.info("Spark session stopped successfully")
    except Exception as e:
        logging.error(f"Error stopping Spark session: {str(e)}")