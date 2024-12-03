from datetime import datetime
from pathlib import Path

from prefect import flow, task

from src.transformations.bronze import create_bronze_layer
from src.transformations.gold import create_gold_layer
from src.transformations.silver import create_silver_layer
from src.utils.core_security import audit_log
from src.utils.log_config import setup_logging
from src.utils.spark_setup import create_spark_session, stop_spark_session


class ETLPaths:
    """Manage ETL paths and directories."""

    def __init__(self, base_dir="data"):
        self.base = Path(base_dir).absolute()

    def layer_path(self, layer_name: str) -> str:
        """Generate timestamped path for layer output."""
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        path = self.base / layer_name / timestamp
        path.mkdir(parents=True, exist_ok=True)
        return str(path)


@task(retries=3, retry_delay_seconds=300)
def process_layer(
    logger, layer_name: str, transform_func, input_path: str, paths: ETLPaths
) -> str:
    """Process a single ETL layer with error handling."""
    spark = create_spark_session(f"{layer_name.capitalize()}ETL")
    try:
        output_path = paths.layer_path(layer_name)
        logger.info(f"Processing {layer_name} layer...")

        row_count = transform_func(spark, input_path, output_path)
        logger.info(f"Processed {row_count:,} rows -> {output_path}")

        return output_path
    finally:
        stop_spark_session(spark)


@flow
@audit_log("pipeline_execution")
def paysim_pipeline(input_path: str = "data/raw/paysim_kaggle.csv") -> dict:
    """Execute ETL pipeline with structured logging and error handling."""
    logger = setup_logging()
    logger.info("Starting PaySim ETL pipeline...")

    start_time = datetime.now()
    paths = ETLPaths()
    input_path = str(Path(input_path).absolute())

    try:
        # Process each layer sequentially
        bronze_path = process_layer(
            logger, "bronze", create_bronze_layer, input_path, paths
        )
        silver_path = process_layer(
            logger, "silver", create_silver_layer, bronze_path, paths
        )
        gold_path = process_layer(logger, "gold", create_gold_layer, silver_path, paths)

        # Log completion information
        duration = datetime.now() - start_time
        logger.info(f"Pipeline completed in {duration}")

        # Return execution summary
        return {
            "bronze_path": bronze_path,
            "silver_path": silver_path,
            "gold_path": gold_path,
            "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "duration": str(duration),
        }
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise
    finally:
        logger.info("Pipeline execution completed")


if __name__ == "__main__":
    paysim_pipeline()
