from prefect import flow, task
from datetime import datetime
from pathlib import Path
from src.utils.spark_setup import create_spark_session, stop_spark_session
from src.utils.log_config import setup_logging
from src.transformations.bronze import create_bronze_layer
from src.transformations.silver import create_silver_layer
from src.transformations.gold import create_gold_layer

# Configuration
RAW_FILE_PATH = "data/raw/paysim_kaggle.csv"

@task(retries=3, retry_delay_seconds=300, log_prints=False)
def process_layer(logger, layer_name: str, transform_func, input_path: str, base_path: str) -> str:
    """Generic task to process any ETL layer."""
    spark = create_spark_session(f"{layer_name.capitalize()}ETL")
    try:
        output_path = str(Path(base_path) / layer_name /
                         datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))
        Path(output_path).mkdir(parents=True, exist_ok=True)

        logger.info(f"Starting {layer_name} layer processing...")
        row_count = transform_func(spark, input_path, output_path)
        logger.info(f"Processed {row_count:,} rows in {layer_name} layer -> {output_path}")

        return output_path
    finally:
        stop_spark_session(spark)

@flow(log_prints=False)
def paysim_pipeline(input_path: str = RAW_FILE_PATH) -> dict:
    """Execute the full ETL pipeline with domain-separated logging."""
    logger = setup_logging()
    logger.info("Starting PaySim ETL pipeline execution...")

    start_time = datetime.now()
    base_path = str(Path("data").absolute())
    input_path = str(Path(input_path).absolute())

    try:
        bronze_path = process_layer(logger, "bronze", create_bronze_layer, input_path, base_path)
        silver_path = process_layer(logger, "silver", create_silver_layer, bronze_path, base_path)
        gold_path = process_layer(logger, "gold", create_gold_layer, silver_path, base_path)

        duration = datetime.now() - start_time
        logger.info(f"\nPipeline completed in {duration}")

        logger.info("\nOutput locations:")
        layers = {
            "bronze": bronze_path,
            "silver": silver_path,
            "gold": gold_path
        }
        for layer, path in layers.items():
            logger.info(f"{layer}: {path}")

        return {
            "bronze_path": bronze_path,
            "silver_path": silver_path,
            "gold_path": gold_path,
            "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "duration": str(duration)
        }
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise
    finally:
        logger.info("Pipeline execution completed.")

if __name__ == "__main__":
    result = paysim_pipeline()