from prefect import flow, task
from pyspark.sql import SparkSession
from pathlib import Path
from datetime import datetime

from src.utils.spark_setup import create_spark_session, stop_spark_session
from src.transformations.bronze import create_bronze_layer
from src.transformations.silver import create_silver_layer
from src.transformations.gold import create_gold_layer

@task
def process_layer(layer_name: str, transform_func, input_path: str, base_path: str) -> str:
    """Generic task to process any ETL layer."""
    spark = create_spark_session(f"{layer_name.capitalize()}ETL")
    try:
        output_path = str(Path(base_path) / layer_name /
                         datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))
        Path(output_path).mkdir(parents=True, exist_ok=True)

        row_count = transform_func(spark, input_path, output_path)
        print(f"Processed {row_count} rows in {layer_name} layer -> {output_path}")

        return output_path
    finally:
        stop_spark_session(spark)

@flow
def etl_pipeline(input_path: str = "data/raw/paysim_sample.csv") -> dict:
    """Execute the full ETL pipeline from raw data to gold layer."""
    start_time = datetime.now()
    base_path = str(Path("data").absolute())
    input_path = str(Path(input_path).absolute())

    # Process each layer using the generic task
    bronze_path = process_layer("bronze", create_bronze_layer, input_path, base_path)
    silver_path = process_layer("silver", create_silver_layer, bronze_path, base_path)
    gold_path = process_layer("gold", create_gold_layer, silver_path, base_path)

    return {
        "bronze_path": bronze_path,
        "silver_path": silver_path,
        "gold_path": gold_path,
        "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
        "duration": str(datetime.now() - start_time)
    }

if __name__ == "__main__":
    result = etl_pipeline()
    print(f"\nPipeline completed in {result['duration']}")
    print("\nOutput locations:")
    for layer, path in result.items():
        if 'path' in layer:
            print(f"{layer.split('_')[0]}: {path}")