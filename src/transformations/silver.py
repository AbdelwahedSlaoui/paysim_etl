from pyspark.sql import SparkSession


def create_silver_layer(spark: SparkSession, input_path: str, output_path: str) -> int:
    """Transform bronze layer data into silver layer with basic cleaning."""
    # Read and clean data
    df = (
        spark.read.parquet(input_path)
        .dropDuplicates()
        .na.fill(0, ["amount"])
        .filter("type is not null")
    )

    # Write cleaned data
    df.write.mode("overwrite").parquet(output_path)
    return df.count()
