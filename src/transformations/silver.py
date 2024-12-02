from pyspark.sql import SparkSession
from quality.expectations.ge_suite import validate_transaction_types, validate_balance_consistency


def create_silver_layer(spark: SparkSession, input_path: str, output_path: str) -> int:
    """Transform bronze layer data into silver layer with quality checks."""

    # Read bronze data
    df = spark.read.parquet(input_path)

    # Apply quality checks
    if not validate_transaction_types(df):
        raise ValueError("Failed transaction type validation")

    if not validate_balance_consistency(df):
        raise ValueError("Failed balance consistency validation")

    # Silver layer transformations
    df_clean = (
        df
        .dropDuplicates()
        .na.fill(0, ["amount"])
        .filter("type is not null")
    )

    # Write cleaned data
    df_clean.write.mode("overwrite").parquet(output_path)
    return df_clean.count()
