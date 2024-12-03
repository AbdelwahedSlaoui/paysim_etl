from pyspark.sql import SparkSession
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType)


def create_bronze_layer(spark: SparkSession, input_path: str, output_path: str) -> int:
    """Load CSV transaction data into Parquet format with schema validation.

    Args:
        spark: Active SparkSession
        input_path: Path to input CSV file
        output_path: Path to write Parquet output

    Returns:
        int: Number of processed rows
    """
    schema = StructType(
        [
            StructField("step", IntegerType(), True),
            StructField("type", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("nameOrig", StringType(), True),
            StructField("oldbalanceOrg", DoubleType(), True),
            StructField("newbalanceOrig", DoubleType(), True),
            StructField("nameDest", StringType(), True),
            StructField("oldbalanceDest", DoubleType(), True),
            StructField("newbalanceDest", DoubleType(), True),
            StructField("isFraud", IntegerType(), True),
            StructField("isFlaggedFraud", IntegerType(), True),
        ]
    )

    # Read CSV with schema validation
    df = spark.read.option("header", "true").schema(schema).csv(f"file://{input_path}")

    # Write to Parquet
    df.write.mode("overwrite").parquet(f"file://{output_path}")

    return df.count()
