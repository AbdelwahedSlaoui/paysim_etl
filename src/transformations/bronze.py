from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

def create_bronze_layer(spark: SparkSession, input_path: str, output_path: str) -> int:
    """Load CSV data into Parquet format with basic schema validation."""
    schema = StructType([
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
        StructField("isFlaggedFraud", IntegerType(), True)
    ])

    # Ensure paths are absolute
    input_path = str(Path(input_path).absolute())
    output_path = str(Path(output_path).absolute())

    # Read with local file path
    df = spark.read.option("header", "true").schema(schema).csv(f"file://{input_path}")

    # Write with local file path
    df.write.mode("overwrite").parquet(f"file://{output_path}")

    return df.count()