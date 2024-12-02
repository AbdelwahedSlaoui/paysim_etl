import pytest
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col

@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    from src.utils.spark_setup import create_spark_session
    spark = create_spark_session("TestSilverETL")
    yield spark
    spark.stop()

@pytest.fixture
def input_data(spark, tmp_path):
    """Create test data with validation scenarios."""
    df = spark.createDataFrame(
        [
            ("PAYMENT", 100.0),   # Valid record
            ("TRANSFER", None),   # Null amount
            (None, 200.0),        # Null type
            ("PAYMENT", 100.0)    # Duplicate
        ],
        StructType([
            StructField("type", StringType(), True),
            StructField("amount", DoubleType(), True)
        ])
    )

    path = f"{tmp_path}/bronze"
    df.write.mode("overwrite").parquet(path)
    return str(path)

def test_create_silver_layer(spark, input_data, tmp_path):
    """Test silver layer transformations."""
    from src.transformations.silver import create_silver_layer

    output_path = f"{tmp_path}/silver"
    row_count = create_silver_layer(spark, input_data, output_path)

    result = spark.read.parquet(output_path)

    # Validate transformations
    assert row_count == 2  # Deduped records with non-null type
    assert not result.filter(col("type").isNull()).count()
    assert not result.filter(col("amount").isNull()).count()