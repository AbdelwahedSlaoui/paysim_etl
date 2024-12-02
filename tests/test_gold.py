import pytest
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


@pytest.fixture(scope="session")
def spark():
    from src.utils.spark_setup import create_spark_session

    spark = create_spark_session("TestGoldETL")
    yield spark
    spark.stop()


def test_create_gold_layer(spark, tmp_path):
    """Test gold layer aggregations with minimal test data."""
    from src.transformations.gold import create_gold_layer

    # Create test data
    data = [("PAYMENT", 100.0), ("PAYMENT", 200.0)]
    schema = StructType(
        [
            StructField("type", StringType(), True),
            StructField("amount", DoubleType(), True),
        ]
    )

    # Setup and process
    input_path = f"{tmp_path}/silver"
    output_path = f"{tmp_path}/gold"
    spark.createDataFrame(data, schema).write.parquet(input_path)

    row_count = create_gold_layer(spark, input_path, output_path)
    result = spark.read.parquet(output_path).collect()[0]

    # Verify core functionality
    assert row_count == 1
    assert result["type"] == "PAYMENT"
    assert pytest.approx(result["total_amount"]) == 300.0
    assert result["transaction_count"] == 2
