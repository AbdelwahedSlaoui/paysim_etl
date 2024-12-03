import pytest
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


@pytest.fixture(scope="session")
def spark():
    """Create test Spark session."""
    from src.utils.spark_setup import create_spark_session

    spark = create_spark_session("TestGoldETL")
    yield spark
    spark.stop()


def test_create_gold_layer(spark, tmp_path):
    """Test gold layer aggregations with simple transaction data."""
    from src.transformations.gold import create_gold_layer

    # Simple test data with known outcomes
    test_data = [
        ("PAYMENT", 1000.0, 2000.0, 1000.0),  # 50% balance reduction
        ("PAYMENT", 1000.0, 2000.0, 1000.0),  # 50% balance reduction
    ]

    # Basic schema for required fields
    schema = StructType(
        [
            StructField("type", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("oldbalanceOrg", DoubleType(), True),
            StructField("newbalanceOrig", DoubleType(), True),
        ]
    )

    # Process test data
    input_path = f"{tmp_path}/silver"
    output_path = f"{tmp_path}/gold"
    spark.createDataFrame(test_data, schema).write.parquet(input_path)

    row_count = create_gold_layer(spark, input_path, output_path)
    result = spark.read.parquet(output_path).collect()[0]

    # Group assertions by metric type
    assert row_count == 1  # One group (PAYMENT)

    # Volume metrics
    assert result["transaction_count"] == 2
    assert result["avg_amount_k"] == 1.0
    assert result["total_volume_m"] == 0.002

    # Balance metrics
    assert result["avg_balance_change_pct"] == -50.0
    assert result["volume_to_balance_ratio"] == 0.5

    # Transaction metrics
    assert result["median_amount_k"] == 1.0
    assert result["zero_balance_count"] == 0
    assert result["percentage_of_total"] == 100.0
