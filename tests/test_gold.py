import pytest
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col


@pytest.fixture(scope="session")
def spark():
    from src.utils.spark_setup import create_spark_session

    spark = create_spark_session("TestGoldETL")
    yield spark
    spark.stop()


def test_create_gold_layer(spark, tmp_path):
    """Test gold layer behavioral analysis with representative test data."""
    from src.transformations.gold import create_gold_layer

    # Create test data with balance information
    data = [
        # type, amount, oldbalanceOrg, newbalanceOrig
        ("PAYMENT", 100.0, 500.0, 400.0),    # Regular payment
        ("PAYMENT", 200.0, 400.0, 200.0),    # Another payment
        ("PAYMENT", 150.0, 0.0, 0.0)         # Zero-balance payment
    ]

    schema = StructType([
        StructField("type", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("oldbalanceOrg", DoubleType(), True),
        StructField("newbalanceOrig", DoubleType(), True)
    ])

    # Setup and process
    input_path = f"{tmp_path}/silver"
    output_path = f"{tmp_path}/gold"
    spark.createDataFrame(data, schema).write.parquet(input_path)

    row_count = create_gold_layer(spark, input_path, output_path)
    result = spark.read.parquet(output_path).collect()[0]

    # Verify behavioral analysis metrics
    assert row_count == 1  # One group (PAYMENT type)
    assert result["type"] == "PAYMENT"
    assert result["transaction_count"] == 3
    assert pytest.approx(result["avg_amount"]) == 150.0
    assert pytest.approx(result["total_volume"]) == 450.0
    assert pytest.approx(result["avg_balance_impact"]) == -100.0  # Average balance reduction
    assert pytest.approx(result["median_amount"]) == 150.0
    assert result["zero_balance_count"] == 1  # One zero-balance transaction
    assert pytest.approx(result["percentage_of_total"]) == 100.0  # All transactions are PAYMENT