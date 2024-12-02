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

    # Create test data with carefully calculated balance changes
    data = [
        # type, amount, oldbalanceOrg, newbalanceOrig
        ("PAYMENT", 100000.0, 300000.0, 200000.0),  # -33.33% change
        ("PAYMENT", 200000.0, 600000.0, 400000.0),  # -33.33% change
        ("PAYMENT", 150000.0, 450000.0, 300000.0)   # -33.33% change
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
    assert row_count == 1
    assert result["type"] == "PAYMENT"
    assert result["transaction_count"] == 3
    assert pytest.approx(result["avg_amount_k"]) == 150.0
    assert pytest.approx(result["total_volume_m"]) == 0.45
    assert pytest.approx(result["avg_balance_change_pct"]) == -33.33
    assert pytest.approx(result["median_amount_k"]) == 150.0
    assert pytest.approx(result["large_txn_threshold_k"]) == 200.0
    assert result["zero_balance_count"] == 0
    assert pytest.approx(result["large_txn_ratio_pct"]) == 100.0
    assert pytest.approx(result["volume_to_balance_ratio"], rel=0.01) == 0.333
    assert pytest.approx(result["percentage_of_total"]) == 100.0