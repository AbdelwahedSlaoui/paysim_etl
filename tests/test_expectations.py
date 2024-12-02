import pytest
from pyspark.sql import SparkSession
from quality.expectations.basic_suite import validate_transaction_types


@pytest.fixture
def spark():
    """Create a basic Spark session for testing."""
    spark = SparkSession.builder.appName("Test").getOrCreate()
    yield spark
    spark.stop()


def test_valid_transactions(spark):
    """Test with valid transaction data."""
    good_data = [("PAYMENT", 100), ("TRANSFER", 200)]
    df = spark.createDataFrame(good_data, ["type", "amount"])
    assert validate_transaction_types(df) is True


def test_invalid_transactions(spark):
    """Test with invalid transaction data (contains null)."""
    bad_data = [("PAYMENT", 100), (None, 200)]
    df = spark.createDataFrame(bad_data, ["type", "amount"])
    assert validate_transaction_types(df) is False
