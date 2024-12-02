import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from quality.expectations.ge_suite import validate_transaction_types

@pytest.fixture
def spark():
    """Create a basic Spark session for testing."""
    spark = SparkSession.builder.appName("Test").getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def sample_schema():
    """Define expected schema for transaction data."""
    return StructType([
        StructField("type", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("oldbalanceOrg", DoubleType(), True),
        StructField("newbalanceOrig", DoubleType(), True),
        StructField("oldbalanceDest", DoubleType(), True),
        StructField("newbalanceDest", DoubleType(), True),
        StructField("isFraud", IntegerType(), True)
    ])

def test_valid_transactions(spark):
    """Test with valid transaction data."""
    good_data = [
        ("PAYMENT", 100.0, 200.0, 100.0, 0.0, 100.0, 0),
        ("TRANSFER", 200.0, 300.0, 100.0, 50.0, 250.0, 0),
        ("CASH_OUT", 150.0, 150.0, 0.0, 1000.0, 1150.0, 0)
    ]
    df = spark.createDataFrame(good_data, ["type", "amount", "oldbalanceOrg",
                                         "newbalanceOrig", "oldbalanceDest",
                                         "newbalanceDest", "isFraud"])
    assert validate_transaction_types(df) is True

def test_invalid_transactions(spark):
    """Test with various invalid transaction scenarios."""
    bad_data = [
        ("PAYMENT", None, 200.0, 200.0, 0.0, 0.0, 0),  # Null amount
        (None, 200.0, 200.0, 200.0, 0.0, 0.0, 0),      # Null type
        ("INVALID", 100.0, 200.0, 200.0, 0.0, 0.0, 0)  # Invalid type
    ]
    df = spark.createDataFrame(bad_data, ["type", "amount", "oldbalanceOrg",
                                        "newbalanceOrig", "oldbalanceDest",
                                        "newbalanceDest", "isFraud"])
    assert validate_transaction_types(df) is False

def test_balance_consistency(spark):
    """Test balance calculations consistency."""
    data = [
        # Initial balance - amount = new balance (valid)
        ("PAYMENT", 100.0, 200.0, 100.0, 0.0, 0.0, 0),
        # Inconsistent balance calculation (invalid)
        ("PAYMENT", 100.0, 200.0, 50.0, 0.0, 0.0, 0)
    ]
    df = spark.createDataFrame(data, ["type", "amount", "oldbalanceOrg",
                                    "newbalanceOrig", "oldbalanceDest",
                                    "newbalanceDest", "isFraud"])

    # Verify balance consistency
    consistent_records = df.filter(
        (df.oldbalanceOrg - df.amount == df.newbalanceOrig) |
        (df.oldbalanceOrg == 0.0)  # Allow zero initial balance
    ).count()
    assert consistent_records == 1

def test_fraud_indicators(spark):
    """Test fraud detection patterns."""
    data = [
        # Normal transaction
        ("PAYMENT", 100.0, 200.0, 100.0, 0.0, 100.0, 0),
        # Suspicious: Large amount with account drainage
        ("TRANSFER", 10000.0, 10000.0, 0.0, 0.0, 10000.0, 1)
    ]
    df = spark.createDataFrame(data, ["type", "amount", "oldbalanceOrg",
                                    "newbalanceOrig", "oldbalanceDest",
                                    "newbalanceDest", "isFraud"])

    # Verify fraud flags
    suspicious = df.filter(
        (df.amount > 1000.0) &
        (df.newbalanceOrig == 0.0) &
        (df.type.isin(["TRANSFER", "CASH_OUT"]))
    ).count()
    assert suspicious == 1