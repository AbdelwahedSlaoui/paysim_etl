import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

@pytest.fixture(scope="module")
def spark():
    """Create a minimal Spark session for testing."""
    spark = SparkSession.builder.appName("GE_Test").getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def transaction_schema():
    """Basic transaction schema for testing."""
    return StructType([
        StructField("type", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("oldbalanceOrg", DoubleType(), True),
        StructField("newbalanceOrig", DoubleType(), True)
    ])

@pytest.fixture
def create_test_df(spark, transaction_schema):
    """Factory fixture for creating test dataframes."""
    def _create_df(data):
        return spark.createDataFrame(data, transaction_schema)
    return _create_df

def test_valid_transaction_types(spark, create_test_df):
    """Verify validation passes for valid transaction types."""
    from quality.expectations.ge_suite import validate_transaction_types

    data = [
        ("PAYMENT", 100.0, 200.0, 100.0),
        ("TRANSFER", 200.0, 300.0, 100.0),
        ("CASH_OUT", 150.0, 150.0, 0.0)
    ]

    assert validate_transaction_types(create_test_df(data)) is True

def test_invalid_transaction_types(spark, create_test_df):
    """Verify validation fails for invalid transaction data."""
    from quality.expectations.ge_suite import validate_transaction_types

    invalid_cases = [
        [("INVALID_TYPE", 100.0, 200.0, 100.0)],  # Invalid type
        [(None, 100.0, 200.0, 100.0)],            # Null type
        [("PAYMENT", None, 200.0, 100.0)]         # Null amount
    ]

    for case in invalid_cases:
        assert validate_transaction_types(create_test_df(case)) is False

def test_balance_validation(spark, create_test_df):
    """Verify balance validation logic."""
    from quality.expectations.ge_suite import validate_balance_consistency

    # Test both valid and invalid balance scenarios
    data = [("PAYMENT", 100.0, 200.0, 100.0)]  # Valid: 200 - 100 = 100
    assert validate_balance_consistency(create_test_df(data)) is True

    data = [("PAYMENT", 100.0, 200.0, 50.0)]   # Invalid: 200 - 100 ≠ 50
    assert validate_balance_consistency(create_test_df(data)) is False