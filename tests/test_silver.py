import pytest
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    from src.utils.spark_setup import create_spark_session
    spark = create_spark_session("TestSilverETL")
    yield spark
    spark.stop()

@pytest.fixture
def test_schema():
    """Define the test data schema."""
    return StructType([
        StructField("type", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("oldbalanceOrg", DoubleType(), True),
        StructField("newbalanceOrig", DoubleType(), True),
    ])

@pytest.fixture
def create_test_data(spark, test_schema, tmp_path):
    """Factory fixture to create test datasets."""
    def _create_data(records, name="test_data"):
        df = spark.createDataFrame(records, test_schema)
        path = f"{tmp_path}/{name}"
        df.write.mode("overwrite").parquet(path)
        return str(path)
    return _create_data

def test_valid_transactions(spark, create_test_data):
    """Test silver layer processing with valid transaction data."""
    from src.transformations.silver import create_silver_layer

    # Valid test cases
    valid_records = [
        ("PAYMENT", 100.0, 200.0, 100.0),  # Standard payment
        ("TRANSFER", 200.0, 400.0, 200.0)  # Standard transfer
    ]

    input_path = create_test_data(valid_records)
    output_path = f"{input_path}_output"

    # Process and validate
    row_count = create_silver_layer(spark, input_path, output_path)
    result = spark.read.parquet(output_path)

    assert row_count == 2
    assert result.count() == 2
    assert set(result.select("type").distinct().toPandas()["type"]) == {"PAYMENT", "TRANSFER"}

def test_invalid_transactions(spark, create_test_data):
    """Test silver layer validation with invalid transaction data."""
    from src.transformations.silver import create_silver_layer

    # Invalid test cases
    invalid_records = [
        ("INVALID_TYPE", 100.0, 100.0, 50.0),
        ("PAYMENT", -50.0, 200.0, 100.0)
    ]

    input_path = create_test_data(invalid_records, "invalid_data")
    output_path = f"{input_path}_output"

    with pytest.raises(ValueError) as exc_info:
        create_silver_layer(spark, input_path, output_path)

    assert "invalid transaction types" in str(exc_info.value).lower()