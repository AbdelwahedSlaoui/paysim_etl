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
def valid_input_data(spark, tmp_path):
    """Create test data that passes quality validations."""
    df = spark.createDataFrame(
        [
            ("PAYMENT", 100.0, 100.0, 0.0),  # Valid record
            ("TRANSFER", 200.0, 200.0, 0.0),  # Another valid record
        ],
        StructType([
            StructField("type", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("oldbalanceOrg", DoubleType(), True),
            StructField("newbalanceOrig", DoubleType(), True),
        ])
    )

    path = f"{tmp_path}/bronze_valid"
    df.write.mode("overwrite").parquet(path)
    return str(path)


@pytest.fixture
def valid_input_data(spark, tmp_path):
    """Create test data that passes quality validations."""
    df = spark.createDataFrame(
        [
            ("PAYMENT", 100.0, 100.0, 100.0),  # Valid record - balanced
            ("TRANSFER", 200.0, 200.0, 200.0),  # Another valid record - balanced
        ],
        StructType([
            StructField("type", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("oldbalanceOrg", DoubleType(), True),
            StructField("newbalanceOrig", DoubleType(), True),
        ])
    )

    path = f"{tmp_path}/bronze_valid"
    df.write.mode("overwrite").parquet(path)
    return str(path)


@pytest.fixture
def invalid_input_data(spark, tmp_path):
    """Create test data that fails quality validations."""
    df = spark.createDataFrame(
        [
            ("INVALID_TYPE", 100.0, 100.0, 50.0),  # Invalid type and unbalanced
            ("PAYMENT", -50.0, 200.0, 100.0),  # Negative amount and unbalanced
        ],
        StructType([
            StructField("type", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("oldbalanceOrg", DoubleType(), True),
            StructField("newbalanceOrig", DoubleType(), True),
        ])
    )

    path = f"{tmp_path}/bronze_invalid"
    df.write.mode("overwrite").parquet(path)
    return str(path)


def test_create_silver_layer_valid_data(spark, valid_input_data, tmp_path):
    """Test silver layer creation with valid data."""
    from src.transformations.silver import create_silver_layer

    output_path = f"{tmp_path}/silver"
    row_count = create_silver_layer(spark, valid_input_data, output_path)

    result = spark.read.parquet(output_path)

    # Validate transformations
    assert row_count == 2  # Expected number of valid records
    assert not result.filter(col("type").isNull()).count()
    assert not result.filter(col("amount").isNull()).count()


def test_create_silver_layer_invalid_data(spark, invalid_input_data, tmp_path):
    """Test silver layer creation with invalid data."""
    from src.transformations.silver import create_silver_layer

    output_path = f"{tmp_path}/silver"

    # Expect validation error
    with pytest.raises(ValueError) as exc_info:
        create_silver_layer(spark, invalid_input_data, output_path)

    assert "Failed transaction type validation" in str(exc_info.value)