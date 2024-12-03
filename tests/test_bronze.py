import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session configured for local testing."""
    spark = (
        SparkSession.builder.appName("TestETL")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.sql.execution.arrow.enabled", "true")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )

    yield spark
    spark.stop()


def test_create_bronze_layer(spark, tmp_path):
    """Test bronze layer transformation with minimal valid data."""
    from src.transformations.bronze import create_bronze_layer

    # Create test input file
    input_file = tmp_path / "test.csv"
    input_file.write_text(
        """step,type,amount,nameOrig,oldbalanceOrg,newbalanceOrig,nameDest,oldbalanceDest,newbalanceDest,isFraud,isFlaggedFraud
1,PAYMENT,9839.64,C1231006815,170136.0,160296.36,M1979787155,0.0,0.0,0,0"""
    )

    # Setup output path
    output_dir = tmp_path / "bronze"

    # Process data
    row_count = create_bronze_layer(spark, str(input_file), str(output_dir))

    # Validate output
    assert row_count == 1
    df = spark.read.parquet(f"file://{str(output_dir)}")
    assert df.count() == row_count
    assert all(col in df.columns for col in ["type", "amount"])
