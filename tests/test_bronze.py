import pytest
from src.utils.spark_setup import create_spark_session
from src.transformations.bronze import create_bronze_layer

@pytest.fixture(scope="session")
def spark():
    """Create a Spark session that will be used for all tests."""
    spark = create_spark_session("TestETL")
    yield spark
    spark.stop()

@pytest.fixture
def test_paths(tmp_path):
    """Create input and output paths with sample data."""
    # Create test file with minimal valid data
    test_file = tmp_path / "test_data.csv"
    sample_content = """step,type,amount,nameOrig,oldbalanceOrg,newbalanceOrig,nameDest,oldbalanceDest,newbalanceDest,isFraud,isFlaggedFraud
1,PAYMENT,9839.64,C1231006815,170136.0,160296.36,M1979787155,0.0,0.0,0,0"""

    test_file.write_text(sample_content)
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    return test_file, output_dir

def test_create_bronze_layer(spark, test_paths):
    """Test the basic functionality of create_bronze_layer."""
    input_file, output_dir = test_paths

    row_count = create_bronze_layer(spark, str(input_file), str(output_dir))

    # Validate basics
    assert row_count > 0
    df = spark.read.parquet(f"file://{str(output_dir)}")
    assert df.count() == row_count
    assert all(col in df.columns for col in ["type", "amount"])