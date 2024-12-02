import pytest
from pathlib import Path
from prefect.testing.utilities import prefect_test_harness
from pyspark.sql import SparkSession

@pytest.fixture(autouse=True)
def prefect_test_fixture():
    with prefect_test_harness():
        yield

@pytest.fixture
def test_data(tmp_path):
    """Create test data and directory structure."""
    test_file = tmp_path / "test.csv"
    test_file.write_text("""step,type,amount,nameOrig,oldbalanceOrg,newbalanceOrig,nameDest,oldbalanceDest,newbalanceDest,isFraud,isFlaggedFraud
1,PAYMENT,9839.64,C1231006815,170136.0,160296.36,M1979787155,0.0,0.0,0,0""")
    return str(test_file), str(tmp_path)

def test_etl_pipeline(test_data, monkeypatch):
    """Test the complete ETL pipeline execution."""
    from flows.paysim_flow import etl_pipeline
    monkeypatch.setenv("PREFECT_API_URL", "")

    input_file, base_dir = test_data
    result = etl_pipeline(input_file)

    # Verify outputs with PySpark
    spark = SparkSession.builder.appName("TestPipeline").getOrCreate()
    try:
        for layer, path in result.items():
            if 'path' in layer:
                # Verify data exists and has expected schema
                df = spark.read.parquet(path)
                assert df.count() > 0
                assert 'type' in df.columns

                # Verify gold layer aggregations
                if 'gold' in layer:
                    assert all(col in df.columns for col in
                             ['total_amount', 'transaction_count'])
    finally:
        spark.stop()