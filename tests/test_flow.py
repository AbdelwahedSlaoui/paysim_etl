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
    """Create minimal test dataset."""
    test_file = tmp_path / "test.csv"
    test_file.write_text("""step,type,amount,nameOrig,oldbalanceOrg,newbalanceOrig,nameDest,oldbalanceDest,newbalanceDest,isFraud,isFlaggedFraud
1,PAYMENT,9839.64,C1231006815,170136.0,160296.36,M1979787155,0.0,0.0,0,0""")
    return str(test_file), str(tmp_path)

def verify_gold_metrics(df):
    """Verify gold layer metrics meet business requirements."""
    required_metrics = {
        'type', 'transaction_count', 'avg_amount_k', 'total_volume_m',
        'avg_balance_change_pct', 'median_amount_k', 'zero_balance_count',
        'volume_to_balance_ratio', 'percentage_of_total'
    }

    # Verify metrics exist
    assert set(df.columns).issuperset(required_metrics), \
        f"Missing required metrics: {required_metrics - set(df.columns)}"

    # Verify business rules
    row = df.collect()[0]
    assert row['transaction_count'] > 0, "No transactions found"
    assert 0 < row['percentage_of_total'] <= 100, "Invalid percentage"
    assert row['avg_amount_k'] > 0, "Invalid average amount"

def test_etl_pipeline(test_data, monkeypatch):
    """Verify end-to-end ETL pipeline execution."""
    from flows.paysim_flow import paysim_pipeline
    monkeypatch.setenv("PREFECT_API_URL", "")

    # Run pipeline
    result = paysim_pipeline(test_data[0])

    # Verify outputs
    spark = SparkSession.builder.appName("TestPipeline").getOrCreate()
    try:
        for layer, path in result.items():
            if not path or 'path' not in layer:
                continue

            # Basic layer validation
            df = spark.read.parquet(path)
            assert df.count() > 0, f"Empty dataset in {layer}"
            assert 'type' in df.columns, f"Missing type column in {layer}"

            # Gold layer specific validation
            if 'gold' in layer:
                verify_gold_metrics(df)
    finally:
        spark.stop()