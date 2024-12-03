"""Verify the complete development environment setup."""
import sys
from datetime import datetime


def verify_environment():
    """Run a comprehensive check of all installed components."""
    start = datetime.now()
    results = []

    # Core Components
    try:
        import prefect

        results.append(f"✓ Prefect {prefect.__version__}")
    except ImportError as e:
        results.append(f"⚠️ Prefect import failed: {str(e)}")

    try:
        import pyspark
        from pyspark.sql import SparkSession

        # Test Spark functionality
        spark = SparkSession.builder.appName("VerifySetup").getOrCreate()
        spark.createDataFrame([("test",)], ["col"]).count()
        results.append(f"✓ PySpark {pyspark.__version__} (with working SparkSession)")
    except Exception as e:
        results.append(f"⚠️ PySpark verification failed: {str(e)}")

    try:
        import duckdb

        results.append(f"✓ DuckDB {duckdb.__version__}")
    except ImportError as e:
        results.append(f"⚠️ DuckDB import failed: {str(e)}")

    # Testing & Quality Tools
    try:
        import great_expectations as ge
        import pytest

        results.append(
            f"✓ Testing tools (pytest {pytest.__version__}, great_expectations {ge.__version__})"
        )
    except ImportError as e:
        results.append(f"⚠️ Testing tools import failed: {str(e)}")

    # Development Tools
    try:
        import black
        import isort
        import mypy
        import ruff

        results.append("✓ Development tools (black, isort, ruff, mypy)")
    except ImportError as e:
        results.append(f"⚠️ Development tools import failed: {str(e)}")

    # Print Results
    print("\nEnvironment Verification Results:")
    print("================================")
    for result in results:
        print(result)

    duration = datetime.now() - start
    print(f"\nPython version: {sys.version.split()[0]}")
    print(f"Verification completed in {duration.total_seconds():.2f}s")


if __name__ == "__main__":
    verify_environment()
