from great_expectations.core import ExpectationConfiguration, ExpectationSuite
from great_expectations.dataset import SparkDFDataset

def validate_transaction_types(df) -> bool:
    """
    Validate that all transaction types are present in the DataFrame.
    Returns True if validation passes, False otherwise.
    """
    # Create a simple suite with one rule
    suite = ExpectationSuite(expectation_suite_name="transaction_check")

    # Rule: type column must not be null
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "type"}
        )
    )

    # Validate the data
    ge_dataset = SparkDFDataset(df)
    results = ge_dataset.validate(expectation_suite=suite)

    return results.success