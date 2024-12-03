from great_expectations.core import ExpectationConfiguration, ExpectationSuite
from great_expectations.dataset import SparkDFDataset

def create_validation_suite() -> ExpectationSuite:
    """Create core validation expectations for transaction data."""
    suite = ExpectationSuite(expectation_suite_name="transaction_validation")

    # Transaction type validation
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "type"}
        )
    )
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "type",
                "value_set": ["PAYMENT", "TRANSFER", "CASH_OUT", "CASH_IN", "DEBIT"]
            }
        )
    )

    # Amount validation
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "amount"}
        )
    )
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "amount",
                "min_value": 0,
                "strict_min": True
            }
        )
    )

    return suite

def validate_transaction_types(df) -> bool:
    """Validate transaction data types and amounts."""
    return SparkDFDataset(df).validate(
        expectation_suite=create_validation_suite()
    ).success

def validate_balance_consistency(df) -> bool:
    """Validate transaction balance arithmetic for PAYMENT transactions."""
    payment_df = df.filter(df.type == "PAYMENT")

    if payment_df.count() == 0:
        return True

    row = payment_df.first()
    expected_balance = row.oldbalanceOrg - row.amount

    # Allow small floating-point differences (0.01 tolerance)
    return abs(expected_balance - row.newbalanceOrig) < 0.01