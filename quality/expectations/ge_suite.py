from great_expectations.core import ExpectationConfiguration, ExpectationSuite
from great_expectations.dataset import SparkDFDataset
from typing import List, Optional

def create_transaction_suite(suite_name: str = "transaction_check") -> ExpectationSuite:
    """Create an expectations suite for transaction validation with updated transaction types."""
    suite = ExpectationSuite(expectation_suite_name=suite_name)

    # Core data quality expectations with updated transaction types
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

    # Numerical validation expectations
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
    """Validate financial transaction data against defined quality expectations."""
    suite = create_transaction_suite()
    ge_dataset = SparkDFDataset(df)
    results = ge_dataset.validate(expectation_suite=suite)
    return results.success


def validate_balance_consistency(df) -> bool:
    """
    Validate balance calculations consistency in transactions.
    For PAYMENT transactions, verifies that new balance = old balance - amount
    """
    ge_dataset = SparkDFDataset(df)

    # First check that balance fields are not null
    base_check = ge_dataset.expect_column_values_to_not_be_null("oldbalanceOrg").success and \
                 ge_dataset.expect_column_values_to_not_be_null("newbalanceOrig").success

    if not base_check:
        return False

    # For payment transactions, verify balance arithmetic
    payment_df = df.filter(df.type == "PAYMENT")
    if payment_df.count() > 0:
        computed_balance = payment_df.select(
            (payment_df.oldbalanceOrg - payment_df.amount).alias("computed_balance")
        )

        # Allow for small floating point differences
        balance_diff = computed_balance.first().computed_balance - payment_df.first().newbalanceOrig
        return abs(balance_diff) < 0.01

    return True