from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, abs as spark_abs

def create_silver_layer(spark: SparkSession, input_path: str, output_path: str) -> int:
    """Transform and validate transaction data for the silver layer.

    Args:
        spark: Active Spark session
        input_path: Location of input parquet files
        output_path: Destination for validated data

    Returns:
        int: Count of processed records

    Raises:
        ValueError: If invalid transaction types are found
    """
    # Read input data
    df = spark.read.parquet(input_path)

    # Validate transaction types
    valid_types = ["PAYMENT", "TRANSFER", "CASH_OUT", "CASH_IN", "DEBIT"]
    invalid_records = df.filter(~col("type").isin(valid_types))

    if invalid_records.count() > 0:
        type_counts = invalid_records.groupBy("type").count().collect()
        error_details = "\n".join(f"- {row['type']}: {row['count']} records"
                                for row in type_counts)
        raise ValueError(f"Invalid transaction types found:\n{error_details}")

    # Apply balance validation rules
    df_validated = df.withColumn(
        "balance_check",
        when(
            (col("type") == "PAYMENT") &
            (spark_abs(col("oldbalanceOrg") - col("amount") - col("newbalanceOrig")) >
             col("amount") * 0.01),
            "payment_fee_exceeded"
        ).when(
            (col("type") == "TRANSFER") &
            (spark_abs(col("oldbalanceOrg") - col("amount") - col("newbalanceOrig")) >
             col("amount") * 0.02),
            "transfer_fee_exceeded"
        ).when(
            (col("type") == "CASH_OUT") &
            (spark_abs(col("oldbalanceOrg") - col("amount") - col("newbalanceOrig")) >
             col("amount") * 0.03),
            "cashout_fee_exceeded"
        ).otherwise("valid")
    )

    # Log balance discrepancies if any
    discrepancies = df_validated.filter(col("balance_check") != "valid")
    if discrepancies.count() > 0:
        print("\nBalance Validation Summary:")
        discrepancies.groupBy("type", "balance_check").count().show()

    # Prepare final dataset
    df_final = (df_validated
        .drop("balance_check")
        .dropDuplicates()
        .na.fill(0, ["amount", "oldbalanceOrg", "newbalanceOrig"])
    )

    # Write output with optimized partitioning
    df_final.write.mode("overwrite").parquet(output_path)

    return df_final.count()