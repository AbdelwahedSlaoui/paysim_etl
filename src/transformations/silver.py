from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, when, monotonically_increasing_id,
    abs as spark_abs, sum as spark_sum, avg as spark_avg
)
from quality.expectations.ge_suite import validate_transaction_types

def create_silver_layer(spark: SparkSession, input_path: str, output_path: str) -> int:
    """Transform bronze layer data with nuanced balance validation."""

    df = spark.read.option("mergeSchema", "true").parquet(input_path)

    # Transaction type validation (unchanged)
    invalid_types = df.filter(~col("type").isin(
        ["PAYMENT", "TRANSFER", "CASH_OUT", "CASH_IN", "DEBIT"]
    )).count()

    if invalid_types > 0:
        type_distribution = df.filter(
            ~col("type").isin(["PAYMENT", "TRANSFER", "CASH_OUT", "CASH_IN", "DEBIT"])
        ).groupBy("type").count().collect()

        error_msg = f"Found {invalid_types} records with invalid transaction types:\n"
        for row in type_distribution:
            error_msg += f"Type: {row['type']}, Count: {row['count']}\n"
        raise ValueError(error_msg)

    # Process valid data
    df_clean = (df
        .dropDuplicates()
        .repartition(8, "type")
    )

    # Enhanced balance consistency check with transaction-specific rules
    balance_analysis = (df_clean.withColumn(
        "balance_mismatch",
        when(
            # PAYMENT transactions: Allow for small fees (up to 1% of transaction)
            (col("type") == "PAYMENT") &
            (col("oldbalanceOrg") > 0) &
            (spark_abs(col("oldbalanceOrg") - col("amount") - col("newbalanceOrig")) >
             spark_abs(col("amount") * 0.01)),
            "payment_mismatch"
        ).when(
            # TRANSFER: Allow for transfer fees (up to 2% of transaction)
            (col("type") == "TRANSFER") &
            (col("oldbalanceOrg") > 0) &
            (spark_abs(col("oldbalanceOrg") - col("amount") - col("newbalanceOrig")) >
             spark_abs(col("amount") * 0.02)),
            "transfer_mismatch"
        ).when(
            # CASH_OUT: Most flexible due to potential service charges
            (col("type") == "CASH_OUT") &
            (col("oldbalanceOrg") > 0) &
            (spark_abs(col("oldbalanceOrg") - col("amount") - col("newbalanceOrig")) >
             spark_abs(col("amount") * 0.03)),
            "cashout_mismatch"
        ).otherwise("valid")
    ))

    # Collect and analyze inconsistencies
    inconsistencies = (balance_analysis
        .filter(col("balance_mismatch") != "valid")
        .groupBy("type", "balance_mismatch")
        .agg(
            count("*").alias("mismatch_count"),
            spark_avg(col("amount")).alias("avg_amount"),
            spark_avg(
                spark_abs(col("oldbalanceOrg") - col("amount") - col("newbalanceOrig"))
            ).alias("avg_discrepancy")
        )
        .collect()
    )

    if inconsistencies:
        print("\nBalance Analysis Summary:")
        for row in inconsistencies:
            print(f"""
Type: {row['type']}
- Mismatch type: {row['balance_mismatch']}
- Count: {row['mismatch_count']}
- Average amount: {row['avg_amount']:.2f}
- Average discrepancy: {row['avg_discrepancy']:.2f}
""")

    # Final transformations for valid data
    df_final = (df_clean
        .na.fill(0, ["amount", "oldbalanceOrg", "newbalanceOrig"])
        .repartition(8)
    )

    df_final.write.mode("overwrite").option(
        "maxRecordsPerFile", "500000"
    ).parquet(output_path)

    return df_final.count()