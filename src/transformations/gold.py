from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    sum, count, col, avg, round, percentile_approx,
    when
)


def create_gold_layer(spark: SparkSession, input_path: str, output_path: str) -> int:
    """Transform silver layer data into business-level aggregations.
    The transformation creates an analytical view of behavioral patterns
    """
    # Read the input data
    df = spark.read.parquet(input_path)

    # Enhanced behavioral analysis
    df_behavior_analysis = df.groupBy("type").agg(
        # Volume metrics
        count("*").alias("transaction_count"),
        round(avg("amount"), 2).alias("avg_amount"),
        round(sum("amount"), 2).alias("total_volume"),

        # Balance impact analysis
        round(avg(col("newbalanceOrig") - col("oldbalanceOrg")), 2).alias("avg_balance_impact"),

        # Transaction size distribution
        round(percentile_approx("amount", 0.5), 2).alias("median_amount"),
        round(percentile_approx("amount", 0.9), 2).alias("large_txn_threshold"),

        # Account behavior patterns
        sum(when(col("oldbalanceOrg") == 0, 1).otherwise(0)).alias("zero_balance_count")
    )

    # Add percentage distribution
    total_transactions = df.count()
    df_behavior_analysis = df_behavior_analysis.withColumn(
        "percentage_of_total",
        round(col("transaction_count") / total_transactions * 100, 2)
    )

    df_behavior_analysis.write.mode("overwrite").parquet(f"{output_path}")
    return df_behavior_analysis.count()  # Return count of analyzed transaction types