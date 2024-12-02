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
        round(avg(col("amount") / 1000), 2).alias("avg_amount_k"),
        round(sum(col("amount") / 1000000), 2).alias("total_volume_m"),

        # Balance impact analysis
        round(
            avg((col("newbalanceOrig") - col("oldbalanceOrg")) /
                when(col("oldbalanceOrg") != 0, col("oldbalanceOrg")).otherwise(1) * 100),
            2
        ).alias("avg_balance_change_pct"),

        # Transaction size distribution
        round(percentile_approx("amount", 0.5) / 1000, 2).alias("median_amount_k"),
        round(percentile_approx("amount", 0.9) / 1000, 2).alias("large_txn_threshold_k"),

        # Account behavior patterns
        sum(when(col("oldbalanceOrg") == 0, 1).otherwise(0)).alias("zero_balance_count"),

        # ratio metrics
        round(
            avg(when(col("amount") > 10000, 1).otherwise(0)) * 100,
            2
        ).alias("large_txn_ratio_pct"),

        # Relative balance impact
        round(
            sum(col("amount")) / sum(col("oldbalanceOrg")),
            4
        ).alias("volume_to_balance_ratio")
    )

    # Add percentage distribution
    total_transactions = df.count()
    df_behavior_analysis = df_behavior_analysis.withColumn(
        "percentage_of_total",
        round(col("transaction_count") / total_transactions * 100, 2)
    )

    df_behavior_analysis.write.mode("overwrite").parquet(f"{output_path}")
    return df_behavior_analysis.count()  # Return count of analyzed transaction types