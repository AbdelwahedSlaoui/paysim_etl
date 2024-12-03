from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    sum, count, col, avg, round, when,
    percentile_approx
)

def create_gold_layer(spark: SparkSession, input_path: str, output_path: str) -> int:
    """Create business-level aggregations of transaction patterns."""
    df = spark.read.parquet(input_path)

    # Calculate core business metrics
    metrics = df.groupBy("type").agg(
        # Transaction volumes
        count("*").alias("transaction_count"),
        round(avg(col("amount") / 1000), 2).alias("avg_amount_k"),
        round(sum(col("amount")) / 1000000, 3).alias("total_volume_m"),

        # Transaction patterns
        round(percentile_approx("amount", 0.5) / 1000, 2).alias("median_amount_k"),

        # Balance changes
        round(
            avg((col("newbalanceOrig") - col("oldbalanceOrg")) /
                when(col("oldbalanceOrg") != 0, col("oldbalanceOrg"))
                .otherwise(1) * 100),
            2
        ).alias("avg_balance_change_pct"),

        # Account patterns
        sum(when(col("oldbalanceOrg") == 0, 1).otherwise(0))
        .alias("zero_balance_count"),

        # Volume patterns
        round(sum(col("amount")) / sum(col("oldbalanceOrg")), 4)
        .alias("volume_to_balance_ratio"),

        # Transaction size patterns
        round(avg(when(col("amount") > 10000, 1).otherwise(0)) * 100, 2)
        .alias("large_txn_ratio_pct")
    )

    # Add percentage distribution
    total_txns = df.count()
    metrics = metrics.withColumn(
        "percentage_of_total",
        round(col("transaction_count") / total_txns * 100, 2)
    )

    metrics.write.mode("overwrite").parquet(output_path)
    return metrics.count()