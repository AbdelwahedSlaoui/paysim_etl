from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count


def create_gold_layer(spark: SparkSession, input_path: str, output_path: str) -> int:
    """Transform silver layer data into business-level aggregations."""
    df_agg = (
        spark.read.parquet(input_path)
        .groupBy("type")
        .agg(
            sum("amount").alias("total_amount"),
            count("type").alias("transaction_count"),
        )
    )

    df_agg.write.mode("overwrite").parquet(output_path)
    return df_agg.count()
