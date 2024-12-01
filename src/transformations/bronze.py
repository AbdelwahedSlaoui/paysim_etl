from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import logging

def create_bronze_layer(spark: SparkSession, input_path: str, output_path: str) -> bool:
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        # Define complete schema based on sample data
        schema = StructType([
            StructField("step", IntegerType(), True),
            StructField("type", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("nameOrig", StringType(), True),
            StructField("oldbalanceOrg", DoubleType(), True),
            StructField("newbalanceOrig", DoubleType(), True),
            StructField("nameDest", StringType(), True),
            StructField("oldbalanceDest", DoubleType(), True),
            StructField("newbalanceDest", DoubleType(), True),
            StructField("isFraud", IntegerType(), True),
            StructField("isFlaggedFraud", IntegerType(), True)
        ])