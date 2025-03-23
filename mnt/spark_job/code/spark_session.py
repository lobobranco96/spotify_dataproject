import os
import logging
from dotenv import load_dotenv

import pyspark
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

S3_ENDPOINT = os.getenv('S3_ENDPOINT')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

def spark_session():
    conf = (
    pyspark.SparkConf()
    .set("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
    .set("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
    .set("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
    .set("spark.hadoop.fs.s3a.path.style.access", "true")
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )

    spark = SparkSession.builder \
    .appName("Minio Integration with PySpark") \
    .config(conf=conf) \
    .getOrCreate()
    logger.info("spark session %s", spark)

    return spark
