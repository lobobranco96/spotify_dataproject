import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import pyspark


load_dotenv(dotenv_path="../../services/conf/.credentials.conf")

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")


def create_spark_session():

    conf = (
    pyspark.SparkConf()
    .set("spark.master", "spark://spark-master:7077")
    .set("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
    .set("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
    .set("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
    .set("spark.hadoop.fs.s3a.path.style.access", "true")
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )

    spark = SparkSession.builder \
    .appName("MiniO data transformation with PySpark") \
    .config(conf=conf) \
    .getOrCreate()

    return spark