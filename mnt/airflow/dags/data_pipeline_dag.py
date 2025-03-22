import os
import logging
from python.extract.fetch_spotify_data import DataIngestion
from python  import config
from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import boto3

#variaveis ambiente
load_dotenv()

# spotify
client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')

#S3 MINIO
MINIO_ENDPOINT = os.getenv('S3_ENDPOINT')
MINIO_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
MINIO_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')


s3_client = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

default_args = {
    'owner': 'lobobranco',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 22),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        'spotify_etl_pipeline',
        default_args=default_args,
        description='ETL pipeline for Spotify data using Airflow',
        schedule_interval='@daily',
        catchup=False
) as dag:

    init = EmptyOperator(task_id="inicio")
    finish = EmptyOperator(task_id="fim_pipeline")

    def fetch_data(**kwargs):
        """
        Busca os dados da playlist no Spotify e os salva no MinIO.
        """
        logging.info("Iniciando ingestão de dados do Spotify")
        data_ingestion = DataIngestion(client_id=client_id, client_secret=client_secret)

        #print(s3_client.list_buckets()) 
        file_path = data_ingestion.fetch_and_save_playlist_tracks(
           playlist_link=config.playlist_link,
           s3_client=s3_client,
           BUCKET_NAME=config.raw_bucket
        )
        file_path
        #logging.info(f"Fetched data file at {file_path}")
    
    fetch_task = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data,
        provide_context=True
    )

    # Definição da sequência do pipeline
    init >> fetch_task >> finish

