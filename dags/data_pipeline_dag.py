import os
import logging
from python_code.fetch_spotify_data import DataIngestion
from python_code  import config
from dotenv import load_dotenv

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from pendulum import datetime
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
RAW_DATA_BUCKET="raw"

@dag(
    start_date=datetime(2024, 3, 19),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
)
def spotify_data_pipeline():

    init = EmptyOperator(task_id="inicio")
    finish = EmptyOperator(task_id="fim_pipeline")

    @task
    def fetch_data():
        """
        Busca os dados da playlist no Spotify e os salva no MinIO.
        """
        logging.info("Iniciando ingestão de dados do Spotify")
     #  data_ingestion = DataIngestion(client_id=client_id, client_secret=client_secret)

        print(s3_client.list_buckets()) 
       #file = data_ingestion.fetch_and_save_playlist_tracks(
        #   playlist_link=config.playlist_link,
         #  s3_client=s3_client,
          # BUCKET_NAME=config.raw_bucket
       #)
#       file
    # Criando as tarefas corretamente
    fetch_data_task = fetch_data()

    # Definição da sequência do pipeline
    init >> fetch_data_task >> finish

# Instancia o DAG no Airflow
spotify_data_pipeline()