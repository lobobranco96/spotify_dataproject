import os
import logging
from python.extract.fetch_spotify_data import DataIngestion
from python  import config
from dotenv import load_dotenv
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

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
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
        'etl_pipeline',
        default_args=default_args,
        description='ETL pipeline for Spotify data using Airflow',
        schedule_interval=None,
        start_date=datetime(2025, 3, 24),
        catchup=False,
) as dag:

    init = EmptyOperator(task_id="inicio")
    finish = EmptyOperator(task_id="fim_pipeline")

    def fetch_data():
        """
        Inicia o processo de ingestão de dados da playlist no Spotify e salva no MinIO.
        
        A função busca os dados de uma playlist do Spotify, extrai as faixas e
        armazena em um bucket S3 (MinIO). O caminho do arquivo gerado é retornado
        e enviado para o próximo passo no DAG via XCom.
        
        Args:
            kwargs (dict): Parâmetros do contexto do Airflow, incluindo XCom.
        
        Returns:
            str: Caminho do arquivo no MinIO onde os dados da playlist foram salvos.
        """
        logging.info("Iniciando ingestão de dados do Spotify")
        data_ingestion = DataIngestion(client_id=client_id, client_secret=client_secret)

        #print(s3_client.list_buckets()) 
        file_path = data_ingestion.fetch_and_save_playlist_tracks(
           playlist_link=config.playlist_link,
           s3_client=s3_client,
           BUCKET_NAME=config.raw_bucket
        )
        return file_path
    
    def spark_dtransformation(**kwargs):
            """
            Read the JSON file, transform the data with Spark, and write parquet files.
            """
            ti = kwargs['ti']
            file_path = ti.xcom_pull(task_ids='fetch_data')
            logging.info(f"Transforming data from file: {file_path}")

            logging.info(f"File Path: {file_path}")
            return SparkSubmitOperator(
            task_id='spark_submit_task',
            application='/opt/spark_job/data_transformation.py',
            conn_id='spark_default',
            conf={
                "spark.executor.memory": "512m",
                "spark.executor.cores": "1",
                "spark.jars": "/opt/spark_job/jars/aws-java-sdk-bundle-1.12.262.jar,"
                "/opt/spark_job/jars/hadoop-aws-3.3.4.jar",
                "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
                "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
                "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.path.style.access": "true"
            },
            application_args=[file_path, config.processed_bucket],
            verbose=True
            ).execute(kwargs)

    fetch_task = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data,
        provide_context=True
    )

    transformation_task = PythonOperator(
        task_id='transformation',
       python_callable=spark_dtransformation,
        op_kwargs={},
    )

    with open("/opt/airflow/sql/create_table.sql", "r") as file:
        query = file.read()

    create_tables = PostgresOperator(
        task_id="create_tables_task",
        postgres_conn_id="postgres_default", 
        sql=query#"/opt/sql/create_table.sql"  
    )

    minio_to_postgres_task = SparkSubmitOperator(
        task_id='minio_to_postgres_task',
        application='/opt/spark_job/minio_to_postgres.py',
        conn_id='spark_default',
        conf={
            "spark.executor.memory": "512m",
            "spark.executor.cores": "1",
            "spark.jars": "/opt/spark_job/jars/aws-java-sdk-bundle-1.12.262.jar,"
            "/opt/spark_job/jars/hadoop-aws-3.3.4.jar,"
            "/opt/spark_job/jars/postgresql-42.7.5.jar,",
            "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
            "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
            "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.path.style.access": "true"
        },
        verbose=True)

    # Definição da sequência do pipeline
    init >> fetch_task >> transformation_task >> create_tables >> minio_to_postgres_task >>finish

