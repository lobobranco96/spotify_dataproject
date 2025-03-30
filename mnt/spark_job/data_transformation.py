"""
Script de transformação de dados do Spotify utilizando Apache Spark.

Este script lê arquivos JSON brutos de um bucket S3 (MinIO), executa transformações nos dados
das entidades `artists`, `albums` e `songs`, e grava os dados transformados
de volta em um bucket de arquivos processados no formato Parquet.

Módulos Utilizados:
    - create_spark_session: Função para iniciar uma SparkSession.
    - process_artists, process_albums, process_songs: Funções de transformação de dados.
    - ParquetWriter: Classe auxiliar para escrita de DataFrames em formato Parquet.

Argumentos:
    - sys.argv[1]: Caminho do arquivo de entrada no S3 (formato JSON).
    - sys.argv[2]: Nome do bucket S3 onde os dados transformados serão salvos.
"""

from code.spark_session import create_spark_session
from code.spotify_transformation import process_artists, process_albums, process_songs
from code.parquet_writer import ParquetWriter
import sys
import logging

import pyspark

logger = logging.getLogger(__name__)


def data_transformation(spark):
    """
    Executa o processo de leitura, transformação e gravação de dados no S3.
    
    Args:
        spark (SparkSession): Sessão Spark ativa.
    """
    file_path = sys.argv[1].replace("s3", "s3a")
    processed_bucket = sys.argv[2]

    logging.info("Leitura iniciada !!!!!!.")
    df_raw = spark.read.option("multiline", "true").json(file_path)
    logging.info("Leitura finalizada !!!!!!!.")

    logging.info("Processo de transformação iniciada.")
    artists = process_artists(df_raw)
    albums = process_albums(df_raw)
    songs = process_songs(df_raw)
    logging.info("Processo de transformação iniciada.")


    logging.info("Carregando dados ao bucket de arquivos tratados.")
    bucket_path = f"s3a://{processed_bucket}"
    parquet_writer = ParquetWriter(mode="overwrite")

    parquet_writer.dataframe_writer(artists, bucket_path, "artists")
    parquet_writer.dataframe_writer(albums, bucket_path, "albums")
    parquet_writer.dataframe_writer(songs, bucket_path, "songs")
    logging.info("Processo finalizado.")

if __name__ == "__main__":
    """
    Ponto de entrada do script. Inicializa a sessão Spark e executa a transformação dos dados.
    """
    spark = create_spark_session()
    data_transformation(spark)
    spark.stop()
