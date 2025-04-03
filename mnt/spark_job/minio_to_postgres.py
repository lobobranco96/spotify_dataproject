import logging
from pyspark.sql.utils import AnalysisException
from code.spark_session import create_spark_session

logger = logging.getLogger(__name__)

def minio_postgres(spark):
    try:
        # Caminho do bucket
        bucket_path = "s3a://processed/"

        # Tenta ler os arquivos Parquet
        try:
            artists_df = spark.read.parquet(bucket_path + "artists.parquet")
            albums_df = spark.read.parquet(bucket_path + "albums.parquet")
            songs_df = spark.read.parquet(bucket_path + "songs.parquet")
            logger.info("Arquivos Parquet lidos com sucesso!")
        except AnalysisException as e:
            logger.error(f"Erro ao ler arquivos Parquet: {e}")
            return  # Encerra a função se a leitura falhar

        # Configuração do PostgreSQL
        postgres_url = "jdbc:postgresql://postgres_airflow:5432/airflow"
        postgres_properties = {
            "user": "airflow",
            "password": "airflow",
            "driver": "org.postgresql.Driver",
            "currentSchema": "spotify"
        }

        # Escrever os DataFrames no 
        logger.info("Escrevendo artists_df no Postgres!")
        artists_df.write.jdbc(
            url=postgres_url,
            table="spotify.artist",
            mode="append",
            properties=postgres_properties)
        
        logger.info("Escrevendo albums_df no Postgres!")
        albums_df.write.jdbc(
            url=postgres_url,
            table="spotify.album",
            mode="append",
            properties=postgres_properties)
        
        logger.info("Escrevendo songs_df no Postgres!")
        songs_df.write.jdbc(
            url=postgres_url,
            table="spotify.song",
            mode="append",
            properties=postgres_properties)

        logger.info("Dados inseridos no PostgreSQL com sucesso!")

    except Exception as e:
        logger.error(f"Erro durante o processo de ingestão: {e}", exc_info=True)


if __name__ == "__main__":
    """
    Ponto de entrada do script. Inicializa a sessão Spark e executa a ingestão de dados dos dados.
    """
    spark = create_spark_session()
    minio_postgres(spark)
    spark.stop()
