from code.spark_session import create_spark_session
from code.spotify_transformation import process_artists, process_albums, process_songs
from code.parquet_writer import ParquetWriter
import sys
import logging
import random

import pyspark

logger = logging.getLogger(__name__)


def data_transformation(spark):
    #logging.info(f"{processed_bucket}.")
    file_path = sys.argv[1].replace("s3", "s3a")
    processed_bucket = sys.argv[2]
    print(pyspark.__version__)


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

    #num_registros = 1000000  # 1 milhão de linhas
    #data = [(i, f"Nome_{i}", random.randint(18, 70)) for i in range(num_registros)]
    #columns = ["id", "nome", "idade"]
    #df = spark.createDataFrame(data, columns)

    #bucket_path = f"s3a://{processed_bucket}"

    #df.write.mode("overwrite").parquet(bucket_path)



if __name__ == "__main__":
    spark = create_spark_session()
    data_transformation(spark)
    spark.stop()
