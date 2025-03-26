from code.spark_session import create_spark_session
from code.spotify_transformation import process_artists, process_albums, process_songs
from code.parquet_writer import ParquetWriter
import sys
import logging

logger = logging.getLogger(__name__)


def data_transformation(spark, file_path, processed_bucket, writer):

    df_raw = spark.read.option("multiline", "true").json(file_path)

    # Process transformation
    artist = process_artists(df_raw)
    song = process_songs(df_raw)
    album = process_albums(df_raw)

    bucket_path = f"s3://processed_bucket"

    writer = ParquetWriter(mode="overwrite")
    writer.dataframe_writer(album, bucket_path, "albums")
    writer.dataframe_writer(artist, bucket_path, "artists")
    writer.dataframe_writer(song, bucket_path, "songs")

    logging.info("Data transformation and parquet file writing completed.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("File path argument is missing!")
        sys.exit(1)

    file_path = sys.argv[1].replace("s3", "s3a")
    processed_bucket = sys.argv[2]
    spark = create_spark_session()
    writer = ParquetWriter(mode="overwrite")
    data_transformation(spark, file_path, processed_bucket, writer)
    spark.stop()
