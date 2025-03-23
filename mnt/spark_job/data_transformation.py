from code.spark_session import spark_session
#from code.spotify_transformation import process_albums, process_artists, process_songs
import sys
import logging
from pyspark.sql.functions import col, explode, explode_outer, to_date, when, size
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

file_path = sys.argv[1]

def process_artists(df: DataFrame) -> DataFrame:
    """Process artist data from a DataFrame.

    Args:
        df (DataFrame): Input PySpark DataFrame with 'items' column

    Returns:
        DataFrame: Processed artist data

    Raises:
        ValueError: If required columns are missing
    """
    required_columns = {'items'}
    if not required_columns.issubset(set(df.columns)):
        raise ValueError(f"DataFrame missing required columns: {required_columns - set(df.columns)}")

    df_items_exploded = df.select(explode(col("items")).alias("item"))
    df_artists_exploded = df_items_exploded.select(
        explode_outer(col("item.track.artists")).alias("artist")
    )
    df_artists = (df_artists_exploded.select(
        col("artist.id").alias("artist_id"),
        col("artist.name").alias("artist_name"),
        col("artist.external_urls.spotify").alias("external_url")
    )
                  .drop_duplicates(["artist_id"])
                  .filter(col("artist_id").isNotNull()))

    return df_artists


def data_transformation(spark):
    #df_raw = spark.read.option("multiline", "true").json("s3a://raw/playlist_20250323_190824.json")
    spark.read.option("multiline", "true").json(file_path).show()

        # Process transformations
    #df_artists = process_artists(df_raw)
    #df_artists = process_artists(df_raw)
    #df_songs = process_songs(df_raw)
    #logger.info(f"Data written successfully to {file_full_path}")
    #try:
    #    file_full_path = "s3a://processed/artists"
    #    df_artists.write \
     #       .mode("overwrite") \
    #        .parquet(file_full_path)
     #   logger.info(f"Data written successfully to {file_full_path}")
    #except Exception as e:
    #    logger.error(f"Error writing the data to {file_full_path}: {str(e)}")
      #  raise e

if __name__ == "__main__":
    spark = spark_session()
    data_transformation(spark)
    spark.stop()