from pyspark.sql.functions import col, explode, explode_outer, to_date, when, size
from pyspark.sql import DataFrame
import logging

logger = logging.getLogger(__name__)

def process_albums(df: DataFrame) -> DataFrame:
    """Process album data from a DataFrame.

    Args:
        df (DataFrame): Input PySpark DataFrame with 'items' column

    Returns:
        DataFrame: Processed album data

    Raises:
        ValueError: If required columns are missing
    """
    required_columns = {'items'}
    if not required_columns.issubset(set(df.columns)):
        raise ValueError(f"DataFrame missing required columns: {required_columns - set(df.columns)}")

    df = (df.withColumn("items", explode("items"))
          .select(
        col("items.track.album.id").alias("album_id"),
        col("items.track.album.name").alias("album_name"),
        col("items.track.album.release_date").alias("release_date"),
        col("items.track.album.total_tracks").alias("total_tracks"),
        col("items.track.album.external_urls.spotify").alias("url")
    )
          .drop_duplicates(["album_id"]))

    # Parse release_date to date type
    df = df.withColumn("release_date", to_date(col("release_date")))
    return df


def process_artists(df):
    required_columns = {'items'}

    # Validação das colunas
    if not required_columns.issubset(set(df.columns)):
        missing_cols = required_columns - set(df.columns)
        logger.error(f"DataFrame está faltando colunas obrigatórias: {missing_cols}")
        raise ValueError(f"DataFrame missing required columns: {missing_cols}")

    try:
        # Explodindo os itens
        df_items_exploded = df.select(explode(col("items")).alias("item"))

        # Extraindo artistas (com validação de 'track')
        df_artists_exploded = df_items_exploded.select(
            explode_outer(col("item").getField("track").getField("artists")).alias("artist")
        )

        # Selecionando e limpando os dados
        df_artists = (
            df_artists_exploded.select(
                col("artist.id").alias("artist_id"),
                col("artist.name").alias("artist_name"),
                col("artist.external_urls.spotify").alias("external_url")
            )
            .distinct()  # Evita processamento redundante antes de drop_duplicates
            .drop_duplicates(["artist_id"])  # Mantém apenas artistas únicos
            .filter(col("artist_id").isNotNull())  # Remove registros inválidos
        )

        return df_artists

    except Exception as e:
        logger.error(f"Erro ao processar artistas: {str(e)}")
        raise Exception(f"Erro ao processar artistas: {str(e)}")


def process_songs(df: DataFrame) -> DataFrame:
    """Process song data from a DataFrame.

    Args:
        df (DataFrame): Input PySpark DataFrame with 'items' column

    Returns:
        DataFrame: Processed song data

    Raises:
        ValueError: If required columns are missing
    """
    required_columns = {'items'}
    if not required_columns.issubset(set(df.columns)):
        raise ValueError(f"DataFrame missing required columns: {required_columns - set(df.columns)}")

    df_exploded = df.select(explode(col("items")).alias("item"))
    df_songs = df_exploded.select(
        col("item.track.id").alias("song_id"),
        col("item.track.name").alias("song_name"),
        col("item.track.duration_ms").alias("duration_ms"),
        col("item.track.external_urls.spotify").alias("url"),
        col("item.track.popularity").alias("popularity"),
        col("item.added_at").alias("song_added"),
        col("item.track.album.id").alias("album_id"),
        when(size(col("item.track.artists")) > 0,
             col("item.track.artists")[0]["id"]).otherwise(None).alias("artist_id")
    ).drop_duplicates(["song_id"])

    df_songs = (df_songs.withColumn("song_added", to_date(col("song_added")))
                .filter(col("song_id").isNotNull()))

    return df_songs