import os
import logging
from python_code.fetch_spotify_data import DataIngestion
from python_code  import config
from dotenv import load_dotenv

from airflow.decorators import dag, task
from pendulum import datetime

#variaveis ambiente
load_dotenv()
client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')


@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["example"],
)
def spotify_data_pipeline():
    @task
    def fetch_data(**kwargs):
        """
        Fetch Spotify playlist data and save it locally.
        Returns the path to the saved JSON file.
        """
        logging.info("Starting Spotify data ingestion")
        data_ingestion = DataIngestion(client_id=client_id, client_secret=client_secret)

        file_path = data_ingestion.fetch_and_save_playlist_tracks(
            playlist_link=config.playlist_link,
            raw_data = config.raw_data
        )
        logging.info(f"Fetched data file at {file_path}")
        return file_path
    
    fetch_data
 
spotify_data_pipeline()