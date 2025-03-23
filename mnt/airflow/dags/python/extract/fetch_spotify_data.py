import json
import os
from datetime import datetime
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials # type: ignore
import logging
import io


class DataIngestion:
    """Class to handle Spotify data ingestion."""

    def __init__(self, client_id, client_secret):
        """Initialize Spotify client with credentials.

        Args:
            client_id (str): Spotify API client ID
            client_secret (str): Spotify API client secret

        Raises:
            spotipy.oauth2.SpotifyOauthError: Se autenticação falhar
        """
        try:
            self.client_id = client_id
            self.client_secret = client_secret
            self.sp = spotipy.Spotify(
                client_credentials_manager=SpotifyClientCredentials(
                    client_id=client_id,
                    client_secret=client_secret
                )
            )
            logging.info("Spotify client iniciou com sucesso.")
        except spotipy.oauth2.SpotifyOauthError as e:
            logging.error(f"Autenticação falhou: {e}")
            raise

    def fetch_and_save_playlist_tracks(self, playlist_link, s3_client, BUCKET_NAME, filename=None):
        """Fetch tracks from a Spotify playlist and save to JSON.

        Args:
            playlist_link (str): URL da playlist do Spotify
            raw_data (str): Directory to save the data
            filename (str, optional): Name of the output file

        Returns:
            str: Path to the saved file

        Raises:
            Exception: If fetching or saving fails
        """
        try:
            playlist_uri = playlist_link.split("/")[-1].split('?')[0]
            logging.info(f"Buscando dados da playlist para URI: {playlist_uri}")

            # Fetch all tracks with pagination
            all_tracks = []
            results = self.sp.playlist_tracks(playlist_uri)
            all_tracks.extend(results['items'])

            while results['next']:
                results = self.sp.next(results)
                all_tracks.extend(results['items'])

            data = {'items': all_tracks}
            logging.info("Playlist data fetched successfully.")
            
            if not filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"playlist_{timestamp}.json"

            json_data = json.dumps(data, ensure_ascii=False, indent=4)
            json_bytes = json_data.encode("utf-8")  # Converte para bytes
            file_obj = io.BytesIO(json_bytes)  # Cria um objeto de arquivo na memória

            # Fazendo o upload para o MinIO
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=filename,
                Body=file_obj,
                ContentType="application/json"
            )
            logging.info(f"JSON salvo no MinIO: s3://{BUCKET_NAME}/{filename}")
            file_path = f"s3://{BUCKET_NAME}/{filename}"
            return file_path
        
        except Exception as e:
            logging.error(f"Erro ao salvar no MinIO: {e}")
            
        except Exception as e:
            logging.error(f"Error in fetching or saving playlist data: {e}")
            raise