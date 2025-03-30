import json
import os
from datetime import datetime
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials # type: ignore
import logging
import io


class DataIngestion:
    """
    Classe responsável pela ingestão de dados do Spotify, especificamente para buscar as faixas
    de uma playlist e salvá-las em um arquivo JSON.

    A classe utiliza a biblioteca `spotipy` para autenticação e interação com a API do Spotify.
    Os dados extraídos são salvos no MinIO em formato JSON.

    Atributos:
        client_id (str): ID do cliente para autenticação na API do Spotify.
        client_secret (str): Chave secreta do cliente para autenticação na API do Spotify.
        sp (spotipy.Spotify): Cliente Spotify autenticado para interagir com a API.

    Métodos:
        __init__(self, client_id, client_secret):
            Inicializa o cliente Spotify com as credenciais fornecidas.

        fetch_and_save_playlist_tracks(self, playlist_link, s3_client, BUCKET_NAME, filename=None):
            Busca as faixas de uma playlist do Spotify e salva os dados em formato JSON no MinIO.
    """

    def __init__(self, client_id, client_secret):
        """
        Inicializa o cliente do Spotify com as credenciais fornecidas.

        Args:
            client_id (str): ID do cliente para autenticação na API do Spotify.
            client_secret (str): Chave secreta do cliente para autenticação na API do Spotify.

        Raises:
            spotipy.oauth2.SpotifyOauthError: Se a autenticação falhar com as credenciais fornecidas.
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
        """
        Busca as faixas de uma playlist do Spotify e salva os dados em formato JSON no MinIO.

        Este método usa a URL da playlist do Spotify para buscar as faixas associadas a ela. Os dados
        das faixas são coletados em várias páginas, se necessário, e são salvos no MinIO como um arquivo JSON.

        Args:
            playlist_link (str): URL da playlist do Spotify.
            s3_client (boto3.client): Cliente S3 para interagir com o MinIO.
            BUCKET_NAME (str): Nome do bucket no MinIO onde os dados serão armazenados.
            filename (str, optional): Nome do arquivo de saída. Se não fornecido, um nome com timestamp será gerado.

        Returns:
            str: Caminho para o arquivo salvo no MinIO, no formato "s3://BUCKET_NAME/filename".

        Raises:
            Exception: Se ocorrer algum erro durante o processo de busca ou salvamento dos dados.
        """
        try:
            playlist_uri = playlist_link.split("/")[-1].split('?')[0]
            logging.info(f"Buscando dados da playlist para URI: {playlist_uri}")

            # Busca todas as faixas com paginação
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

            json_data = json.dumps(data, ensure_ascii=False)  # Remove indentação para JSON compacto
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=filename,
                Body=json_data,
                ContentType="application/json"
            )

            logging.info(f"JSON salvo no MinIO: s3://{BUCKET_NAME}/{filename}")
            file_path = f"s3://{BUCKET_NAME}/{filename}"
            return file_path
        
        except Exception as e:
            logging.error(f"Erro ao salvar no MinIO: {e}")
            raise