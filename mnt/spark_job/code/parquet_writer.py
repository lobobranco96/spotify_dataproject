import logging
import traceback
import os

logger = logging.getLogger(__name__)

class ParquetWriter:
    """
    Classe para escrever DataFrames do PySpark em arquivos Parquet.

    A classe permite salvar um DataFrame no formato Parquet em um caminho especificado,
    utilizando um modo de gravação definido pelo usuário.

    Atributos:
        mode (str): Define o modo de gravação. Pode ser "overwrite", "append", "ignore" ou "error".
    
    Métodos:
        dataframe_writer(df, bucket_path, nome_arquivo):
            Escreve o DataFrame no caminho especificado em formato Parquet.
    """

    def __init__(self, mode: str):
        """
        Inicializa a classe ParquetWriter.

        Args:
            mode (str): Modo de gravação do arquivo Parquet.
                        Opções: "overwrite", "append", "ignore" ou "error".
        """
        self.mode = mode

    def dataframe_writer(self, df, bucket_path: str, nome_arquivo: str):
        """
        Escreve um DataFrame do PySpark em um arquivo Parquet.

        Args:
            df (DataFrame): DataFrame do PySpark a ser salvo.
            bucket_path (str): Caminho do diretório no armazenamento.
            nome_arquivo (str): Nome do arquivo Parquet a ser salvo.

        Raises:
            Exception: Se ocorrer um erro durante a escrita do arquivo.
        """
        file_path = f"{bucket_path}/{nome_arquivo}.parquet"
        
        try:
            df.write \
                .mode(self.mode) \
                .parquet(file_path)
            logger.info(f"Data written successfully to {file_path}")
        except Exception as e:
            logger.error(f"Error writing the data to {file_path}: {str(e)}")
            traceback_message = traceback.format_exc()
            print(traceback_message)
            raise e
