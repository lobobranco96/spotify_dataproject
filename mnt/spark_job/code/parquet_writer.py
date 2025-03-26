import logging
import traceback
import os


logger = logging.getLogger(__name__)

class ParquetWriter:
    def __init__(self, mode):
        self.mode = mode

    def dataframe_writer(self, df, bucket, folder):

        file_path = f"{bucket}/{folder}"
        
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