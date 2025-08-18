import sys
from src.constants import *
from src.logger import logging
from src.exception import MyException
from src.pipeline.stage01_data_ingestion import Data_Ingestion_Pipeline

try:
    logging.info(f">>>>>> stage {DATA_UPLOAD_STAGE_NAME} started <<<<<<")
    data_ingestion = Data_Ingestion_Pipeline()
    data_ingestion.main()
    logging.info(f">>>>>> stage {DATA_UPLOAD_STAGE_NAME} completed <<<<<<\n\nx==========x")
except MyException as e:
    logging.exception(e, sys)
    raise e
