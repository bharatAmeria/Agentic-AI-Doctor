import sys
from src.logger import logging
from src.exception import MyException
from src.pipeline.stage01_data_ingestion import Data_Ingestion_Pipeline
from src.pipeline.stage02_memory import Vector_DB_Pipeline

try:
    logging.info(f">>>>>> stage Data Ingestion started <<<<<<")
    data_ingestion = Data_Ingestion_Pipeline()
    data_ingestion.main()
    logging.info(f">>>>>> stage Data Ingestion completed <<<<<<\n\nx==========x")
except MyException as e:
    logging.exception(e, sys)
    raise e

try:
    logging.info(f">>>>>> stage Vector DB preration started <<<<<<")
    data_ingestion = Vector_DB_Pipeline()
    data_ingestion.main()
    logging.info(f">>>>>> stage Vector DB preration completed <<<<<<\n\nx==========x")
except MyException as e:
    logging.exception(e, sys)
    raise e
