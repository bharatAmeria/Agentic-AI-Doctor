import sys
import argparse
import subprocess
from src.constants import *
from src.components.download_data_file import Ingest_Data
from src.logger import logging
from src.exception import MyException

class Data_Ingestion_Pipeline:
    def __init__(self):
        pass

    def run_tests(self):
        """Run pytest before executing ingestion pipeline"""
        logging.info("Running unit tests before ingestion...")
        result = subprocess.run(["pytest", "--maxfail=1", "--disable-warnings", "-q"])
        if result.returncode != 0:
            raise MyException("Unit tests failed. Aborting pipeline.", sys)


    def parse_args():
        parser = argparse.ArgumentParser(description="Data ingestion pipeline")
        parser.add_argument("--source_url", type=str, help="Dataset source URL (Google Drive/public link)")
        parser.add_argument("--local_data_file", type=str, help="Path to save downloaded file")
        parser.add_argument("--unzip_dir", type=str, help="Directory to extract dataset")
        parser.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
        return parser.parse_args()


    @staticmethod
    def main():
        args = Data_Ingestion_Pipeline.parse_args()
        uploader = Ingest_Data(
            source_url=args.source_url,
            local_data_file=args.local_data_file,
            unzip_dir=args.unzip_dir,
            overwrite=args.overwrite,
        )

        uploader.download_file()
        uploader.extract_file()
            



if __name__ == '__main__':
    try:
        logging.info(f">>>>>> stage {INGESTION_STAGE_NAME} started <<<<<<")
        obj = Data_Ingestion_Pipeline()
        obj.run_tests()
        obj.main()
    except MyException as e:
            raise MyException(e, sys)