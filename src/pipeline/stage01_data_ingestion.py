import sys
import argparse
from src.components.download_data_file import Ingest_Data
from src.exception import MyException

class Data_Ingestion_Pipeline:
    def __init__(self):
        pass

    def parse_args():
        parser = argparse.ArgumentParser(description="Data ingestion pipeline")
        parser.add_argument("--source_url", type=str, help="Dataset source URL (Google Drive/public link)")
        parser.add_argument("--zipFile", type=str, help="Path to save downloaded file")
        parser.add_argument("--unzipData", type=str, help="Directory to extract dataset")
        parser.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
        return parser.parse_args()


    @staticmethod
    def main():
        args = Data_Ingestion_Pipeline.parse_args()
        uploader = Ingest_Data(
            sourceURL=args.source_url,
            zipFile=args.zipFile,
            unzipData=args.unzipData,
            overwrite=args.overwrite,
        )

        uploader.download_file()
        uploader.extract_file()
            



if __name__ == '__main__':
    try:
        obj = Data_Ingestion_Pipeline()
        obj.main()
    except MyException as e:
            raise MyException(e, sys)