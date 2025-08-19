import os
import sys
import zipfile
import tarfile
import gdown

import time
from typing import Optional
from dotenv import load_dotenv
from src.logger import logging
from src.exception import MyException
from src.config import CONFIG

load_dotenv()


class Ingest_Data:
    """
    Production-grade data ingestion utility for downloading and extracting datasets.

    Features:
        - Download from Google Drive/public URLs with retry mechanism.
        - Extract ZIP/TAR archives.
        - Configurable overwrite and validation.
        - Works with both config file and runtime arguments.

    Example (programmatically):
        >>> uploader = UploadData(
        ...     source_url="https://drive.google.com/file/d/FILE_ID/view?usp=sharing",
        ...     local_data_file="artifacts/data.zip",
        ...     unzip_dir="artifacts/data",
        ...     overwrite=True
        ... )
        >>> uploader.download_file()
        >>> uploader.extract_file()

    Example (CLI):
        $ python upload_data.py --source_url "<gdrive_url>" \
                                --local_data_file "artifacts/data.zip" \
                                --unzip_dir "artifacts/data" \
                                --overwrite
    """

    def __init__(self, sourceURL: Optional[str] = None, zipFile: Optional[str] = None, 
                 unzipData: Optional[str] = None, overwrite: bool = False,
    ):
        try:
            config = CONFIG.get("DATA INGESTION", {})
            self.source_url = sourceURL or os.getenv("SOURCE_URL")
            self.local_data_file = zipFile or config.get("ZIP FILE")
            self.unzip_dir = unzipData or config.get("UNZIP DATA")
            self.overwrite = overwrite

            if not self.source_url or not self.local_data_file or not self.unzip_dir:
                raise MyException("Missing required configuration parameters", sys)

            logging.info("✅ UploadData initialized successfully.")

        except Exception as e:
            raise MyException(e, sys)

    def _retry_download(self, url: str, output: str, retries: int = 3, delay: int = 5):
        """Retry mechanism for downloads."""
        for attempt in range(1, retries + 1):
            try:
                logging.info(f"Attempt {attempt}/{retries} to download {url}")
                if "drive.google.com" in url:
                    file_id = url.split("/")[-2]
                    prefix = "https://drive.google.com/uc?/export=download&id="
                    gdown.download(prefix + file_id, output, quiet=False)
                else:
                    gdown.download(url, output, quiet=False)

                if os.path.exists(output) and os.path.getsize(output) > 0:
                    logging.info("✅ Download successful.")
                    return
                else:
                    raise Exception("File not found or empty after download.")

            except Exception as e:
                logging.warning(f"Download attempt {attempt} failed: {e}")
                if attempt < retries:
                    time.sleep(delay)
                else:
                    raise MyException(f"Failed to download after {retries} attempts", sys)

    def download_file(self):
        """Download dataset with retries and validation."""
        try:
            if os.path.exists(self.local_data_file) and not self.overwrite:
                logging.info(f"File already exists at {self.local_data_file}. Skipping download.")
                return

            os.makedirs(os.path.dirname(self.local_data_file), exist_ok=True)
            self._retry_download(self.source_url, self.local_data_file)

        except Exception as e:
            logging.error("❌ Error in download_file", exc_info=True)
            raise MyException(e, sys)

    def extract_file(self):
        """Extract ZIP/TAR archives to target directory."""
        try:
            if not os.path.exists(self.local_data_file):
                raise MyException(f"File not found at {self.local_data_file}", sys)

            os.makedirs(self.unzip_dir, exist_ok=True)

            if zipfile.is_zipfile(self.local_data_file):
                with zipfile.ZipFile(self.local_data_file, "r") as zip_ref:
                    zip_ref.extractall(self.unzip_dir)
                logging.info(f"✅ Extracted ZIP to {self.unzip_dir}")

            elif tarfile.is_tarfile(self.local_data_file):
                with tarfile.open(self.local_data_file, "r:*") as tar_ref:
                    tar_ref.extractall(self.unzip_dir)
                logging.info(f"✅ Extracted TAR to {self.unzip_dir}")

            else:
                raise MyException("Unsupported archive format. Only ZIP/TAR supported.", sys)

        except Exception as e:
            logging.error("❌ Error in extract_file", exc_info=True)
            raise MyException(e, sys)
