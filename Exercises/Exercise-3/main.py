from dataclasses import dataclass
import boto3
import botocore.session
import tempfile
import os
import gzip
import logging

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")


@dataclass
class S3Client:
    client = None

    def __init__(self):
        session = botocore.session.get_session()

        access_key = session.get_credentials().access_key or os.getenv(
            "AWS_ACCESS_KEY_ID")
        secret_key = session.get_credentials().secret_key or os.getenv(
            "AWS_SECRET_ACCESS_KEY")
        self.client = self.get_s3_client(access_key,
                                         secret_key)

    def get_s3_client(self, access_key, secret_key):
        try:
            self.client = boto3.client(
                "s3",
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key
            )
            logging.info("Successfully initialized boto3 client for S3.")
            return self.client
        except Exception as e:
            logging.error(f"Failed to initialize S3 client: {e}")
            raise

    def download_file(self, bucket_name, key, local_path):
        try:
            self.client.download_file(bucket_name, key, local_path)
            logging.info(
                f"Downloaded {key} from bucket {bucket_name} to {local_path}."
                )
        except Exception as e:
            logging.error(f"Failed to download file {key} from S3: {e}")
            raise


def process_gz_file(file_path):
    try:
        with gzip.open(file_path, "rt", encoding="utf-8") as gz_file:
            first_line = gz_file.readline().strip()
            logging.info(f"Read first line from {file_path}: {first_line}")
            return first_line
    except Exception as e:
        logging.error(f"Failed to process gzip file {file_path}: {e}")
        raise


def main():
    bucket_name = "commoncrawl"
    file_key = "crawl-data/CC-MAIN-2022-05/wet.paths.gz"

    with tempfile.TemporaryDirectory() as temp_dir:
        client = S3Client()

        try:
            first_local_path = os.path.join(temp_dir,
                                            os.path.basename(file_key))

            client.download_file(bucket_name,
                                 file_key,
                                 first_local_path)

            first_uri = process_gz_file(first_local_path)

            second_local_path = os.path.join(temp_dir, "result.gz")

            client.download_file(bucket_name,
                                 first_uri,
                                 second_local_path)

            with gzip.open(second_local_path,
                           "rt",
                           encoding="utf-8") as gz_file:
                contents = gz_file.readlines()
                logging.info(
                    f"Contents of {second_local_path}:\n{''.join(contents)}"
                    )

        except Exception as e:
            logging.error(f"An error occurred: {e}")
            raise


if __name__ == "__main__":
    main()
