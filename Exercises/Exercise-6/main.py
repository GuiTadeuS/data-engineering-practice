from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZipFile
from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")


def process_csv(zip_file: ZipFile, filename, spark: SparkSession):
    with TemporaryDirectory() as tempdir:
        csv_path = zip_file.extract(filename, path=tempdir)
        try:
            df = spark.read.csv(csv_path, header=True, inferSchema=True)
            logging.info("Successfully created DataFrame")
            logging.info("Showing DataFrame head data")
            logging.info(df.head(2))
            return df
        except Exception as e:
            logging.error(f"Failed to create DataFrame with {csv_path}: {e}")


def main():
    logging.info("Start")
    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()

    data_directory = Path(__file__).parent / "data"
    zips = [str(file) for file in data_directory.rglob("*.zip")]

    for zip_file in zips:
        with ZipFile(zip_file) as zip_file:
            zipinfo = zip_file.infolist()
            for file_info in zipinfo:
                if file_info.filename.endswith(".csv"):
                    process_csv(zip_file, file_info.filename, spark)
    logging.info("End")


if __name__ == "__main__":
    main()
