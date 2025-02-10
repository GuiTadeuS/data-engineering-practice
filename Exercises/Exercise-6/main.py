import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Callable, List
from zipfile import ZipFile
from pyspark.sql import SparkSession, DataFrame


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")


REPORTS_DIRECTORY: Path = str(Path(__file__).parent / "reports")


def get_df_csv_from_zip(zip_file: ZipFile, filename, spark: SparkSession):
    with TemporaryDirectory() as tempdir:
        csv_path = zip_file.extract(filename, path=tempdir)
        try:
            df = spark.read.csv(csv_path, header=True, inferSchema=True)
            df.cache()
            df.count()
            logging.info("Successfully created DataFrame")
            return df
        except Exception as e:
            logging.error(f"Failed to create DataFrame with {csv_path}: {e}")


def process_dataframes(dfs: List[DataFrame]):
    output_report(dfs, average_trip_duration)


def output_report(dfs: List[DataFrame], processment_function: Callable):
    result_df: DataFrame = processment_function(dfs)
    if result_df:
        try:
            output_path = f"{REPORTS_DIRECTORY}\\{processment_function.__name__}.csv"
            logging.info(f"Attempting to write output to: {output_path}")

            result_df.coalesce(1).write \
                .format("csv") \
                .mode('overwrite') \
                .option("header", True) \
                .save(output_path)

            logging.info(f"Successfully saved output to: {output_path}")
        except Exception as e:
            logging.error(f"Failed to process and generate output: {e}")
            logging.error(f"Attempted to access file at: {output_path}")
    else:
        logging.error("The resulting DataFrame is empty or invalid.")


def average_trip_duration(dfs: List[DataFrame]):
    return dfs[0]


def main():
    logging.info("Start")

    spark: SparkSession = SparkSession \
        .builder \
        .appName("Exercise6") \
        .enableHiveSupport() \
        .getOrCreate()

    data_directory = Path(__file__).parent / "data"
    zips = [str(file) for file in data_directory.rglob("*.zip")]

    dfs: List[DataFrame] = []
    for zip_file in zips:
        with ZipFile(zip_file) as zip_file:
            zipinfo = zip_file.infolist()
            for file_info in zipinfo:
                if file_info.filename.endswith(".csv") and "__MACOSX" not in file_info.filename:
                    dfs.append(get_df_csv_from_zip(zip_file,
                                                   file_info.filename,
                                                   spark))
    if dfs:
        process_dataframes(dfs)

    logging.info("End")


if __name__ == "__main__":
    main()
