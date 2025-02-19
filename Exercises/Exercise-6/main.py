import logging
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Callable, List
from zipfile import ZipFile
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (col,
                                   avg,
                                   to_date,
                                   unix_timestamp)


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")


REPORTS_DIRECTORY = str(Path(__file__).parent / "reports")


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
            output_dir = os.path.join(REPORTS_DIRECTORY,
                                      processment_function.__name__)
            logging.info(f"Attempting to write output to: {output_dir}")

            result_df.coalesce(1).write \
                .format("csv") \
                .mode('overwrite') \
                .option("header", True) \
                .save(output_dir)

            logging.info(f"Successfully saved output to: {output_dir}")

            files = os.listdir(output_dir)
            for file in files:
                if file.endswith(".csv"):
                    csv_file_path = os.path.join(output_dir, file)

                    new_file_name = processment_function.__name__ + ".csv"

                    new_csv_file_path = os.path.join(output_dir,
                                                     new_file_name)

                    os.rename(csv_file_path, new_csv_file_path)
                else:
                    file_to_delete = os.path.join(output_dir, file)
                    os.remove(file_to_delete)

        except Exception as e:
            logging.error(f"Failed to process and generate output: {e}")
            logging.error(f"Attempted to access file at: {output_dir}")
    else:
        logging.error("The resulting DataFrame is empty or invalid.")


def average_trip_duration(dfs: List[DataFrame]):
    if not dfs:
        logging.error("No dataframes found")
        return

    combined_df = None

    for df in dfs:
        if "start_time" in df.columns:
            df = df.withColumnRenamed("start_time", "started_at")
            df = df.withColumnRenamed("from_station_name",
                                      "start_station_name")
            df = df.withColumnRenamed("end_time", "ended_at")

        df = df.withColumn("day",
                           to_date(col("started_at")))
        df = df.withColumn(
            "trip_duration",
            (unix_timestamp("ended_at") - unix_timestamp("started_at"))
        )
        df = df.select("day", "trip_duration")

        if combined_df is None:
            combined_df = df
        else:
            combined_df = combined_df.union(df)

    return combined_df.groupBy("day") \
        .agg(avg(col("trip_duration"))) \
        .orderBy("day")


def main():
    logging.info("Start")

    spark: SparkSession = SparkSession \
        .builder \
        .appName("Exercise6") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

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
