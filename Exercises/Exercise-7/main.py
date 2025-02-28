import logging
import os
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory
from typing import Callable
from zipfile import ZipFile
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")


DATA_DIRECTORY = Path(__file__).parent / "data"
RESULT_DIRECTORY = Path(__file__).parent / "result"


def process_datafame(df: DataFrame, csv_file_name: str):
    try:
        df = df.withColumn("source_file", F.lit(csv_file_name))

        match = r"\d{4}-\d{2}-\d{2}"
        df = df.withColumn("file_date",
                           F.to_date(
                               F.regexp_extract(F.col("source_file"),
                                                match, 0)))
        df = df.withColumn("brand",
                           F.when(
                               F.col("model").contains(" "),
                               F.split(F.col("model"), " ")[0]
                               ).otherwise("unknow"))

        df = df.withColumn("primary_key",
                           F.sha2(
                               F.concat_ws("_",
                                           F.col("serial_number"),
                                           F.col("model")), 256))

        capacity_bytes = (
            df.select("capacity_bytes").distinct().orderBy("capacity_bytes")
        )

        window_spec = Window.orderBy(F.desc("capacity_bytes"))
        capacity_bytes = capacity_bytes.withColumn("rank",
                                                   F.dense_rank()
                                                   .over(window_spec))
        capacity_bytes = capacity_bytes.dropDuplicates(["rank"])

        df = (
            df.join(
                capacity_bytes,
                df["capacity_bytes"] == capacity_bytes["capacity_bytes"],
                "inner",
            )
            .drop("capacity_bytes")
            .drop("rank")
        )

        return df

    except Exception as e:
        logging.error(f"An error has occured while processing: {e}")


def output_report(df: DataFrame,
                  csv_file_name: str,
                  processment_function: Callable):
    result_df: DataFrame = processment_function(df, csv_file_name)
    if result_df:
        try:
            output_dir = os.path.join(str(RESULT_DIRECTORY),
                                      processment_function.__name__)
            logging.info(f"Attempting to write output to: {output_dir}")

            result_df.coalesce(1).write \
                .format("csv") \
                .mode('overwrite') \
                .option("header", True) \
                .save(output_dir)

            # creates a list of all file names in a dir
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

            logging.info(f"Successfully saved output to: {output_dir}")

        except Exception as e:
            logging.error(f"Failed to process and generate output: {e}")
            logging.error(f"Attempted to access file at: {output_dir}")

    else:
        logging.error("The resulting DataGrame is empty or invalid")


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


def main():
    logging.info("Start")

    spark: SparkSession = (SparkSession
                           .builder
                           .appName("Exercise7")
                           .enableHiveSupport()
                           .getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')

    if os.path.exists(str(RESULT_DIRECTORY)):
        shutil.rmtree(str(RESULT_DIRECTORY))

    zip_file = next(DATA_DIRECTORY.rglob("*.zip"), None)

    df: DataFrame = None

    with ZipFile(zip_file) as zip_file:
        zip_content = zip_file.infolist()[0]

        if (zip_content
            and zip_content.filename.endswith(".csv")
                and "__MACOSX" not in zip_content.filename):

            df = get_df_csv_from_zip(zip_file,
                                     zip_content.filename,
                                     spark)
            output_report(df, zip_content.filename, process_datafame)

    logging.info("End")


if __name__ == "__main__":
    main()
