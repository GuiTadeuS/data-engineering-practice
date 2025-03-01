import os
from pathlib import Path
import polars as pl
from polars import DataFrame, LazyFrame

DATA_DIRECTORY = str(Path(__file__).parent / "data")

schema = {
    "ride_id": pl.String,
    "rideable_type": pl.Categorical,
    "started_at": pl.Datetime,
    "ended_at": pl.Datetime,
    "start_station_name": pl.String,
    "start_station_id": pl.String,
    "end_station_name": pl.String,
    "end_station_id": pl.String,
    "start_lat": pl.Float32,
    "start_lng": pl.Float32,
    "end_lat": pl.Float32,
    "end_lng": pl.Float32,
    "member_casual": pl.Categorical
}


def rides_per_day(lf: LazyFrame) -> DataFrame:
    return (lf.group_by(pl.col("started_at").dt.date())
            .count()
            .sort("count")).collect()


def average_max_minimum(lf: pl.LazyFrame) -> pl.DataFrame:
    # equivalent of a SQL query that groups by and other that
    # wraps arount and selects the results using functions
    total_rides = (lf.group_by(pl.col("started_at").dt.week()
                               .alias("week_number")).count())

    return total_rides.select(
                pl.mean("count").alias("mean"),
                pl.min("count").alias("min"),
                pl.max("count").alias("max")
            ).collect()


def diff_last_week(lf: pl.LazyFrame) -> pl.DataFrame:
    pass


def main():
    csv_file = os.path.join(DATA_DIRECTORY, "202306-divvy-tripdata.csv")

    lf = pl.scan_csv(csv_file,
                     has_header=True,
                     separator=',',
                     infer_schema=True,
                     schema=schema)

    print(rides_per_day(lf))

    print(average_max_minimum(lf))


if __name__ == "__main__":
    main()
