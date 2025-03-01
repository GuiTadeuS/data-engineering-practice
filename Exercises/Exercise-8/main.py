from enum import Enum
import os
from pathlib import Path
import duckdb


DATA_DIRECTORY = str(Path(__file__).parent / "data")
RESULT_DIRECTORY = str(Path(__file__).parent / "parquet_result")


class SQLQueries(Enum):
    NUMBER_OF_CAR_PER_CITY = """
    SELECT City, COUNT(*) as number_of_cars
    FROM eletric_cars
    GROUP BY City
    """

    TOP_THREE_ELETRIC_CARS = """
    SELECT Model, COUNT(*) as top_three
    FROM eletric_cars
    GROUP BY Model
    ORDER BY top_three DESC
    LIMIT 3
    """

    MOST_POPULAR_IN_EACH_POSTAL_CODE = """
    SELECT "Postal Code", "Model", RowNum
    FROM (
        SELECT "Postal Code", "Model", COUNT(*) AS count,
        ROW_NUMBER() OVER (PARTITION BY "Postal Code" ORDER BY count DESC, "Model" ASC) as RowNum
        FROM eletric_cars
        GROUP BY "Postal Code", "Model"
    ) AS ranked
    WHERE RowNum = 1;
    """

    ELETRIC_CARS_BY_MODEL_YEAR = """
    SELECT "Model Year", COUNT(*) AS count
    FROM eletric_cars
    GROUP BY "Model Year"
    """


db_schema = {
    "VIN (1-10)": "VARCHAR",
    "Country": "VARCHAR",
    "City": "VARCHAR",
    "State": "VARCHAR",
    "Postal Code": "VARCHAR",
    "Model Year": "BIGINT",
    "Make": "VARCHAR",
    "Model": "VARCHAR",
    "Electric Vehicle Type": "VARCHAR",
    "Clean Alternative Fuel Vehicle (CAFV) Eligibility": "VARCHAR",
    "Electric Range": "BIGINT",
    "Base MSRP": "BIGINT",
    "Legislative District": "BIGINT",
    "DOL Vehicle ID": "BIGINT",
    "Vehicle Location": "VARCHAR",
    "Electric Utility": "VARCHAR",
    "2020 Census Tract": "BIGINT",
}


def main():
    csv_file = os.path.join(DATA_DIRECTORY,
                            "Electric_Vehicle_Population_Data.csv")
    eletric_cars = duckdb.read_csv(csv_file, header=True)

    for query in SQLQueries:
        try:
            if (query.name == SQLQueries.ELETRIC_CARS_BY_MODEL_YEAR.name):
                file_name = query.name + ".parquet"
                duckdb.sql(query.value) \
                    .write_parquet(file_name,
                                   partition_by=['Model Year'],
                                   overwrite=True)

            duckdb.sql(query.value).show()
        except Exception as e:
            print(f"Error writing {query.name} to parquet: {e}")


if __name__ == "__main__":
    main()
