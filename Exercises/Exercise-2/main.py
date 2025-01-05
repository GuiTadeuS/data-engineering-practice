import pandas as pd


def get_file_uri(uri) -> str:
    df = pd.read_html(uri)[0]
    first_match = df.loc[df['Last modified'] == '2024-01-19 10:27'].iloc[0]
    return uri+first_match['Name']


def process_csv(uri):
    df = pd.read_csv(uri)
    print(df.loc[df['HourlyDryBulbTemperature'].max()].to_string())


def main():
    file_url = get_file_uri(
        'https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/'
        )
    process_csv(file_url)


if __name__ == "__main__":
    main()
