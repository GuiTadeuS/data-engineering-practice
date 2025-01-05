import io
import zipfile
from pathlib import Path
from requests import Response, HTTPError, Session

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
]


def get_uri(uri: str) -> Response:
    with Session() as session:
        try:
            with session.get(url=uri, stream=True, timeout=60) as response:
                response.raise_for_status()
                content = b""
                for chunk in response.iter_content(chunk_size=8192):
                    content += chunk
                response._content = content
                return response
        except HTTPError as e:
            print(f"Error downloading {uri}: {e}")
            return None


def main():
    download_directory: Path = Path(__file__).parent / "downloads"

    for uri in download_uris:
        res = get_uri(uri)
        if res is not None:
            try:
                zip_content = io.BytesIO(res.content)
                with zipfile.ZipFile(zip_content) as zip_file:
                    zip_file.extractall(download_directory)
                print(f"Extracted csv from zip in {res.url}")
            except zipfile.BadZipFile as e:
                print(f"Skipping invalid zip file: {uri}, error: {e}")


if __name__ == "__main__":
    main()
