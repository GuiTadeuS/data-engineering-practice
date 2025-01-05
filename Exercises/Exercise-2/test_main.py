from main import get_file_uri


def test_get_file_uri():
    uri = (
        'https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/'
        )
    result = get_file_uri(uri)
    assert result.endswith('.csv')
    assert result.startswith(uri)
