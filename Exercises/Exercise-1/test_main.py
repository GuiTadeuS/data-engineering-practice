from unittest.mock import Mock, patch
from main import main


@patch('main.get_uri', return_value=None)
def test_main_uri_not_found(mock_get_uri: Mock):
    main()

    mock_get_uri.assert_called()
