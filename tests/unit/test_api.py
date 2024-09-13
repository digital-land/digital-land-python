from digital_land.api import API
from unittest.mock import Mock, mock_open


def _mock_get(data):
    def _mocked_get(_):
        response = Mock()
        response.json = Mock(return_value=data)
        return response

    return _mocked_get


class mocked_DictWriter:
    def __init__(self):
        self.mock = Mock()
        self.fieldnames = None

    def __call__(self, _, fieldnames):
        self.fieldnames = [n for n in fieldnames]
        return self.mock

    def assert_write_header(self, fieldnames):
        assert fieldnames == self.fieldnames
        self.mock.writeheader.assert_called()

    def assert_write_rows(self, rows):
        self.mock.writerows.assert_called_with(rows)


def test_download_dataset(mocker):
    get_data = {"entities": [{"test": 42}], "links": {}, "count": 1}
    mocked_dw = mocked_DictWriter()

    mocker.patch("requests.get", _mock_get(get_data))
    mocker.patch("builtins.open", mock_open())
    mocker.patch("csv.DictWriter", mocked_dw)
    mocker.patch("os.makedirs", Mock())

    api = API("http://test", "test-cache")
    api.download_dataset("test")

    mocked_dw.assert_write_header(["test"])
    mocked_dw.assert_write_rows([{"test": 42}])


def test_download_dataset_error():
    pass


def test_download_dataset_paged():
    pass


def test_get_categorical_field_values_download():
    pass


def test_get_categorical_field_from_cache():
    pass
