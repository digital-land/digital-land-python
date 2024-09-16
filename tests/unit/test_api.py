from digital_land.api import API
from unittest.mock import Mock, mock_open
import pytest
from requests import Response, RequestException
from os import stat_result

_tpz_type_data = b"""dataset,end-date,entity,entry-date,geojson,geometry,name,organisation-entity,point,prefix,reference,start-date,typology,description,notes
tree-preservation-zone-type,,18100000,2023-09-11,,,Area,600001,,tree-preservation-zone-type,area,2023-09-11,category,,
tree-preservation-zone-type,,18100001,2023-09-11,,,Group,600001,,tree-preservation-zone-type,group,2023-09-11,category,,
tree-preservation-zone-type,,18100002,2023-09-11,,,Woodland,600001,,tree-preservation-zone-type,woodland,2023-09-11,category,,"""


def _mock_get(status_code, content):
    def _mocked_get(_):
        response = Response()
        response.status_code = status_code
        response._content = content
        return response

    return _mocked_get


def test_download_dataset_cache(mocker):
    mocker.patch("requests.get", _mock_get(200, _tpz_type_data))
    mocker.patch("builtins.open", mock_open())
    mocker.patch("os.makedirs", Mock())

    api = API("http://test", "/test/cache-dir")

    api.download_dataset("test")


def test_download_dataset_specified(mocker):
    mocker.patch("requests.get", _mock_get(200, _tpz_type_data))
    mocker.patch("builtins.open", mock_open())
    mocker.patch("os.makedirs", Mock())

    api = API("http://test", "/test/cache-dir")

    api.download_dataset("test", csv_path="test.csv")


def test_download_dataset_overwrite(mocker):
    mocker.patch("requests.get", _mock_get(200, _tpz_type_data))
    mocker.patch("builtins.open", mock_open())
    mocker.patch("os.makedirs", Mock())

    api = API("http://test", "/test/cache-dir")

    api.download_dataset("test", overwrite=True, csv_path="test.csv")


def test_download_dataset_error(mocker):
    mocker.patch("requests.get", _mock_get(404, "Nothing to see here."))
    mocker.patch("os.makedirs", Mock())

    api = API("http://test", "/test/cache-dir")

    with pytest.raises(RequestException):
        api.download_dataset("test")


def test_get_categorical_field_values_download(mocker):
    mocker.patch("requests.get", _mock_get(200, _tpz_type_data))
    mocker.patch("builtins.open", mock_open())
    mocker.patch("os.makedirs", Mock())
    mocker.patch("os.path.exists", Mock(return_value=False))
    mocker.patch(
        "os.stat", Mock(return_value=stat_result((0, 0, 0, 0, 0, 0, 0, 0, 0, 0)))
    )

    api = API("http://test", "/test/cache-dir")
    api.get_valid_category_values(["test"])


def test_get_categorical_field_from_cache(mocker):
    get_mock = Mock()

    mocker.patch("requests.get", get_mock)
    mocker.patch("builtins.open", mock_open(read_data=_tpz_type_data.decode("ascii")))
    mocker.patch("os.makedirs", Mock())
    mocker.patch("os.path.exists", Mock(return_value=True))
    mocker.patch(
        "os.stat",
        Mock(
            return_value=stat_result((0, 0, 0, 0, 0, 0, len(_tpz_type_data), 0, 0, 0))
        ),
    )

    get_mock.assert_not_called()

    api = API("http://test", "/test/cache-dir")
    values = api.get_valid_category_values(["tree-preservation-zone-type"])

    assert values == {"tree-preservation-zone-type": ["area", "group", "woodland"]}


def test_get_categorical_field_from_cache_no_data(mocker):
    get_mock = Mock()
    open_mock = Mock()

    mocker.patch("requests.get", get_mock)
    mocker.patch("builtins.open", open_mock)
    mocker.patch("os.makedirs", Mock())
    mocker.patch("os.path.exists", Mock(return_value=True))
    mocker.patch(
        "os.stat", Mock(return_value=stat_result((0, 0, 0, 0, 0, 0, 0, 0, 0, 0)))
    )

    get_mock.assert_not_called()
    open_mock.assert_not_called()

    api = API("http://test", "/test/cache-dir")
    values = api.get_valid_category_values(["tree-preservation-zone-type"])

    assert values == {"tree-preservation-zone-type": []}
