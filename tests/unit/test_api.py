import csv
import os
from digital_land.api import API
from unittest.mock import Mock, mock_open
import pytest
from requests import Response, RequestException
from os import stat_result

from digital_land.pipeline.main import Pipeline

_tpz_type_data = b"""dataset,end-date,entity,entry-date,geojson,geometry,name,organisation-entity,point,prefix,reference,start-date,typology,description,notes
tree-preservation-zone-type,,18100000,2023-09-11,,,Area,600001,,tree-preservation-zone-type,area,2023-09-11,category,,
tree-preservation-zone-type,,18100001,2023-09-11,,,Group,600001,,tree-preservation-zone-type,group,2023-09-11,category,,
tree-preservation-zone-type,,18100002,2023-09-11,,,Woodland,600001,,tree-preservation-zone-type,woodland,2023-09-11,category,,"""

_cad_type_data = b"""dataset,end-date,entity,entry-date,geojson,geometry,name,organisation-entity,point,prefix,reference,start-date,typology,description,notes
conservation-area-document-type,,4210000,2024-05-20,,,Area appraisal,600001,,conservation-area-document-type,area-appraisal,2022-01-01,category,,
conservation-area-document-type,,4210001,2024-05-20,,,Notice,600001,,conservation-area-document-type,notice,2022-01-01,category,,
conservation-area-document-type,,4210002,2024-05-20,,,Designation-report,600001,,conservation-area-document-type,designation-report,2022-01-01,category,,
conservation-area-document-type,,4210003,2024-05-20,,,Area map,600001,,conservation-area-document-type,area-map,2022-01-01,category,,"""

_stat_result_empty = stat_result((0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
_stat_result_tpzt = stat_result((0, 0, 0, 0, 0, 0, len(_tpz_type_data), 0, 0, 0))
_stat_result_cadt = stat_result((0, 0, 0, 0, 0, 0, len(_cad_type_data), 0, 0, 0))


@pytest.fixture
def pipeline_dir(tmp_path_factory):
    pipeline_dir = tmp_path_factory.mktemp("pipeline")

    row = {
        "dataset": "conservation-area-document",
        "field": "DocumentType",
        "replacement-field": "document-type",
    }

    fieldnames = row.keys()

    with open(os.path.join(pipeline_dir, "transform.csv"), "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=fieldnames)
        dictwriter.writeheader()
        dictwriter.writerow(row)

    return pipeline_dir


class MockSpecification:
    def __init__(self):
        self.category_fields = {
            "tree-preservation-zone": ["tree-preservation-zone-type"],
            "conservation-area-document": ["document-type"],
        }
        self.dataset_field_dataset = {
            "tree-preservation-zone": {"tree-preservation-zone-type": ""},
            "conservation-area-document": {
                "document-type": "conservation-area-document-type"
            },
        }

    def get_category_fields(self, dataset):
        return self.category_fields[dataset]


def _mock_get(status_code, content, calls=[]):
    def _mocked_get(request):
        calls.append(request)
        response = Response()
        response.status_code = status_code
        response._content = content
        return response

    return _mocked_get


def test_download_dataset_cache(mocker):
    mocker.patch("requests.get", _mock_get(200, _tpz_type_data))
    mocker.patch("builtins.open", mock_open())
    mocker.patch("os.makedirs", Mock())

    api = API(Mock(), "http://test", "/test/cache-dir")

    api.download_dataset("test")


def test_download_dataset_specified(mocker):
    mocker.patch("requests.get", _mock_get(200, _tpz_type_data))
    mocker.patch("builtins.open", mock_open())
    mocker.patch("os.makedirs", Mock())

    api = API(Mock(), "http://test", "/test/cache-dir")

    api.download_dataset("test", csv_path="test.csv")


def test_download_dataset_overwrite(mocker):
    mocker.patch("requests.get", _mock_get(200, _tpz_type_data))
    mocker.patch("builtins.open", mock_open())
    mocker.patch("os.makedirs", Mock())

    api = API(Mock(), "http://test", "/test/cache-dir")

    api.download_dataset("test", overwrite=True, csv_path="test.csv")


def test_download_dataset_error(mocker):
    mocker.patch("requests.get", _mock_get(404, "Nothing to see here."))
    mocker.patch("os.makedirs", Mock())

    api = API(Mock(), "http://test", "/test/cache-dir")

    with pytest.raises(RequestException):
        api.download_dataset("test")


def test_get_categorical_field_values_download(mocker, pipeline_dir):
    get_calls = []
    mocker.patch("requests.get", _mock_get(200, _tpz_type_data, get_calls))
    mocker.patch("builtins.open", mock_open(read_data=_tpz_type_data.decode("ascii")))
    mocker.patch("os.makedirs", Mock())
    mocker.patch("os.path.exists", Mock(return_value=False))
    mocker.patch("os.stat", Mock(return_value=_stat_result_tpzt))

    pipeline = Pipeline(pipeline_dir, "tree-preservation-zone")

    api = API(MockSpecification(), "http://test", "/test/cache-dir")

    values = api.get_valid_category_values("tree-preservation-zone", pipeline)

    assert len(get_calls) == 1
    assert get_calls[0] == "http://test/dataset/tree-preservation-zone-type.csv"

    assert values == {"tree-preservation-zone-type": ["area", "group", "woodland"]}


def test_get_categorical_field_from_cache(mocker, pipeline_dir):
    get_mock = Mock()

    mocker.patch("requests.get", get_mock)
    mocker.patch("builtins.open", mock_open(read_data=_tpz_type_data.decode("ascii")))
    mocker.patch("os.makedirs", Mock())
    mocker.patch("os.path.exists", Mock(return_value=True))
    mocker.patch("os.stat", Mock(return_value=_stat_result_tpzt))

    pipeline = Pipeline(pipeline_dir, "tree-preservation-zone")

    get_mock.assert_not_called()

    api = API(MockSpecification(), "http://test", "/test/cache-dir")
    values = api.get_valid_category_values("tree-preservation-zone", pipeline)

    assert values == {"tree-preservation-zone-type": ["area", "group", "woodland"]}


def test_get_categorical_field_from_cache_no_data(mocker, pipeline_dir):
    get_mock = Mock()
    open_mock = Mock()

    mocker.patch("requests.get", get_mock)
    mocker.patch("builtins.open", open_mock)
    mocker.patch("os.makedirs", Mock())
    mocker.patch("os.path.exists", Mock(return_value=True))
    mocker.patch("os.stat", Mock(return_value=_stat_result_empty))

    pipeline = Pipeline(pipeline_dir, "tree-preservation-zone")

    get_mock.assert_not_called()
    open_mock.assert_not_called()

    api = API(MockSpecification(), "http://test", "/test/cache-dir")
    values = api.get_valid_category_values("tree-preservation-zone", pipeline)

    # Empty files aren't put in the list
    assert values == {}


def test_get_categorical_field_values_download_field_category(mocker, pipeline_dir):
    pipeline = Pipeline(pipeline_dir, "conservation-area-document")

    get_calls = []
    mocker.patch("requests.get", _mock_get(200, _tpz_type_data, get_calls))
    mocker.patch("builtins.open", mock_open(read_data=_cad_type_data.decode("ascii")))
    mocker.patch("os.makedirs", Mock())
    mocker.patch("os.path.exists", Mock(return_value=False))
    mocker.patch("os.stat", side_effect=[_stat_result_cadt, _stat_result_cadt])

    api = API(MockSpecification(), "http://test", "/test/cache-dir")

    values = api.get_valid_category_values("conservation-area-document", pipeline)

    assert len(get_calls) == 1
    assert get_calls[0] == "http://test/dataset/conservation-area-document-type.csv"
    assert values == {
        "document-type": ["area-appraisal", "notice", "designation-report", "area-map"],
        "DocumentType": ["area-appraisal", "notice", "designation-report", "area-map"],
    }
