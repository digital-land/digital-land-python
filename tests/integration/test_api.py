import csv
import os

import pytest

from digital_land.api import API
from digital_land.pipeline.main import Pipeline


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
            "conservation-area-document": ["document-type"],
        }
        self.dataset_field_dataset = {
            "conservation-area-document": {
                "document-type": "conservation-area-document-type"
            },
        }

    def get_category_fields(self, dataset):
        return self.category_fields[dataset]


def test_get_categorical_field_read_csv(pipeline_dir, tmp_path_factory):
    cache_dir = tmp_path_factory.mktemp("cache")
    dataset = "conservation-area-document"
    field_dataset = "conservation-area-document-type"

    headers = [
        "dataset",
        "end-date",
        "entity",
        "entry-date",
        "geojson",
        "geometry",
        "name",
        "organisation-entity",
        "point",
        "prefix",
        "reference",
        "start-date",
        "typology",
        "description",
        "notes",
    ]

    rows = [
        {
            "dataset": "conservation-area-document-type",
            "end-date": "",
            "entity": "4210000",
            "entry-date": "2024-05-20",
            "geojson": "",
            "geometry": "",
            "name": "Area appraisal",
            "organisation-entity": "600001",
            "point": "",
            "prefix": "conservation-area-document-type",
            "reference": "NEW TYPE",
            "start-date": "2022-01-01",
            "typology": "category",
            "description": "",
            "notes": "",
        },
    ]

    os.mkdir(os.path.join(cache_dir, "dataset"))
    with open(os.path.join(cache_dir, "dataset", f"{field_dataset}.csv"), "w") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)

    pipeline = Pipeline(pipeline_dir, dataset)

    api = API(MockSpecification(), "http://test", cache_dir)

    values = api.get_valid_category_values(dataset, pipeline)

    # check that the valid value is the exact same as in the dataset (no capitalisation difference)
    assert values["document-type"] == ["NEW TYPE"]

    # check that the replacement-field has also been given valid values
    assert values["DocumentType"] == ["NEW TYPE"]
