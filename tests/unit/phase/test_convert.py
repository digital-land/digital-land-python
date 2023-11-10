#!/usr/bin/env -S py.test -svv
import os
from digital_land.log import DatasetResourceLog
from digital_land.phase.convert import ConvertPhase


def test_load_xlsm():
    path = os.path.join(os.getcwd(), "tests/data/brentwood.xlsm")
    log = DatasetResourceLog()
    reader = ConvertPhase(path, dataset_resource_log=log).process()
    block = next(reader)
    assert block["resource"] == "brentwood"
    assert block["line-number"] == 1
    assert "OrganisationURI" in block["line"]
    assert log.mime_type == "application/vnd.ms-excel"


def test_convert_features_to_csv_save_geojson():
    path = os.path.join(os.getcwd(), "tests/data/resource_examples/geojson.resource")
    log = DatasetResourceLog()
    ConvertPhase(
        path,
        dataset_resource_log=log,
        output_path="converted/geojson.csv",
    ).process()

    assert os.path.isfile(os.path.join(os.getcwd(), "converted/geojson.csv"))
    os.remove(os.path.join(os.getcwd(), "converted/geojson.csv"))


def test_convert_features_to_csv_save_geopackage():
    path = os.path.join(os.getcwd(), "tests/data/resource_examples/geopackage.resource")
    log = DatasetResourceLog()
    ConvertPhase(
        path,
        dataset_resource_log=log,
        output_path="converted/geopackage.csv",
    ).process()

    assert os.path.isfile(os.path.join(os.getcwd(), "converted/geopackage.csv"))
    os.remove(os.path.join(os.getcwd(), "converted/geopackage.csv"))


def test_convert_features_to_csv_save_converted_gml():
    path = os.path.join(os.getcwd(), "tests/data/resource_examples/gml.resource")
    log = DatasetResourceLog()
    ConvertPhase(
        path,
        dataset_resource_log=log,
        output_path="converted/gml.csv",
    ).process()

    assert os.path.isfile(os.path.join(os.getcwd(), "converted/gml.csv"))
    os.remove(os.path.join(os.getcwd(), "converted/gml.csv"))


def test_convert_features_to_csv_save_converted_kml():
    path = os.path.join(os.getcwd(), "tests/data/resource_examples/kml.resource")
    log = DatasetResourceLog()
    ConvertPhase(
        path,
        dataset_resource_log=log,
        output_path="converted/kml.csv",
    ).process()

    assert os.path.isfile(os.path.join(os.getcwd(), "converted/kml.csv"))
    os.remove(os.path.join(os.getcwd(), "converted/kml.csv"))


def test_convert_features_to_csv_save_converted_sqlite3():
    path = os.path.join(
        os.getcwd(),
        "tests/expectations/resources_to_test_expectations/data_for_url_expect_test.sqlite3",
    )
    log = DatasetResourceLog()
    ConvertPhase(
        path,
        dataset_resource_log=log,
        output_path="converted/sqlite3.csv",
    ).process()

    assert os.path.isfile(
        os.path.join(os.getcwd(), "converted/data_for_url_expect_test.csv")
    )
    os.remove(os.path.join(os.getcwd(), "converted/data_for_url_expect_test.csv"))


def test_convert_features_to_csv_converted_not_saved():
    path = os.path.join(
        os.getcwd(),
        "tests/expectations/resources_to_test_expectations/data_for_url_expect_test.sqlite3",
    )
    log = DatasetResourceLog()
    ConvertPhase(
        path,
        dataset_resource_log=log,
    ).process()

    for root, dirs, files in os.walk(os.getcwd()):
        assert "data_for_url_expect_test.csv" not in files
