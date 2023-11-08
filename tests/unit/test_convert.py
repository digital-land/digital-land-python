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


def test_save_converted_geojson():
    path = os.path.join(os.getcwd(), "tests/data/resource_examples/geojson.resource")
    log = DatasetResourceLog()
    ConvertPhase(
        path,
        dataset_resource_log=log,
        custom_temp_dir="data",
    ).process()

    assert os.path.isfile(os.path.join(os.getcwd(), "converted/geojson.csv"))
    os.remove(os.path.join(os.getcwd(), "converted/geojson.csv"))


def test_save_converted_geopackage():
    path = os.path.join(os.getcwd(), "tests/data/resource_examples/geopackage.resource")
    log = DatasetResourceLog()
    ConvertPhase(
        path,
        dataset_resource_log=log,
        custom_temp_dir="data",
    ).process()

    assert os.path.isfile(os.path.join(os.getcwd(), "converted/geopackage.csv"))
    os.remove(os.path.join(os.getcwd(), "converted/geopackage.csv"))


def test_save_converted_gml():
    path = os.path.join(os.getcwd(), "tests/data/resource_examples/gml.resource")
    log = DatasetResourceLog()
    ConvertPhase(
        path,
        dataset_resource_log=log,
        custom_temp_dir="data",
    ).process()

    assert os.path.isfile(os.path.join(os.getcwd(), "converted/gml.csv"))
    os.remove(os.path.join(os.getcwd(), "converted/gml.csv"))


def test_save_converted_kml():
    path = os.path.join(os.getcwd(), "tests/data/resource_examples/kml.resource")
    log = DatasetResourceLog()
    ConvertPhase(
        path,
        dataset_resource_log=log,
        custom_temp_dir="data",
    ).process()

    assert os.path.isfile(os.path.join(os.getcwd(), "converted/kml.csv"))
    os.remove(os.path.join(os.getcwd(), "converted/kml.csv"))
