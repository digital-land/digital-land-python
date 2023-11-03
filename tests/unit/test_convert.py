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


def test_converted_csv_saved():
    path = os.path.join(os.getcwd(), "tests/data/brentwood.xlsm")
    log = DatasetResourceLog()
    reader = ConvertPhase(
        path,
        dataset_resource_log=log,
        custom_temp_dir="data",
        custom_temp_file="input.csv",
    ).process()
    block = next(reader)
    assert block["resource"] == "brentwood"
    assert os.path.isfile(os.path.join(os.getcwd(), "data/input.csv"))
