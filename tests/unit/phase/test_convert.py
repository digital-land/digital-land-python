#!/usr/bin/env -S py.test -svv
import os
import pytest

from digital_land.log import DatasetResourceLog
from digital_land.phase.convert import ConvertPhase


class TestConvertPhase:
    def test_process_xlsm_is_loaded(self):
        path = os.path.join(os.getcwd(), "tests/data/brentwood.xlsm")
        log = DatasetResourceLog()
        reader = ConvertPhase(path, dataset_resource_log=log).process()
        block = next(reader)
        assert block["resource"] == "brentwood"
        assert block["line-number"] == 1
        assert "OrganisationURI" in block["line"]
        assert log.mime_type == "application/vnd.ms-excel"

    @pytest.mark.parametrize(
        "input_path",
        [
            "tests/data/resource_examples/geojson.resource",
            "tests/data/resource_examples/geopackage.resource",
            "tests/data/resource_examples/gml.resource",
            "tests/data/resource_examples/kml.resource",
            "tests/expectations/resources_to_test_expectations/data_for_url_expect_test.sqlite3",
        ],
    )
    def test_process_file_is_saved_to_output_path(self, input_path, tmp_path):
        path = os.path.join(os.getcwd(), input_path)
        output_path = os.path.join(tmp_path, "converted/geojson.csv")
        log = DatasetResourceLog()
        ConvertPhase(path, dataset_resource_log=log, output_path=output_path).process()

        assert os.path.isfile(output_path)
        # os.remove(os.path.join(os.getcwd(), "converted/geojson.csv"))

    def test_process_converted_file_not_saved(self):
        files_before = []
        for root, dirs, files in os.walk(os.getcwd()):
            files_before.extend(files)

        path = os.path.join(
            os.getcwd(),
            "tests/expectations/resources_to_test_expectations/data_for_url_expect_test.sqlite3",
        )
        log = DatasetResourceLog()
        ConvertPhase(
            path,
            dataset_resource_log=log,
        ).process()

        files_after = []
        for root, dirs, files in os.walk(os.getcwd()):
            files_after.extend(files)

        assert files_before == files_after

    def test_process_efficient_json_format(self, tmp_path):
        path = os.path.join(
            os.getcwd(), "tests/data/resource_examples/json_efficient_format.resource"
        )
        output_path = os.path.join(
            tmp_path, "converted/json_efficient_format.resource.csv"
        )
        log = DatasetResourceLog()
        reader = ConvertPhase(
            path, dataset_resource_log=log, output_path=output_path
        ).process()

        assert os.path.isfile(output_path)

        headers = next(reader)
        assert headers["line"][0] == "reference"
        assert headers["line"][1] == "name"

        block = next(reader)
        assert block["line"][0] == "ref"
        assert block["line"][1] == "name"

    def test_process_efficient_json_format_data_first(self, tmp_path):
        path = os.path.join(
            os.getcwd(),
            "tests/data/resource_examples/json_efficient_format_data_first.resource",
        )
        output_path = os.path.join(
            tmp_path, "converted/json_efficient_format_data_first.resource.csv"
        )
        log = DatasetResourceLog()
        reader = ConvertPhase(
            path, dataset_resource_log=log, output_path=output_path
        ).process()

        assert os.path.isfile(output_path)

        headers = next(reader)
        assert headers["line"][0] == "reference"
        assert headers["line"][1] == "name"

        block = next(reader)
        assert block["line"][0] == "ref"
        assert block["line"][1] == "name"

    def test_process_xlsx_file(self, tmp_path):
        path = os.path.join(
            os.getcwd(),
            "tests/data/resource_examples/xlsx_res.xlsx",
        )
        output_path = os.path.join(tmp_path, "converted/xlsx_res.csv")
        log = DatasetResourceLog()
        reader = ConvertPhase(
            path, dataset_resource_log=log, output_path=output_path
        ).process()

        assert os.path.isfile(output_path)

        headers = next(reader)
        assert headers["line"][0] == "Organisation URL"
        assert headers["line"][1] == "Organisation Label"

        block = next(reader)
        assert (
            block["line"][0]
            == "http://opendatacommunities.org/id/district-council/south-cambridgeshire"
        )
        assert block["line"][1] == "South Cambridgeshire District Council"
