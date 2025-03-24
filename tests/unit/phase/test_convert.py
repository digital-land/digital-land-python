#!/usr/bin/env -S py.test -svv
import os
import csv
import pytest

from digital_land.log import DatasetResourceLog, ConvertedResourceLog
from digital_land.phase.convert import ConvertPhase


class TestConvertPhase:
    def test_process_xlsm_is_loaded(self):
        path = os.path.join(os.getcwd(), "tests/data/brentwood.xlsm")
        dataset_resource_log = DatasetResourceLog()
        converted_resource_log = ConvertedResourceLog()
        reader = ConvertPhase(
            path,
            dataset_resource_log=dataset_resource_log,
            converted_resource_log=converted_resource_log,
        ).process()
        block = next(reader)
        assert block["resource"] == "brentwood"
        assert block["line-number"] == 1
        assert "OrganisationURI" in block["line"]
        assert dataset_resource_log.mime_type == "application/vnd.ms-excel"

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
        dataset_resource_log = DatasetResourceLog()
        converted_resource_log = ConvertedResourceLog()
        ConvertPhase(
            path,
            dataset_resource_log=dataset_resource_log,
            converted_resource_log=converted_resource_log,
            output_path=output_path,
        ).process()

        assert os.path.isfile(output_path)
        converted_resource_log_path = os.path.join(tmp_path, "converted-resource.csv")

        converted_resource_log.save(converted_resource_log_path)
        with open(converted_resource_log_path, "r") as f:
            rows = [r for r in csv.DictReader(f)]

            assert len(rows) == 1
            assert rows[0]["status"] == "success"
            assert rows[0]["exception"] == ""

        # os.remove(os.path.join(os.getcwd(), "converted/geojson.csv"))

    def test_process_converted_file_not_saved(self):
        files_before = []
        for root, dirs, files in os.walk(os.getcwd()):
            files_before.extend(files)

        path = os.path.join(
            os.getcwd(),
            "tests/expectations/resources_to_test_expectations/data_for_url_expect_test.sqlite3",
        )
        dataset_resource_log = DatasetResourceLog()
        converted_resource_log = ConvertedResourceLog()
        ConvertPhase(
            path,
            dataset_resource_log=dataset_resource_log,
            converted_resource_log=converted_resource_log,
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
        dataset_resource_log = DatasetResourceLog()
        converted_resource_log = ConvertedResourceLog()
        reader = ConvertPhase(
            path,
            dataset_resource_log=dataset_resource_log,
            converted_resource_log=converted_resource_log,
            output_path=output_path,
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
        dataset_resource_log = DatasetResourceLog()
        converted_resource_log = ConvertedResourceLog()
        reader = ConvertPhase(
            path,
            dataset_resource_log=dataset_resource_log,
            converted_resource_log=converted_resource_log,
            output_path=output_path,
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
        dataset_resource_log = DatasetResourceLog()
        converted_resource_log = ConvertedResourceLog()
        reader = ConvertPhase(
            path,
            dataset_resource_log=dataset_resource_log,
            converted_resource_log=converted_resource_log,
            output_path=output_path,
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

    def test_conversion_throws_exception(self, tmp_path):
        input_path = "tests/data/resource_examples/kml-bad.resource"

        path = os.path.join(os.getcwd(), input_path)
        output_path = os.path.join(tmp_path, "converted/kml.csv")
        dataset_resource_log = DatasetResourceLog()
        converted_resource_log = ConvertedResourceLog()
        ConvertPhase(
            path,
            dataset_resource_log=dataset_resource_log,
            converted_resource_log=converted_resource_log,
            output_path=output_path,
        ).process()

        assert not os.path.isfile(output_path)
        converted_resource_log_path = os.path.join(tmp_path, "converted-resource.csv")

        converted_resource_log.save(converted_resource_log_path)
        with open(converted_resource_log_path, "r") as f:
            rows = [r for r in csv.DictReader(f)]

            assert len(rows) == 1
            assert rows[0]["status"] == "failed"
            assert rows[0]["exception"].startswith("ogr2ogr failed (1)")

    def test_process_file_encoded_to_utf8(self, tmp_path):
        path = os.path.join(
            os.getcwd(), "tests/data/resource_examples/windows_encoded.resource"
        )

        with open(path, "w", encoding="windows-1252") as f:
            f.write(
                """reference,name,point,notes,start_date,entry_date\nref,"name – Tree",POINT (),notes,01/01/2025,01/01/2025"""
            )

        output_path = os.path.join(tmp_path, "converted/windows-encoded.resource.csv")

        dataset_resource_log = DatasetResourceLog()
        converted_resource_log = ConvertedResourceLog()
        reader = ConvertPhase(
            path,
            dataset_resource_log=dataset_resource_log,
            converted_resource_log=converted_resource_log,
            output_path=output_path,
        ).process()

        headers = next(reader)
        assert headers["line"][0] == "reference"
        assert headers["line"][1] == "name"

        block = next(reader)
        assert block["line"][0] == "ref"
        assert block["line"][1] == "name – Tree"
