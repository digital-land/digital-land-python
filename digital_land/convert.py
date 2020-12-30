import csv
import logging
import os
import pathlib
import sqlite3
import subprocess
import tempfile
import zipfile
from io import StringIO

import pandas as pd

from .load import detect_file_encoding, reader_with_line, resource_hash_from


class Converter:
    def __init__(self, conversions={}):
        self.conversions = conversions

    def convert(self, input_path):
        reader = self._read_binary_file(input_path)

        if not reader:
            encoding = detect_file_encoding(input_path)
            if encoding:
                logging.debug("encoding detected: %s", encoding)
                reader = self._read_text_file(input_path, encoding)

        if not reader:
            logging.debug("failed to create reader, cannot process ", input_path)
            reader = iter(())  # Empty iterator, immediately sends StopIteration

        return reader_with_line(reader, resource=resource_hash_from(input_path))

    def _read_text_file(self, input_path, encoding):
        f = read_csv(input_path, encoding)
        content = f.read(10)
        f.seek(0)
        converted_csv_file = None

        if content.lower().startswith("<!doctype "):
            logging.warn(f"{input_path} has <!doctype, IGNORING!")
            f.close()
            return None

        elif content.lower().startswith(("<?xml ", "<wfs:")):
            logging.debug(f"{input_path} looks like xml")
            converted_csv_file = convert_features_to_csv(input_path)

        elif content.lower().startswith("{"):
            logging.debug(f"{input_path} looks like json")
            converted_csv_file = convert_features_to_csv(input_path)

        if converted_csv_file:
            f.close()
            reader = read_csv(converted_csv_file)
        else:
            reader = f

        return reader

    def _read_binary_file(self, input_path):
        # First try excel
        excel_reader = read_excel(input_path)
        if excel_reader:
            logging.debug(f"{input_path} looks like excel")
            return excel_reader

        # Then try zip
        if zipfile.is_zipfile(input_path):
            logging.debug(f"{input_path} looks like zip")
            internal_path = self._path_to_shp_files(input_path)
            temp_path = tempfile.NamedTemporaryFile(suffix=".zip").name
            os.link(input_path, temp_path)
            zip_path = f"/vsizip/{temp_path}{internal_path}"
            csv_path = convert_features_to_csv(zip_path)
            return read_csv(csv_path)

        # Then try SQLite (GeoPackage)
        try:
            conn = sqlite3.connect(input_path)
            cursor = conn.cursor()
            cursor.execute("pragma quick_check")
        except:  # noqa: E722
            pass
        else:
            logging.debug(f"{input_path} looks like SQLite")
            csv_path = convert_features_to_csv(input_path)
            return read_csv(csv_path)

        return None

    def _path_to_shp_files(self, input_file):
        zip_ = zipfile.ZipFile(input_file)
        files = zip_.namelist()
        shp_files = filter(lambda s: s.endswith(".shp"), files)
        shp_dirs = set([pathlib.Path(file).parent for file in shp_files])
        if len(shp_dirs) != 1:
            raise ValueError(
                "Expected exactly one directory containing shp files, but none found"
            )
        shp_dir = shp_dirs.pop()
        if shp_dir.name:
            return f"/{shp_dir}"
        return ""


def execute(command):
    proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    try:
        outs, errs = proc.communicate(timeout=600)
    except subprocess.TimeoutExpired:
        proc.kill()
        outs, errs = proc.communicate()

    return proc.returncode, outs.decode("utf-8"), errs.decode("utf-8")


def read_csv(input_path, encoding="utf-8"):
    logging.debug("encoding", encoding)
    return open(input_path, encoding=encoding, newline=None)


def read_excel(path):
    try:
        excel = pd.read_excel(path)
    except:  # noqa: E722
        return None

    string = excel.to_csv(
        index=None, header=True, encoding="utf-8", quoting=csv.QUOTE_ALL
    )
    f = StringIO(string)

    return f


def convert_features_to_csv(input_path):
    output_path = temp_file_for(input_path)
    execute(
        [
            "ogr2ogr",
            "-oo",
            "DOWNLOAD_SCHEMA=NO",
            "-lco",
            "GEOMETRY=AS_WKT",
            "-lco",
            "LINEFORMAT=CRLF",
            "-f",
            "CSV",
            "-nlt",
            "MULTIPOLYGON",
            "-nln",
            "MERGED",
            "--config",
            "OGR_WKT_PRECISION",
            "10",
            output_path,
            input_path,
        ]
    )
    return output_path


def temp_file_for(input_path):
    return tempfile.NamedTemporaryFile(
        suffix=f"_{resource_hash_from(input_path)}.csv",
    ).name
