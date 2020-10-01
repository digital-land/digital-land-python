import csv
import logging
import subprocess
import tempfile
import zipfile
from io import StringIO

import pandas as pd

from .load import detect_encoding, reader_with_line, resource_hash_from


class Converter:
    def __init__(self, conversions={}):
        self.conversions = conversions

    def convert(self, input_path):
        encoding = detect_encoding(input_path)
        logging.debug("encoding detected: %s", encoding)
        if encoding:
            return self._read_text_file(input_path, encoding)
        else:
            return self._read_binary_file(input_path)

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
            reader = read_csv(converted_csv_file, "utf-8")
        else:
            reader = f

        return reader_with_line(reader, resource_hash_from(input_path))

    def _read_binary_file(self, input_path):
        # First try excel
        excel_reader = read_excel(input_path)
        if excel_reader:
            return excel_reader


def execute(command):
    proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    try:
        outs, errs = proc.communicate(timeout=15)
    except subprocess.TimeoutExpired:
        proc.kill()
        outs, errs = proc.communicate()

    return proc.returncode, outs.decode("utf-8"), errs.decode("utf-8")


def read_csv(input_path, encoding):
    return open(input_path, encoding=encoding, newline="")


def read_excel(path):
    try:
        excel = pd.read_excel(path)
    except:  # noqa: E722
        return None

    string = excel.to_csv(
        index=None, header=True, encoding="utf-8", quoting=csv.QUOTE_ALL
    )
    f = StringIO(string)

    return reader_with_line(f, resource_hash_from(path))


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
            "-nln",
            "MERGED",
            output_path,
            input_path,
        ]
    )
    return output_path


def temp_file_for(input_path):
    return tempfile.NamedTemporaryFile(
        suffix=f"_{resource_hash_from(input_path)}.csv",
    ).name
