import csv
from cchardet import UniversalDetector
import logging
import os
import os.path
import sqlite3
import subprocess
import tempfile
import zipfile
from io import StringIO
import pandas as pd
from ..stream import Stream
from .phase import Phase


def detect_file_encoding(path):
    with open(path, "rb") as f:
        return detect_encoding(f)


def detect_encoding(f):
    detector = UniversalDetector()
    detector.reset()
    for line in f:
        detector.feed(line)
        if detector.done:
            break
    detector.close()
    return detector.result["encoding"]


def load_csv(path, encoding="UTF-8"):
    logging.debug(f"trying csv {path}")

    if not encoding:
        encoding = detect_file_encoding(path)

        if not encoding:
            return None

        logging.debug(f"detected encoding {encoding}")

    f = open(path, encoding=encoding, newline=None)
    content = f.read()
    if content.lower().startswith("<!doctype "):
        logging.debug(f"{path} has <!doctype")
        return None

    f.seek(0)

    return Stream(path, f=f)


def execute(command):
    proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    try:
        outs, errs = proc.communicate(timeout=600)
    except subprocess.TimeoutExpired:
        proc.kill()
        outs, errs = proc.communicate()

    return proc.returncode, outs.decode("utf-8"), errs.decode("utf-8")


def read_csv(input_path, encoding="utf-8"):
    logging.debug("reading %s with encoding %s", input_path, encoding)
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
    output_path = tempfile.NamedTemporaryFile(suffix=".csv").name
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
    if not os.path.isfile(output_path):
        return None

    return output_path


class ConvertPhase(Phase):
    def process(self, input_path):
        reader = self._read_binary_file(input_path)

        if not reader:
            encoding = detect_file_encoding(input_path)
            if encoding:
                logging.debug("encoding detected: %s", encoding)
                reader = self._read_text_file(input_path, encoding)

        if not reader:
            logging.debug("failed to create reader, cannot process %s", input_path)

            # raise StopIteration()
            reader = iter(())

        return Stream(input_path, f=reader)

    def _read_text_file(self, input_path, encoding):
        f = read_csv(input_path, encoding)
        content = f.read(10)
        f.seek(0)
        converted_csv_file = None

        if content.lower().startswith("<!doctype "):
            logging.warn("%s has <!doctype, IGNORING!", input_path)
            f.close()
            return None

        elif content.lower().startswith(("<?xml ", "<wfs:")):
            logging.debug("%s looks like xml", input_path)
            converted_csv_file = convert_features_to_csv(input_path)
            if not converted_csv_file:
                f.close()
                logging.warning("conversion from XML to CSV failed")
                return None

        elif content.lower().startswith("{"):
            logging.debug("%s looks like json", input_path)
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
            internal_path = self._find_zip_file(
                input_path, ".shp"
            ) or self._find_zip_file(input_path, ".gml")
            if internal_path:
                temp_path = tempfile.NamedTemporaryFile(suffix=".zip").name
                os.link(input_path, temp_path)
                zip_path = f"/vsizip/{temp_path}{internal_path}"
                logging.debug("zip_path: %s" % zip_path)
                csv_path = convert_features_to_csv(zip_path)
                encoding = detect_file_encoding(csv_path)
                return read_csv(csv_path, encoding)

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
            encoding = detect_file_encoding(csv_path)
            return read_csv(csv_path, encoding)

        return None

    def _find_zip_file(self, input_file, suffix=".gml"):
        zip_ = zipfile.ZipFile(input_file)
        files = zip_.namelist()
        files = list(set(filter(lambda s: s.endswith(suffix), files)))
        if not files or not len(files):
            return None
        if len(files) > 1:
            raise ValueError("Zipfile contains more than one %s file" % suffix)
        return "/" + files[0]
