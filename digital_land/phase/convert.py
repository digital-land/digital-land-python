import csv
from cchardet import UniversalDetector
import logging
import json_stream
import os
import os.path
import sqlite3
import subprocess
import tempfile
import time
import zipfile
from packaging.version import Version
import pandas as pd
from .load import Stream
from .phase import Phase
from ..utils.gdal_utils import get_gdal_version
from digital_land.log import ConvertedResourceLog
from requests import HTTPError


class ConversionError(Exception):
    pass


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


def load_csv(path, encoding="UTF-8", log=None):
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

    return Stream(path, f=f, log=log)


def execute(command, env=os.environ):
    logging.debug("execute: %s", command)
    proc = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env
    )

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

    return excel


def convert_features_to_csv(input_path, output_path=None):
    if not output_path:
        output_path = tempfile.NamedTemporaryFile(suffix=".csv").name

    gdal_version = get_gdal_version()

    command = [
        "ogr2ogr",
        "-oo",
        "DOWNLOAD_SCHEMA=NO",
        "-lco",
        "GEOMETRY=AS_WKT",
        "-lco",
        "GEOMETRY_NAME=WKT",
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
    env = (
        dict(os.environ, OGR_GEOJSON_MAX_OBJ_SIZE="0")
        if gdal_version >= Version("3.5.2")
        else dict(os.environ)
    )

    rc, outs, errs = execute(command, env=env)

    if rc != 0:
        raise ConversionError(
            f"ogr2ogr failed ({rc}). stdout='{outs}', stderr='{errs}'. gdal version {gdal_version}"
        )

    if not os.path.isfile(output_path):
        return None

    return output_path


def save_efficient_json_as_csv(output_path, columns, data):
    with open(output_path, "w") as csv_file:
        cw = csv.writer(csv_file)
        cw.writerow(columns)

        for row in data:
            cw.writerow(row)


def convert_json_to_csv(input_path, encoding, output_path=None):
    if not output_path:
        output_path = tempfile.NamedTemporaryFile(suffix=".csv").name
    with open(input_path, "r", encoding=encoding) as json:
        js = json_stream.load(json)

        columns = None
        data = None

        for item in js.items():
            if item[0] in ["columns"]:
                columns = [x for x in item[1].persistent()]
                if data is not None:
                    save_efficient_json_as_csv(
                        output_path,
                        columns,
                        data,
                    )
                    return output_path

            if item[0] in ["data"]:
                if columns is not None:
                    save_efficient_json_as_csv(
                        output_path,
                        columns,
                        item[1],
                    )
                    return output_path
                else:
                    data = [x for x in item[1].persistent()]

        return convert_features_to_csv(
            input_path,
            output_path,
        )


class ConvertPhase(Phase):
    def __init__(
        self,
        path=None,
        dataset_resource_log=None,
        converted_resource_log=None,
        output_path=None,
    ):
        """
        given a fie/filepath will aim to convert  it to a csv and return the path to a csv, if the file is aready a csv

        Args:
            path (str): Path to the shapefile or geojson
            dataset_resource_log (DatasetResourceLog): DatasetResourceLog object
            converted_resource_log (ConvertedResourceLog): ConvertedResourceLog object
            output_path (str): Optional output path for the converted csv
        """
        self.path = path
        self.dataset_resource_log = dataset_resource_log
        self.converted_resource_log = converted_resource_log
        self.charset = ""
        self.output_path = output_path
        if output_path:
            output_dir = os.path.dirname(output_path)
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)

    def process(self, stream=None):
        input_path = self.path
        start_time = time.time()

        try:
            reader = self._read_binary_file(input_path)

            if not reader:
                encoding = detect_file_encoding(input_path)
                if encoding:
                    logging.debug("encoding detected: %s", encoding)
                    self.charset = ";charset=" + encoding
                    reader = self._read_text_file(input_path, encoding)

            if not reader:
                raise ConversionError(
                    f"failed to create reader, cannot process {input_path}"
                )

                # raise StopIteration()
                reader = iter(())

            if self.converted_resource_log:
                self.converted_resource_log.add(
                    elapsed=time.time() - start_time,
                    status=ConvertedResourceLog.Success,
                )

            return Stream(input_path, f=reader, log=self.dataset_resource_log)

        except Exception as ex:
            if self.converted_resource_log:
                self.converted_resource_log.add(
                    elapsed=time.time() - start_time,
                    status=ConvertedResourceLog.Failed,
                    exception=str(ex),
                )

            return Stream(input_path, f=iter(()), log=self.dataset_resource_log)

    # should  this  be  a method and not a function? I think we  re-factor it  into a function let's remove references to self
    def _read_text_file(self, input_path, encoding):
        if encoding.lower() == "windows-1252":
            temp_path = input_path + ".temp"
            try:
                with open(input_path, "r", encoding=encoding) as input_file:
                    with open(temp_path, "w", encoding="UTF-8") as output_file:
                        output_file.write(input_file.read())
                        os.replace(temp_path, input_path)
                        encoding = "UTF-8"
            except Exception as e:
                raise HTTPError(
                    f"Failed to convert {input_path} from Windows-1252 to UTF-8 - {e}"
                )

        f = read_csv(input_path, encoding)
        self.dataset_resource_log.mime_type = "text/csv" + self.charset
        content = f.read(10)
        f.seek(0)
        converted_csv_file = None

        if content.lower().startswith("<!doctype "):
            self.dataset_resource_log.mime_type = "text/html" + self.charset
            logging.warn("%s has <!doctype, IGNORING!", input_path)
            f.close()
            return None

        elif content.lower().startswith(("<?xml ", "<wfs:")):
            logging.debug("%s looks like xml", input_path)
            self.dataset_resource_log.mime_type = "application/xml" + self.charset
            converted_csv_file = convert_features_to_csv(
                input_path,
                self.output_path,
            )
            if not converted_csv_file:
                f.close()
                logging.warning("conversion from XML to CSV failed")
                return None

        elif content.lower().startswith("{"):
            logging.debug("%s looks like json", input_path)
            self.dataset_resource_log.mime_type = "application/json" + self.charset
            converted_csv_file = convert_json_to_csv(
                input_path,
                encoding,
                self.output_path,
            )

        if converted_csv_file:
            f.close()
            reader = read_csv(converted_csv_file)
        else:
            reader = f

        return reader

    def _find_zip_file(self, input_file, suffix=".gml"):
        zip_ = zipfile.ZipFile(input_file)
        files = zip_.namelist()
        files = list(
            set(
                filter(
                    lambda s: s.endswith(suffix) or s.endswith(suffix.upper()), files
                )
            )
        )
        if not files or not len(files):
            return None
        if len(files) > 1:
            raise ValueError("Zipfile contains more than one %s file" % suffix)
        return "/" + files[0]

    def find_internal_path(self, input_path):
        internal_path = self._find_zip_file(input_path, ".shp")
        if internal_path:
            return internal_path, "x-gis/x-shapefile"

        internal_path = self._find_zip_file(input_path, ".gml")
        if internal_path:
            return internal_path, "application/gml+xml"

        internal_path = self._find_zip_file(input_path, ".tab")
        if internal_path:
            return internal_path, "x-gis/x-mapinfo-tab"

        internal_path = self._find_zip_file(input_path, ".geojson")
        if internal_path:
            return internal_path, "application/vnd.geo+json"

        internal_path = self._find_zip_file(input_path, ".json")
        if internal_path:
            return internal_path, "application/vnd.geo+json"

        internal_path = self._find_zip_file(input_path, ".kml")
        if internal_path:
            return internal_path, "application/vnd.google-earth.kml+xml"

        return None, None

    def _read_binary_file(self, input_path):
        # First try excel
        excel = read_excel(input_path)
        if excel is not None:
            logging.debug(f"{input_path} looks like excel")
            self.dataset_resource_log.mime_type = "application/vnd.ms-excel"
            if not self.output_path:
                self.output_path = tempfile.NamedTemporaryFile(
                    suffix=".csv", delete=False
                ).name
            excel.to_csv(
                self.output_path,
                index=False,
                header=True,
                encoding="utf-8",
                quoting=csv.QUOTE_ALL,
            )

            return read_csv(self.output_path, encoding="utf-8")

        # Then try zip
        if zipfile.is_zipfile(input_path):
            logging.debug(f"{input_path} looks like zip")
            self.dataset_resource_log.mime_type = "application/zip"

            internal_path, mime_type = self.find_internal_path(input_path)
            if internal_path:
                self.dataset_resource_log.internal_path = internal_path
                self.dataset_resource_log.internal_mime_type = mime_type
                # TODO erpace temp path with output path
                if self.output_path:
                    temp_path = tempfile.NamedTemporaryFile(
                        suffix=".zip", dir=str(self.output_path.parent)
                    ).name
                else:
                    temp_path = tempfile.NamedTemporaryFile(suffix=".zip").name
                os.link(input_path, temp_path)
                zip_path = f"/vsizip/{temp_path}{internal_path}"
                logging.debug(f"zip_path: {zip_path} mime_type: {mime_type}")
                csv_path = convert_features_to_csv(zip_path, self.output_path)
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
            self.dataset_resource_log.mime_type = "application/geopackage+sqlite3"
            csv_path = convert_features_to_csv(input_path, self.output_path)
            encoding = detect_file_encoding(csv_path)
            return read_csv(csv_path, encoding)

        return None
