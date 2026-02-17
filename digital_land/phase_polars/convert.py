import csv
import logging
import os
import tempfile
import time
from pathlib import Path

import polars as pl

from .phase import PolarsPhase
from ..phase.convert import (
    ConversionError,
    convert_features_to_csv,
    convert_json_to_csv,
    detect_file_encoding,
    read_csv,
    read_excel,
)
from ..log import ConvertedResourceLog

import sqlite3
import zipfile

logger = logging.getLogger(__name__)


class ConvertPhase(PolarsPhase):
    """
    Detect and convert input file format then load into a Polars DataFrame.

    Re-uses the existing format-detection and conversion helpers so the
    behaviour is identical to the streaming ConvertPhase.
    """

    def __init__(
        self,
        path=None,
        dataset_resource_log=None,
        converted_resource_log=None,
        output_path=None,
    ):
        self.path = path
        self.dataset_resource_log = dataset_resource_log
        self.converted_resource_log = converted_resource_log
        self.charset = ""
        self.output_path = output_path
        if output_path:
            output_dir = os.path.dirname(str(output_path))
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir)

    def _resource_from_path(self, path):
        return Path(path).stem

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
        for suffix, mime in [
            (".shp", "x-gis/x-shapefile"),
            (".gml", "application/gml+xml"),
            (".tab", "x-gis/x-mapinfo-tab"),
            (".geojson", "application/vnd.geo+json"),
            (".json", "application/vnd.geo+json"),
            (".kml", "application/vnd.google-earth.kml+xml"),
        ]:
            internal_path = self._find_zip_file(input_path, suffix)
            if internal_path:
                return internal_path, mime
        return None, None

    def _get_csv_path(self, input_path):
        """Return (csv_path, should_delete_temp) by converting the input to CSV if needed."""

        # Try binary formats first
        excel = read_excel(input_path)
        if excel is not None:
            logger.debug(f"{input_path} looks like excel")
            if self.dataset_resource_log:
                self.dataset_resource_log.mime_type = "application/vnd.ms-excel"
            tmp = self.output_path or tempfile.NamedTemporaryFile(
                suffix=".csv", delete=False
            ).name
            excel.to_csv(
                str(tmp), index=False, header=True, encoding="utf-8", quoting=csv.QUOTE_ALL
            )
            return str(tmp), False

        if zipfile.is_zipfile(input_path):
            logger.debug(f"{input_path} looks like zip")
            if self.dataset_resource_log:
                self.dataset_resource_log.mime_type = "application/zip"
            internal_path, mime_type = self.find_internal_path(input_path)
            if internal_path:
                if self.dataset_resource_log:
                    self.dataset_resource_log.internal_path = internal_path
                    self.dataset_resource_log.internal_mime_type = mime_type
                parent = str(self.output_path.parent) if self.output_path else None
                tmp = tempfile.NamedTemporaryFile(suffix=".zip", dir=parent).name
                os.link(input_path, tmp)
                zip_path = f"/vsizip/{tmp}{internal_path}"
                csv_path = convert_features_to_csv(zip_path, self.output_path)
                return csv_path, False

        try:
            conn = sqlite3.connect(input_path)
            cursor = conn.cursor()
            cursor.execute("pragma quick_check")
            conn.close()
            logger.debug(f"{input_path} looks like SQLite")
            if self.dataset_resource_log:
                self.dataset_resource_log.mime_type = "application/geopackage+sqlite3"
            csv_path = convert_features_to_csv(input_path, self.output_path)
            return csv_path, False
        except Exception:
            pass

        # Text-based formats
        encoding = detect_file_encoding(input_path)
        if not encoding:
            raise ConversionError(f"Cannot detect encoding for {input_path}")

        self.charset = ";charset=" + encoding
        with open(input_path, encoding=encoding) as f:
            content = f.read(10)

        if content.lower().startswith("<!doctype "):
            if self.dataset_resource_log:
                self.dataset_resource_log.mime_type = "text/html" + self.charset
            raise ConversionError(f"{input_path} is HTML")

        if content.lower().startswith(("<?xml ", "<wfs:")):
            logger.debug("%s looks like xml", input_path)
            if self.dataset_resource_log:
                self.dataset_resource_log.mime_type = "application/xml" + self.charset
            csv_path = convert_features_to_csv(input_path, self.output_path)
            if not csv_path:
                raise ConversionError("XML to CSV conversion failed")
            return csv_path, False

        if content.lower().startswith("{") or content.lower().startswith("[{"):
            logger.debug("%s looks like json", input_path)
            if self.dataset_resource_log:
                self.dataset_resource_log.mime_type = (
                    "application/json" + self.charset
                )
            csv_path = convert_json_to_csv(input_path, encoding, self.output_path)
            return csv_path, False

        # plain CSV
        if self.dataset_resource_log:
            self.dataset_resource_log.mime_type = "text/csv" + self.charset
        return str(input_path), False

    def process(self, df=None):
        input_path = self.path
        resource = self._resource_from_path(input_path)
        start_time = time.time()

        try:
            csv_path, _ = self._get_csv_path(input_path)

            result = pl.read_csv(
                csv_path,
                infer_schema_length=0,  # read everything as strings
                null_values=[""],
                truncate_ragged_lines=True,
                ignore_errors=True,
            )

            # Replace nulls with empty strings to match streaming behaviour
            result = result.with_columns(
                pl.all().cast(pl.Utf8).fill_null("")
            )

            n = result.height
            result = result.with_columns(
                pl.lit(resource).alias("__resource"),
                pl.arange(2, n + 2).alias("__line_number"),  # 1-based, skip header
                pl.arange(1, n + 1).alias("__entry_number"),
                pl.lit(str(input_path)).alias("__path"),
            )

            if self.dataset_resource_log:
                if not self.dataset_resource_log.resource:
                    self.dataset_resource_log.resource = resource
                self.dataset_resource_log.line_count = n + 1  # include header

            if self.converted_resource_log:
                self.converted_resource_log.add(
                    elapsed=time.time() - start_time,
                    status=ConvertedResourceLog.Success,
                )

            return result

        except Exception as ex:
            logger.error(f"ConvertPhase failed: {ex}")
            if self.converted_resource_log:
                self.converted_resource_log.add(
                    elapsed=time.time() - start_time,
                    status=ConvertedResourceLog.Failed,
                    exception=str(ex),
                )
            # Return empty DataFrame with metadata columns
            return pl.DataFrame(
                {
                    "__resource": pl.Series([], dtype=pl.Utf8),
                    "__line_number": pl.Series([], dtype=pl.Int64),
                    "__entry_number": pl.Series([], dtype=pl.Int64),
                    "__path": pl.Series([], dtype=pl.Utf8),
                }
            )
