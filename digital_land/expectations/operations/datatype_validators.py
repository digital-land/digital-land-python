import json
import re
import urllib.parse
from datetime import datetime
from decimal import Decimal, InvalidOperation

import shapely.errors
import shapely.wkt
from shapely.geometry import GeometryCollection, MultiPolygon, Point, Polygon, shape


def _is_valid_datetime_value(value):
    value = value.strip().strip('",').lower()

    # https://github.com/digital-land/digital-land-python/blob/1dbbad99e0c5939d87d5a8a8ece372e4c43eba77/digital_land/datatype/date.py#L22
    patterns = [
        # Date/date-like formats
        "%Y-%m-%d",
        "%Y%m%d",
        "%Y/%m/%d",
        "%Y %m %d",
        "%Y.%m.%d",
        "%Y-%d-%m",  # risky!
        "%Y-%m",
        "%Y.%m",
        "%Y/%m",
        "%Y %m",
        "%Y",
        "%Y.0",
        "%d/%m/%Y",
        "%d/%m/%y",
        "%d-%m-%Y",
        "%d-%m-%y",
        "%d.%m.%Y",
        "%d.%m.%y",
        "%d-%b-%Y",
        "%d-%b-%y",
        "%d %B %Y",
        "%b %d, %Y",
        "%b %d, %y",
        "%b-%y",
        "%B %Y",
        "%m/%d/%Y",  # risky!
        # Datetime formats
        "%Y-%m-%dT%H:%M:%S.000Z",
        "%Y-%m-%dT%H:%M:%S.000",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y/%m/%d %H:%M:%S%z",
        "%Y/%m/%d %H:%M:%S+00",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%Y/%m/%dT%H:%M:%S",
        "%Y/%m/%dT%H:%M:%S.000Z",
        "%Y/%m/%dT%H:%M:%S.000",
        "%Y/%m/%dT%H:%M:%S.%fZ",
        "%Y/%m/%dT%H:%M:%S.%f%z",
        "%Y/%m/%dT%H:%M:%S.%f",
        "%Y/%m/%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
    ]

    # Handle fractional seconds with extra precision.
    if "." in value and "Z" in value:
        parts = value.replace("Z", "").split(".")
        if len(parts) == 2 and len(parts[1]) > 6:
            value = parts[0] + "." + parts[1][:6] + "Z"
    elif "." in value and "+" in value:
        parts = value.split("+")
        base_part = parts[0]
        tz_part = "+" + parts[1]
        if "." in base_part:
            date_time, frac = base_part.rsplit(".", 1)
            if len(frac) > 6:
                frac = frac[:6]
            value = date_time + "." + frac + tz_part

    for pattern in patterns:
        try:
            datetime.strptime(value, pattern)
            return True
        except ValueError:
            continue

    # Try unix timestamp
    try:
        float_val = float(value)
        return -62135596800 < float_val < 253402300800  # Year 1 to 9999
    except ValueError:
        pass

    return False


def _is_valid_integer_value(value):
    try:
        num = float(value)
        return num == int(num)
    except (ValueError, OverflowError):
        return False


def _is_valid_decimal_value(value):
    try:
        Decimal(value)
        return True
    except (InvalidOperation, ValueError):
        return False


def _is_valid_flag_value(value):
    value = value.strip().lower()

    lookup = {
        "y": "yes",
        "n": "no",
        "true": "yes",
        "false": "no",
    }

    normalized = lookup.get(value, value)
    return normalized in {"", "yes", "no"}


def _is_valid_json_value(value):
    try:
        json.loads(value)
        return True
    except json.JSONDecodeError:
        return False


def _is_valid_reference_value(value):
    return bool(value.strip()) and not any(ch.isspace() for ch in value)


def _is_valid_curie_value(value):
    return bool(re.fullmatch(r"[a-z0-9-]+:[^\s:][^\s]*", value))


def _is_valid_curie_list_value(value):
    text = (value or "").strip()
    if not text:
        return False

    parts = [part.strip() for part in text.split(";")]
    if any(not part for part in parts):
        return False

    curie_re = re.compile(r"[a-z0-9-]+:[^\s:][^\s]*")
    return all(bool(curie_re.fullmatch(part)) for part in parts)


def _is_valid_address_value(value):
    if not value or not value.strip():
        return False

    value = value.strip()

    # https://github.com/digital-land/digital-land-python/blob/1dbbad99e0c5939d87d5a8a8ece372e4c43eba77/digital_land/datatype/address.py#L10
    value = ", ".join(value.split("\n"))
    value = value.replace(";", ",")

    comma_re = re.compile(r",\s*,+")
    value = comma_re.sub(", ", value)
    value = value.strip(", ")

    hyphen_re = re.compile(r"\s*-\s*")
    value = hyphen_re.sub("-", value)

    value = " ".join(value.split()).replace('"', "")

    return bool(value.strip())


def _is_valid_url_value(value):
    candidate = (value or "").strip().strip("'")
    parsed = urllib.parse.urlparse(candidate)
    return bool(parsed.scheme and parsed.netloc)


def _is_valid_hash_value(value):
    if ":" in value:
        _, digest = value.split(":", 1)
    else:
        digest = value
    return bool(re.fullmatch(r"[0-9a-fA-F]+", digest))


def _is_valid_pattern_value(value):
    try:
        re.compile(value)
        return True
    except re.error:
        return False


def _is_valid_latitude_value(value):
    try:
        numeric = float(value)
    except ValueError:
        return False
    return -90 <= numeric <= 90


def _is_valid_longitude_value(value):
    try:
        numeric = float(value)
    except ValueError:
        return False
    return -180 <= numeric <= 180


def _is_valid_multipolygon_value(value):
    candidate = (value or "").strip()
    if not candidate:
        return False

    try:
        geometry = shapely.wkt.loads(candidate)
    except shapely.errors.WKTReadingError:
        try:
            geojson = json.loads(candidate)
            geometry = shape(geojson)
        except Exception:
            return False

    if not isinstance(geometry, (Polygon, MultiPolygon, GeometryCollection)):
        return False

    # Shapely normal validity check where available.
    is_valid = getattr(geometry, "is_valid", True)
    return bool(is_valid)


def _is_valid_point_value(value):
    candidate = value
    if candidate is None:
        return False

    # Try WKT first.
    try:
        point = shapely.wkt.loads(candidate if isinstance(candidate, str) else str(candidate))
        if not isinstance(point, Point):
            return False
        return bool(getattr(point, "is_valid", True))
    except shapely.errors.WKTReadingError:
        pass
    except Exception:
        return False

    # Fallback to coordinate pair.
    try:
        if isinstance(candidate, (list, tuple)) and len(candidate) == 2:
            x_raw, y_raw = candidate[0], candidate[1]
        elif isinstance(candidate, str):
            text = candidate.strip()
            if not text:
                return False

            if text.startswith("["):
                coords = json.loads(text)
                if not isinstance(coords, list) or len(coords) != 2:
                    return False
                x_raw, y_raw = coords[0], coords[1]
            else:
                parts = [p.strip() for p in text.split(",")]
                if len(parts) != 2:
                    return False
                x_raw, y_raw = parts[0], parts[1]
        else:
            return False

        point = Point(float(x_raw), float(y_raw))
        return bool(getattr(point, "is_valid", True))
    except Exception:
        return False
