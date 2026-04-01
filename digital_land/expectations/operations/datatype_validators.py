import json

import shapely.errors
import shapely.wkt
from shapely.geometry import GeometryCollection, MultiPolygon, Point, Polygon, shape


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
        point = shapely.wkt.loads(
            candidate if isinstance(candidate, str) else str(candidate)
        )
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
