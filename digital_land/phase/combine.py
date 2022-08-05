from copy import deepcopy

from shapely.geometry import MultiPolygon

from .phase import Phase
from shapely.ops import unary_union
from digital_land.datatype.wkt import dump_wkt
import shapely.wkt


def combine_geometries(wkts, precision=6):
    # https://shapely.readthedocs.io/en/stable/manual.html#shapely.ops.unary_union
    geometries = [shapely.wkt.loads(x)[0] for x in wkts]
    union = unary_union(geometries)
    if not isinstance(union, MultiPolygon):
        union = MultiPolygon([union])
    return dump_wkt(union, precision=precision)


class FactCombinePhase(Phase):
    """
    combine a field value from multiple facts
    """

    def __init__(self, issue_log=None, fields=[]):
        self.issues = issue_log
        self.fields = fields
        self.cache = {}  # this in-memory cache could get quite large

    def process(self, stream):
        for block in stream:
            row = block["row"]
            field = row["field"]

            if field in self.fields:
                e = self.cache.setdefault(row["entity"], {})
                e.setdefault(field, [])
                e[field].append(deepcopy(block))
                continue

            yield block

        for entity, e in self.cache.items():
            for field, blocks in e.items():
                # combine unique values for this entity
                values = (
                    block["row"]["value"] for block in blocks if block["row"]["value"]
                )
                values = sorted(set(values))

                if field == "geometry":
                    value = combine_geometries(values)
                else:
                    value = self.fields[field].join(values)

                # emit blocks with the combined value
                for block in blocks:
                    self.issues.line_number = block["line-number"]
                    self.issues.entry_number = block["entry-number"]
                    self.issues.log_issue(field, "combined-value", entity)

                    block["row"]["value"] = value
                    yield block
