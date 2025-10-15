from .address import AddressDataType
from .datatype import DataType
from .date import DateDataType
from .decimal import DecimalDataType
from .flag import FlagDataType
from .integer import IntegerDataType
from .multipolygon import MultiPolygonDataType
from .point import PointDataType
from .string import StringDataType
from .uri import URIDataType
from .latitude import LatitudeDataType
from .longitude import LongitudeDataType


def datatype_factory(datatype_name, **kwargs):
    typemap = {
        "integer": IntegerDataType,
        "decimal": DecimalDataType,
        "latitude": LatitudeDataType,
        "longitude": LongitudeDataType,
        "string": StringDataType,
        "address": AddressDataType,
        "text": StringDataType,  # TODO do we need dedicated type for Text?
        "datetime": DateDataType,
        "url": URIDataType,
        "flag": FlagDataType,
        "multipolygon": MultiPolygonDataType,
        "point": PointDataType,
        "curie": DataType,  # TODO create proper curie type
    }

    if datatype_name in typemap:
        return typemap[datatype_name](**kwargs)

    # TODO double check that the below isn't needed but OrganisationURI has a url datatype
    # so it would never be called.
    # if field name in ["OrganisationURI"]:
    #     return OrganisationURIDataType()

    raise ValueError("unknown datatype '%s'" % (datatype_name))
