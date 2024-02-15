"""
Module containing the dataclasses representing the different
expectation errors that can be recorded when running expectations
"""

from pydantic.dataclasses import dataclass


def error_factory(scope):
    """
    Factory to return the correct error dataclass
    """


@dataclass
class BaseError:
    message: str = ""

    def to_dict(self):
        dict = {}
        for key in self.__signature__.parameters.keys():
            dict[key] = getattr(self, key)
        return dict


class ResourceError(BaseError):
    pass


class ColumnError(BaseError):
    pass


class EntryError(BaseError):
    pass


class EntryValueError(BaseError):
    pass


@dataclass
class DatasetError(BaseError):
    entity: int = None


class OrganisationError(BaseError):
    pass


class FieldError(BaseError):
    pass


class EntityValueError(BaseError):
    pass
