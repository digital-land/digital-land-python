"""
Module containing the dataclasses representing the different
expectation errors that can be recorded when running expectations
"""


def error_factory(scope):
    """
    Factory to return the correct error dataclass
    """


class BaseError:
    pass


class ResourceError(BaseError):
    pass


class ColumnError(BaseError):
    pass


class EntryError(BaseError):
    pass


class EntryValueError(BaseError):
    pass


class DatasetError(BaseError):
    pass


class OrganisationError(BaseError):
    pass


class FieldError(BaseError):
    pass


class EntityValueError(BaseError):
    pass
