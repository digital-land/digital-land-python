"""
Module containing the dataclasses representing the different
expectation errors that can be recorded when running expectations
"""

from pydantic.dataclasses import dataclass
from typing import Any


def issue_factory(scope):
    """
    Factory to return the correct error dataclass based on scope
    """
    SCOPE_MAP = {"entity": EntityIssue, "entity-value": EntityValueIssue}
    if scope in SCOPE_MAP:
        return SCOPE_MAP[scope]
    else:
        raise TypeError(f"scope ({scope}) of expectation issue is nnot corrrect")


@dataclass
class BaseIssue:
    scope: str
    msg: str

    def keys(self):
        return self.__signature__.parameters.keys()

    def get(self, key, default):
        if key in self.__signature__.parameters.keys():
            return getattr(self, key)
        else:
            return default

    def to_dict(self):
        dict = {}
        for key in self.__signature__.parameters.keys():
            dict[key] = getattr(self, key)
        return dict


@dataclass
class EntityIssue(BaseIssue):
    dataset: str
    # might need to replace the below wiht something else
    organisation: str
    entity: int
    # let's give all values in the entity, it will be useful later
    entity_json: dict
    details: dict = None


@dataclass
class EntityValueIssue(BaseIssue):
    dataset: str
    # might need to replace the below wiht something else
    organisatiton: str
    entity: int
    # let's give all values in the entity, it will be useful later
    field: str
    value: Any
    entity_json: dict
    details: dict = None
