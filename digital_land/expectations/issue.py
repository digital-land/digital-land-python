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
    issue_class = getattr(SCOPE_MAP, scope, "")
    if issue_class:
        return issue_class
    else:
        raise TypeError("scope of expectation issue is nnot corrrect")


@dataclass
class BaseError:
    scope: str
    message: str

    def to_dict(self):
        dict = {}
        for key in self.__signature__.parameters.keys():
            dict[key] = getattr(self, key)
        return dict


@dataclass
class EntityIssue(BaseError):
    dataset: str
    # might need to replace the below wiht something else
    organisatiton: str
    entity: int
    # let's give all values in the entity, it will be useful later
    entity_json: dict
    details: dict = None


@dataclass
class EntityValueIssue(BaseError):
    dataset: str
    # might need to replace the below wiht something else
    organisatiton: str
    entity: int
    # let's give all values in the entity, it will be useful later
    field: str
    value: Any
    entity_json: dict
    details: dict = None
