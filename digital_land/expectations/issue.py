"""
Module containing the dataclasses representing the different
expectation errors that can be recorded when running expectations
"""

from pydantic.dataclasses import dataclass
from dataclasses_json import config, dataclass_json
from dataclasses import field


def issue_factory(scope):
    """
    Factory to return the correct error dataclass based on scope
    """
    SCOPE_MAP = {
        "dataset": DatasetIssue,
        "organisation": OrganisationIssue,
        "field": FieldIssue,
        "row-group": RowGroupIssue,
        "row": RowIssue,
        "value": ValueIssue,
    }
    if scope in SCOPE_MAP:
        return SCOPE_MAP[scope]
    else:
        raise TypeError(f"scope ({scope}) of expectation issue is not corrrect")


# TODO review below against pydantic classes to see if we're following best practice
@dataclass_json
@dataclass
class Issue:
    expectation_result: str = field(metadata=config(field_name="expectation-result"))
    scope: str
    message: str

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
class DatasetIssue(Issue):
    dataset: str

    def __post_init__(self):
        issue_scope = "dataset"
        if self.scope != issue_scope:
            raise ValueError(f"scope must be '{issue_scope}'.")


@dataclass
class OrganisationIssue(Issue):
    dataset: str
    organisation: str

    def __post_init__(self):
        issue_scope = "organisation"
        if self.scope != issue_scope:
            raise ValueError(f"scope must be '{issue_scope}'.")


@dataclass
class FieldIssue(Issue):
    scope: str
    dataset: str
    table_name: str = field(metadata=config(field_name="table-name"))
    field_name: str = field(metadata=config(field_name="field-name"))
    organisation: str

    def __post_init__(self):
        issue_scope = "field"
        if self.scope != issue_scope:
            raise ValueError(f"scope must be '{issue_scope}'.")


@dataclass
class RowGroupIssue(Issue):
    scope: str
    dataset: str
    table_name: str = field(metadata=config(field_name="table-name"))
    row_id: str = field(metadata=config(field_name="row-id"))
    rows: list
    organisation: str

    def __post_init__(self):
        issue_scope = "row-group"
        if self.scope != issue_scope:
            raise ValueError(f"scope must be '{issue_scope}'.")


@dataclass
class RowIssue(Issue):
    scope: str
    dataset: str
    table_name: str = field(metadata=config(field_name="table-name"))
    row_id: str = field(metadata=config(field_name="row-id"))
    row: dict
    organisation: str

    def __post_init__(self):
        issue_scope = "row"
        if self.scope != issue_scope:
            raise ValueError(f"scope must be '{issue_scope}'.")


@dataclass
class ValueIssue(Issue):
    scope: str
    dataset: str
    table_name: str = field(metadata=config(field_name="table-name"))
    field_name: str
    row_id: str
    value: str
    organisation: str

    def __post_init__(self):
        issue_scope = "value"
        if self.scope != issue_scope:
            raise ValueError(f"scope must be '{issue_scope}'.")
