"""
Contains data classes representing the result of an expectation being ran from expectation functions
"""

from datetime import datetime
import os

# TODO I think we can remove copy, often promotes bad behaviour
import copy

# TODO is warnings the right module for this? or should we use logging
import warnings

from enum import Enum
from pydantic.dataclasses import dataclass
from dataclasses_json import dataclass_json
from typing import Optional


class SeverityEnum(str, Enum):
    """
    Enumeration for severity csv in specification, may need to be replaced in the future
    with a different mechanism to read from csv
    """

    critical = "critical"
    error = "error"
    warning = "warning"
    notice = "notice"
    info = "info"
    debug = "debug"


# TODO write unit tests for this class
@dataclass_json
@dataclass
class ExpectationResponse:
    """Class to keep inputs and results of expectations"""

    run: str = None
    checkpoint: str = None
    result: bool = None
    severity: SeverityEnum = None
    msg: str = None
    errors: list = None
    data_name: str = None
    data_path: str = None
    name: str = None
    description: Optional[str] = None
    expectation: str = None
    entry_date: Optional[str] = None

    def __post_init__(self):
        "Adds a few more interesting items and adjusts response for log"

        """
        check_for_kwargs = self.expectation_input.get("kwargs", None)
        if check_for_kwargs:
            entry_date = check_for_kwargs.get("entry_date", None)
        else:
            entry_date = None

        if entry_date:
            self.entry_date = entry_date
        else:
        """
        self.entry_date = datetime.now().isoformat()

    def save_to_file(self, dir_path: str):
        "Prepares a naming convention and saves the response to a provided path"

        if self.result:
            name_status = "success"
        else:
            name_status = "fail"

        name_hash = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")[:-3]
        file_name = f"{self.entry_date}_{name_status}_{self.expectation_input['expectation_name']}_{name_hash}.json"

        with open(os.path.join(dir_path, file_name), "w") as f:
            self_save_version = copy.deepcopy(self)
            self_save_version.result = str(self_save_version.result)
            f.write(self_save_version.to_json())

    def act_on_failure(self):
        """
        Returns 1 if severity is critical or 0 if severity is not critical
        raises a warning for failed tests
        Could be moved to expection suite class
        """

        failure_count = 0

        if not self.result:
            warnings.warn(self.msg)
            if self.severity == "critical":
                failure_count = 1

        return failure_count
