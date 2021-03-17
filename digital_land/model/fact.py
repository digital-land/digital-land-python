from dataclasses import dataclass


@dataclass(frozen=True)
class Fact:
    entity: str
    attribute: str
    value: str
