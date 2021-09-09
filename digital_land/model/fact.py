from dataclasses import dataclass


@dataclass(frozen=True)
class Fact:
    entity: int
    slug: str
    attribute: str
    value: str
