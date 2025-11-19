import logging
from .phase import Phase
from digital_land.configuration.main import Config


class PriorityPhase(Phase):
    """
    Deduce priority of the entry when assembling facts
    """

    def __init__(self, config: Config = None, providers=[]):
        self.providers = providers
        self.default_priority = 1
        if config:
            self.config = config
        else:
            self.config = None
            logging.warning(
                f"No config provided so priority defaults to {self.default_priority}"
            )

    def priority(self, entity, organisation):
        return (
            2
            if self.config.get_entity_organisation(entity) == organisation
            else self.default_priority
        )

    def process(self, stream):
        for block in stream:
            row = block["row"]
            if self.config:
                authoritative_organisation = self.config.get_entity_organisation(
                    row["entity"]
                )
                if authoritative_organisation is not None:
                    if authoritative_organisation in self.providers:
                        block["priority"] = 2
                    else:
                        block["priority"] = self.default_priority
                        row["organisation"] = authoritative_organisation
                else:
                    block["priority"] = self.default_priority

            else:
                block["priority"] = self.default_priority
            yield block
