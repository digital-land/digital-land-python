import logging
from .phase import Phase
from digital_land.configuration.main import Config


class PriorityPhase(Phase):
    """
    Deduce priority of the entry when assembling facts
    """

    def __init__(self, config: Config = None):
        # if not connection:s
        # TODO causes errors when not supplied i think we should add this default in
        # a higher up level
        # connection = sqlite3.connect("var/cache/pipeline.sqlite3")
        self.default_priority = 2
        if config:
            self.config = config
        else:
            self.config = None
            logging.error(
                f"No config provided so priority defaults to {self.default_priority}"
            )

        # if connection:
        #     self.cursor = connection.cursor()

    # move to config class

    def priority(self, entity, organisation):
        # this implies online one type of priority is available
        # Do we want it to be more
        return (
            1
            if self.config.get_entity_organisation(entity) == organisation
            else self.default_priority
        )

    def process(self, stream):
        for block in stream:
            row = block["row"]
            if self.config:
                block["priority"] = self.priority(row["entity"], row["organisation"])
            else:
                block["priority"] = self.default_priority
            yield block

            # if no config is supplied then default priority should be set
            # if config supplied and no match then deafult priority should be used
            # if match then use priority
