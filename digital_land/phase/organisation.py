from .phase import Phase


class OrganisationPhase(Phase):
    """
    lookup the organisation
    """

    def __init__(self, organisation={}):
        self.organisation = organisation

    def process(self, stream):
        for block in stream:
            row = block["row"]
            row["organisation"] = self.organisation.lookup(row["organisation"])
            yield block
