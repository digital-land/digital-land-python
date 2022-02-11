from .phase import Phase


class ParsePhase(Phase):
    """
    convert stream lines of text into rows of fields
    """

    def process(self, stream):

        block = next(stream)
        fieldnames = block["line"]

        for block in stream:
            block["row"] = dict(zip(fieldnames, block["line"]))
            del block["line"]
            yield block
