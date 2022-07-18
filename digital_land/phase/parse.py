from .phase import Phase


class ParsePhase(Phase):
    """
    convert stream lines of text into rows of fields
    """

    def process(self, stream):

        try:
            block = next(stream)
        except StopIteration:
            return None

        fieldnames = block["line"]
        entry_number = 0

        for block in stream:

            block["row"] = dict(zip(fieldnames, block["line"]))
            del block["line"]

            entry_number = entry_number + 1
            block["entry-number"] = entry_number

            yield block
