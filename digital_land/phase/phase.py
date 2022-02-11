class Phase:
    """
    a step in a pipeline process
    """

    def process(self, stream):
        for block in stream:
            yield block
