class Phase:
    """
    a step in a pipeline process
    """

    def process(self, reader):
        for stream_data in reader:
            yield stream_data
