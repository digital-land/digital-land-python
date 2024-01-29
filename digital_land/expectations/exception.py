class DataQualityException(Exception):
    """Exception raised for failed expectations with severity RaiseError.
    Attributes: response
    """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)
