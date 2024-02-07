class DataType:
    def format(self, value):
        return value

    def normalise(self, value, issues=None):
        return value

    def split_and_capitalize(self, input):
        words = input.split("-")
        capitalized_words = [word.capitalize() for word in words]
        result_string = " ".join(capitalized_words)
        return result_string
