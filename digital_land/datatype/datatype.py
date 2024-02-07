class DataType:
    def format(self, value):
        return value

    def normalise(self, value, issues=None):
        return value

    def split_and_capitalize(self, input):
        words = input.split("-")
        result_string = words[0].capitalize()
        if len(words) > 1:
            result_string += " " + " ".join(words[1:])
        return result_string
