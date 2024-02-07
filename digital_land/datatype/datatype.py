class DataType:
    SPECIAL_CASES_MAPPING = {
        "url": "URL",
        # Add more special cases as needed
    }

    def format(self, value):
        return value

    def normalise(self, value, issues=None):
        return value

    def split_and_capitalize(self, input):
        words = input.split("-")
        result_words = []

        for i, word in enumerate(words):
            if word.lower() in self.SPECIAL_CASES_MAPPING:
                result_words.append(self.SPECIAL_CASES_MAPPING[word.lower()])
            else:
                result_words.append(word.capitalize() if i == 0 else word)

        result_string = " ".join(result_words)
        return result_string
