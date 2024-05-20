from digital_land.phase.phase import Phase


class PostConversionPhase(Phase):
    def __init__(
        self,
        issues,
    ):
        self.issues = issues
        self.duplicates = {}

    def process(self, stream):
        for block in stream:
            row = block.get("row", None)
            if not row:
                return

            reference = row.get("reference", None)
            line_number = block.get("line-number", None)

            if reference and line_number:
                self.validate_references(reference, line_number)
                self.check_for_duplicate_references(reference, line_number)
            yield block

        for ref, lines in self.duplicates.items():
            if len(lines) > 1:
                self.issues.log_issue(
                    "reference",
                    "duplicate-reference",
                    ref,
                    f"Duplicate reference '{ref}' found on lines: {', '.join(map(str, lines))}",
                )

    def validate_references(self, reference, line_number):
        if not reference:  # This will be True for both None and empty strings
            self.issues.log_issue(
                "reference",
                "missing-reference",
                "",
                "",
                line_number,
            )

    def check_for_duplicate_references(self, reference, line_number):
        if reference in self.duplicates:
            self.duplicates[reference].append(line_number)
        else:
            self.duplicates[reference] = [line_number]
