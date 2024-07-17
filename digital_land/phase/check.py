from digital_land.phase.phase import Phase
import duckdb


class CheckPhase(Phase):
    def __init__(self, harmonised_path=None, issues=None, enabled=False):
        self.issues = issues
        self.harmonised_path = harmonised_path
        self.enabled = enabled

    def process(self, stream):
        print("path in check:", self.harmonised_path)

        # csv_path = self.harmonised_path
        csv_path = "test.csv"
        duckdb.read_csv(csv_path)
        sql = f"""
        SELECT "reference", "end-date", COUNT(*)
        FROM '{csv_path}'
        GROUP BY "reference", "end-date"
        HAVING COUNT(*) > 1
        """
        result = duckdb.sql(sql).df()
        if len(result) > 1:
            duplicate_references = result["references"].tolist()
            for reference in duplicate_references:
                self.issues.log(
                    "duplicate reference",
                    reference,
                    "There are multiple entries in the resource with the same reference",
                )

            print("duplicate references:", duplicate_references)

        yield from stream
