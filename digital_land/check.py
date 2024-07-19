import duckdb


def duplicate_reference_check(issues=None, csv_path=None):
    # csv_path = "test.csv"
    try:
        duckdb.read_csv(csv_path)
        sql = f"""
        SELECT
            "field",
            "value",
            "entry-date",
            COUNT(*) AS count,
            STRING_AGG("entry-number", ',') AS entry_numbers
        FROM read_csv_auto('{csv_path}')
        WHERE "field" IN ('reference')
        GROUP BY "field", "value", "entry-date";
        """
        count_table = duckdb.sql(sql).df()
        duplicate_references = count_table[count_table["count"] > 1]
        for idx, row in duplicate_references.iterrows():
            for entry_number in row["entry_numbers"].split(","):
                issues.log_issue(
                    "reference",
                    "duplicate reference",
                    row["reference"],
                    entry_number=entry_number,
                    message="Reference must be unique in resource",
                )
                print(
                    "issue for reference",
                    row["reference"],
                    "for entry number",
                    entry_number,
                )
    except Exception as e:
        print(e)
    return issues
