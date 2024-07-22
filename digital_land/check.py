import duckdb
import logging


def duplicate_reference_check(issues=None, csv_path=None):
    try:
        duckdb.read_csv(csv_path)
        sql = f"""
        SELECT
            "field",
            "value",
            "entry-date",
            COUNT(*) AS count,
            STRING_AGG("entry-number", ',') AS entry_numbers
        FROM read_csv('{csv_path}')
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
                    row["value"],
                    entry_number=int(entry_number),
                    line_number=None,  # Can't get line number as we are outside pipeline
                    message="Reference must be unique in resource",
                )
    except Exception as e:
        logging.warning(
            "Duplicate reference check for csv at path: %s has failed" % (csv_path)
        )
        logging.warning(e)
        # What do we do in here?
    return issues
