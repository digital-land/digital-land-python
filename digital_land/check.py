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
        if len(count_table) >= 1:
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
        else:
            logging.warning(
                "Duplicate reference check: No references found for %s" % (csv_path)
            )
    except Exception as e:
        logging.warning("Duplicate reference check: Failed for %s" % (csv_path))
        logging.warning(e)
        # What do we do in here?
    return issues
