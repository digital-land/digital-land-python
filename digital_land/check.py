import duckdb
import logging


# TODO This might need to move into expectations as it is a form of data checking
def duplicate_reference_check(issues=None, csv_path=None):
    try:
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
                        "reference values are not unique",
                        row["value"],
                        entry_number=int(entry_number),
                        line_number=int(entry_number)
                        + 1,  # TODO Check this makes sense in all cases
                        message="Reference must be unique in resource",
                    )
    except Exception as e:
        logging.error("Duplicate reference check: Failed for %s" % (csv_path))
        logging.error(e)
    return issues
