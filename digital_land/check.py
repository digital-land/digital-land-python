import duckdb
import logging
import dask.dataframe as dd


# TODO This might need to move into expectations as it is a form of data checking
def duplicate_reference_check(issues=None, csv_path=None):
    try:
        conn = duckdb.connect()

        ddf = dd.read_csv(csv_path, dtype={"entry-date": "string"})
        ddf.columns = ddf.columns.str.replace("-", "_")

        filtered_df = ddf[ddf["field"] == "reference"].compute()  # noqa
        conn.execute("CREATE TABLE filtered_table AS SELECT * FROM filtered_df")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_field_value_date ON filtered_table(field, value, entry_date);"
        )
        # SQL query to identify duplicate references
        sql = """
        SELECT
            "field",
            "value",
            "entry_date",
            COUNT(*) AS count,
            STRING_AGG("entry_number"::TEXT, ',') AS entry_numbers
        FROM filtered_table
        GROUP BY "field", "value", "entry_date"
        HAVING COUNT(*) > 1;
        """

        count_table = conn.execute(sql).fetchdf()

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
    finally:
        conn.execute("DROP TABLE IF EXISTS filtered_table;")
        conn.close()
    return issues
