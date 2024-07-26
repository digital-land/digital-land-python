import duckdb
import logging
import pandas as pd


# TODO This might need to move into expectations as it is a form of data checking
def duplicate_reference_check(issues=None, csv_path=None):
    try:
        conn = duckdb.connect()

        conn.execute(
            """
        CREATE TABLE IF NOT EXISTS temp_table (
            "field" VARCHAR,
            "value" VARCHAR,
            "entry_date" VARCHAR,
            "entry_number" VARCHAR
        );
        """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_field_value_date ON temp_table(field, value, entry_date);"
        )

        # Read the entire CSV file into a DataFrame
        df = pd.read_csv(csv_path)

        # Rename columns to match SQL naming conventions
        df.columns = df.columns.str.replace("-", "_")
        df = df.rename(
            columns={
                "field": "field",
                "value": "value",
                "entry_date": "entry_date",
                "entry_number": "entry_number",
            }
        )

        # Filter the DataFrame to include only rows where "field" is 'reference'
        filtered_df = df[df["field"] == "reference"]

        # Register the DataFrame with DuckDB
        conn.register("filtered_df", filtered_df)

        # Insert the filtered data into the DuckDB table
        conn.execute(
            """
            INSERT INTO temp_table ("field", "value", "entry_date", "entry_number")
            SELECT "field", "value", "entry_date", "entry_number"
            FROM filtered_df;
        """
        )

        # SQL query to identify duplicate references
        sql = """
        SELECT
            "field",
            "value",
            "entry_date",
            COUNT(*) AS count,
            STRING_AGG("entry_number"::TEXT, ',') AS entry_numbers
        FROM temp_table
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
        conn.execute("DROP TABLE IF EXISTS temp_table;")
        conn.close()
    return issues
