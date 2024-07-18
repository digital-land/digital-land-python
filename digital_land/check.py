import duckdb


def duplicate_reference_check(issues=None, csv_path=None):
    # csv_path = "test.csv"
    duckdb.read_csv(csv_path)
    sql = f"""
    SELECT *
    FROM '{csv_path}'
    """
    result = duckdb.sql(sql).df()
    print(result.head(5))
    return issues
    # if len(result) > 1:
    #     duplicate_references = result["references"].tolist()
    #     for reference in duplicate_references:
    #         self.issues.log(
    #             "duplicate reference",
    #             reference,
    #             "There are multiple entries in the resource with the same reference",
    #         )

    # print("duplicate references:", duplicate_references)
