import pandas as pd


def duplicate_reference_check(issues=None, csv_path=None):
    # csv_path = "test.csv"
    df = pd.read_csv(csv_path)
    if len(df) > 0:
        try:
            count_table = pd.pivot_table(
                df[df["field"].isin(["reference", "entry-date"])],
                values="value",
                index="entry-number",
                columns="field",
                aggfunc="first",
            ).reset_index()
            count_table["entry-number"] = count_table["entry-number"].astype(str)
            count_table = (
                count_table.groupby(["reference", "entry-date"])
                .agg(
                    count=("reference", "count"),
                    entry_numbers=("entry-number", ",".join),
                )
                .reset_index()
            )
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
                    # print(count_table.head(10))
        except Exception as e:
            print(e)
    return issues
