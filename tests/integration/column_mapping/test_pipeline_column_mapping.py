from digital_land.pipeline import Pipeline
import pandas as pd


def get_columns_csv_data_one():
    return {
        "dataset": [
            "article-4-direction-area",
            "article-4-direction-area",
            "article-4-direction-area",
            "article-4-direction-area",
            "article-4-direction-area",
            "article-4-direction-area",
            "article-4-direction-area",
            "article-4-direction-area",
            "article-4-direction-area",
            "article-4-direction-area",
            "article-4-direction-area",
            "article-4-direction-area",
            "article-4-direction-area",
            "article-4-direction-area"
        ],
        "resource": [
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "5158d13bfc6f0723b1fb07c975701a906e83a1ead4aee598ee34e241c79a5f3d",
            "5158d13bfc6f0723b1fb07c975701a906e83a1ead4aee598ee34e241c79a5f3d",
            "5158d13bfc6f0723b1fb07c975701a906e83a1ead4aee598ee34e241c79a5f3d",
            "",
            ""
        ],
        "endpoint": [
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "d779ad1c91c5a46e2d4ace4d5446d7d7f81df1ed058f882121070574697a5412",
            "d779ad1c91c5a46e2d4ace4d5446d7d7f81df1ed058f882121070574697a5412"
        ],
        "column": [
            "NAME",
            "LOCATION_1",
            "WKT",
            "START_DATE",
            "CODE",
            "DESCRIPTIO",
            "COMMENT",
            "Designatio",
            "geography",
            "OBJECTID",
            "Article_4_Direction",
            "More_information",
            "test_fiel",
            "silly_fiel"
        ],
        "field": [
            "name",
            "name",
            "geometry",
            "start-date",
            "reference",
            "description",
            "notes",
            "notes",
            "reference",
            "reference",
            "description",
            "notes",
            "test_field",
            "silly_field"
        ]
    }


def test_all_resources_no_endpoints(tmpdir):
    columns_csv = tmpdir / "columns.csv"

    # dataset,resource,endpoint,column,field
    csv_data = get_columns_csv_data_one()
    columns_data = pd.DataFrame.from_dict(csv_data)
    columns_data.to_csv(columns_csv, index=False)


def test_all_endpoints_no_resources():
    pass


def test_resources_and_endpoints():
    pass


