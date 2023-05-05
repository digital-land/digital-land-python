import os
import pandas as pd
from digital_land.phase.map import MapPhase


def get_columns_csv_data_all():
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
            "dummy_fiel",
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
            "res_field_one",
            "res_field_two",
            "res_field_three",
            "ep_field_one",
            "ep_field_two",
            "ep_field_three"
        ]
    }


def get_columns_csv_data_res():
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
            "5158d13bfc6f0723b1fb07c975701a906e83a1ead4aee598ee34e241c79a5f3d"
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
            ""
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
            "More_information"
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
            "res_field_one",
            "res_field_two",
            "res_field_three"
        ]
    }


def get_columns_csv_data_eps():
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
            "",
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
            "d779ad1c91c5a46e2d4ace4d5446d7d7f81df1ed058f882121070574697a5412",
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
            "test_fiel",
            "dummy_fiel",
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
            "ep_field_one",
            "ep_field_two",
            "ep_field_three"
        ]
    }


def prepare_data(data_src: str = ""):

    data_selector = data_src.split("/")[-1].split(".")[0]

    if data_selector == "column_eps":
        csv_data = get_columns_csv_data_eps()
    elif data_selector == "column_res":
        csv_data = get_columns_csv_data_res()
    else:
        csv_data = get_columns_csv_data_all()

    columns_data = pd.DataFrame.from_dict(csv_data)
    columns_data.to_csv(data_src, index=False)


def process_pipeline(params: dict = {}):

    resource = params["resource"]
    pipeline = params["pipeline"]
    endpoints = params["endpoints"]
    specification = params["specification"]
    column_field_log = params["column_field_log"]
    output_dir = params["output_dir"]

    columns = pipeline.columns(resource, endpoints)
    fieldnames = specification.intermediate_fieldnames(pipeline)
    log = column_field_log

    mock_input_stream = [
        {
            "row": columns,
            "entry-number": 1,
        }
    ]

    map_phase = MapPhase(
        fieldnames=fieldnames,
        columns=columns,
        log=column_field_log,
    )

    output = [block for block in map_phase.process(mock_input_stream)]

    log.save(os.path.join(output_dir, resource + ".csv"))

    return output
