import pandas as pd

from digital_land.commands import pipeline_run
from digital_land.pipeline import Pipeline
from digital_land.specification import Specification


test_pipeline = "national-park"
test_resource = "5158d13bfc6f0723b1fb07c975701a906e83a1ead4aee598ee34e241c79a5f3d"
test_endpoint = "d779ad1c91c5a46e2d4ace4d5446d7d7f81df1ed058f882121070574697a5412"


def test_pipeline_run(test_dirs):
    """
    This test uses commands.py pipeline_run to perform a run-through
    of all the pipeline phases, using test data generated by the input
    fixture test_dirs, which itself uses the global vars:
    test_pipeline
    test_resource
    test_endpoint
    :param test_dirs:
    :return: None
    The function checks the transformed file at the end to verify that the fact
    for each field is generated.
    """
    # -- Arrange --
    pipeline_dir = test_dirs["pipeline_dir"]
    pipeline = Pipeline(pipeline_dir, test_pipeline)
    pipeline.lookup = {"": {",statistical-geography,,": "1"}}
    pipeline.redirect_lookup = {"1": {"entity": "10", "status": "301"}}

    specification_dir = test_dirs["specification_dir"]
    specification = Specification(specification_dir)

    input_path = test_dirs["collection_dir"] / f"{test_resource}.csv"
    output_path = test_dirs["transformed_dir"] / f"{test_resource}.csv"

    collection_dir = test_dirs["collection_dir"]
    issue_dir = test_dirs["issues_log_dir"]
    organisation_path = "tests/data/listed-building/organisation.csv"
    dataset_resource_dir = test_dirs["datasource_log_dir"]
    test_endpoints = [test_endpoint]

    # -- Act --
    pipeline_run(
        dataset=test_pipeline,
        pipeline=pipeline,
        specification=specification,
        input_path=input_path,
        output_path=output_path,
        collection_dir=collection_dir,  # TBD: remove, replaced by endpoints, organisations and entry_date
        null_path=None,  # TBD: remove this
        issue_dir=issue_dir,
        organisation_path=organisation_path,
        save_harmonised=False,
        column_field_dir=pipeline_dir,
        dataset_resource_dir=dataset_resource_dir,
        custom_temp_dir=None,  # TBD: rename to "tmpdir"
        endpoints=test_endpoints,
        organisations=[],
        entry_date="",
    )

    # -- Asert --
    df = pd.read_csv(output_path, index_col=False)

    assert "10" in df["entity"].astype(str).values
    assert "ep_field_one" in df["field"].values
    assert "ep-col-val" in df["value"].values
    assert "start-date" in df["field"].values
    assert "2020-01-12" in df["value"].values