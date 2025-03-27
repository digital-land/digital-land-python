from digital_land.makerules import (
    ProcessingOption,
    get_processing_option,
    pipeline_makerules,
)
from digital_land.state import State
from digital_land.collection import Collection
from types import MethodType


def test_makerules_removes_old_entities_410_removed(mocker, capsys):
    """
    if redirects are provided and the status is 410 then they shouldn't
    be in the makerules output as they don't need further processing
    """
    # Create a fake class
    fake_collection = mocker.Mock()

    dataset_resourcce_map = {"teast_dataset": ["test1", "test2"]}
    # Mock required methods
    fake_collection.dataset_resource_map = mocker.Mock(
        return_value=dataset_resourcce_map
    )
    fake_collection.resource_endpoints = mocker.Mock(return_value=["endpoint"])
    fake_collection.resource_organisations = mocker.Mock(return_value=["org"])
    old_entities = [{"old-resource": "test1", "status": "410", "resource": ""}]
    # Mock old resources
    fake_collection.old_resource.entries = old_entities

    specification_dir = "specification/"
    pipeline_dir = "pipeline/"
    resource_dir = "resource/"
    incremental_loading_override = False

    pipeline_makerules(
        fake_collection,
        specification_dir,
        pipeline_dir,
        resource_dir,
        incremental_loading_override,
    )

    printed_output = capsys.readouterr()
    assert "test1" not in printed_output.out


def test_makerules_removes_old_entities_310_both_resources_referenced(mocker, capsys):
    """
    if redirects are provided and the status is 410 then they shouldn't
    be in the makerules output whatso ever
    """
    # Create a fake class
    fake_collection = mocker.Mock()

    dataset_resourcce_map = {"test_dataset": ["test1", "test2"]}
    # Mock required methods of collection
    fake_collection.dataset_resource_map = mocker.Mock(
        return_value=dataset_resourcce_map
    )
    fake_collection.resource_endpoints = mocker.Mock(return_value=["endpoint"])
    fake_collection.resource_organisations = mocker.Mock(return_value=["org"])
    fake_collection.resource_path = mocker.Mock(side_effect=lambda x: x)
    old_entities = [{"old-resource": "test1", "status": "301", "resource": "test3"}]

    # mock old resoures
    fake_collection.old_resource.entries = old_entities

    specification_dir = "specification/"
    pipeline_dir = "pipeline/"
    resource_dir = "resource/"
    incremental_loading_override = False

    pipeline_makerules(
        fake_collection,
        specification_dir,
        pipeline_dir,
        resource_dir,
        incremental_loading_override,
    )

    printed_output = capsys.readouterr()
    assert "test1" in printed_output.out, "old resource is not in the output"
    assert "test3" in printed_output.out, "replacement resource is not in the output"


# def test_collection_pipeline_makerules()


def test_get_processing_option_no_state_change(mocker):
    mocker.patch("digital_land.makerules.compare_state", return_value=None)

    fake_collection = mocker.Mock()

    option = get_processing_option(fake_collection, "", "", "", False, "state.json")

    assert option == ProcessingOption.PROCESS_NONE


def test_get_processing_option_code_change(mocker):
    mocker.patch("digital_land.makerules.compare_state", return_value=["code"])

    fake_collection = mocker.Mock()

    option = get_processing_option(fake_collection, "", "", "", False, "state.json")

    assert option == ProcessingOption.PROCESS_ALL


def test_get_processing_option_specification_change(mocker):
    mocker.patch("digital_land.makerules.compare_state", return_value=["specification"])

    fake_collection = mocker.Mock()

    option = get_processing_option(fake_collection, "", "", "", False, "state.json")

    assert option == ProcessingOption.PROCESS_ALL


def test_get_processing_option_pipeline_change(mocker):
    mocker.patch("digital_land.makerules.compare_state", return_value=["pipeline"])

    fake_collection = mocker.Mock()

    option = get_processing_option(fake_collection, "", "", "", False, "state.json")

    assert option == ProcessingOption.PROCESS_ALL


def test_get_processing_option_collection_change(mocker):
    mocker.patch("digital_land.makerules.compare_state", return_value=["collection"])

    fake_collection = mocker.Mock()

    option = get_processing_option(fake_collection, "", "", "", False, "state.json")

    assert option == ProcessingOption.PROCESS_ALL


def test_get_processing_option_resource_change(mocker):
    mocker.patch("digital_land.makerules.compare_state", return_value=["resource"])

    fake_collection = mocker.Mock()

    option = get_processing_option(fake_collection, "", "", "", False, "state.json")

    assert option == ProcessingOption.PROCESS_PARTIAL


def test_get_processing_option_unknown_change(mocker):
    mocker.patch("digital_land.makerules.compare_state", return_value=["unknown"])

    fake_collection = mocker.Mock()

    option = get_processing_option(fake_collection, "", "", "", False, "state.json")

    assert option == ProcessingOption.PROCESS_ALL


def test_get_processing_option_no_state(mocker):
    mocker.patch("digital_land.makerules.compare_state", return_value=["unknown"])

    fake_collection = mocker.Mock()

    option = get_processing_option(fake_collection, "", "", "", False, None)

    assert option == ProcessingOption.PROCESS_ALL


def test_pipeline_makerules_process_none(mocker, capsys):
    mocker.patch(
        "digital_land.makerules.get_processing_option",
        return_value=ProcessingOption.PROCESS_NONE,
    )

    # Create a fake class
    fake_collection = mocker.Mock()

    dataset_resource_map = {"test_dataset": []}
    # Mock required methods
    fake_collection.dataset_resource_map = mocker.Mock(
        return_value=dataset_resource_map
    )
    fake_collection.resource_endpoints = mocker.Mock(return_value=["endpoint"])
    fake_collection.resource_organisations = mocker.Mock(return_value=["org"])
    fake_collection.old_resource.entries = []

    specification_dir = "specification/"
    pipeline_dir = "pipeline/"
    resource_dir = "resource/"
    incremental_loading_override = False

    pipeline_makerules(
        fake_collection,
        specification_dir,
        pipeline_dir,
        resource_dir,
        incremental_loading_override,
    )

    printed_output = capsys.readouterr()

    # assert "transformed:: $(TEST_DATASET_TRANSFORMED_FILES)" not in printed_output.out
    assert (
        'transformed::\n\techo "No state change and no new resources to transform"'
        in printed_output.out
    )
    # assert "dataset:: $(TEST_DATASET_DATASET)" not in printed_output.out
    assert (
        'dataset::\n\techo "No state change so no resources have been transformed"'
        in printed_output.out
    )


def test_pipeline_makerules_process_all(mocker, capsys):
    mocker.patch(
        "digital_land.makerules.get_processing_option",
        return_value=ProcessingOption.PROCESS_ALL,
    )

    # Create a fake class
    fake_collection = mocker.Mock()

    dataset_resource_map = {"test_dataset": ["test1", "test2"]}
    # Mock required methods
    fake_collection.dataset_resource_map = mocker.Mock(
        return_value=dataset_resource_map
    )
    fake_collection.resource_endpoints = mocker.Mock(return_value=["endpoint"])
    fake_collection.resource_organisations = mocker.Mock(return_value=["org"])
    fake_collection.old_resource.entries = []

    specification_dir = "specification/"
    pipeline_dir = "pipeline/"
    resource_dir = "resource/"
    incremental_loading_override = False

    pipeline_makerules(
        fake_collection,
        specification_dir,
        pipeline_dir,
        resource_dir,
        incremental_loading_override,
    )

    printed_output = capsys.readouterr()

    assert "test1" in printed_output.out
    assert "test2" in printed_output.out
    assert "transformed:: $(TEST_DATASET_TRANSFORMED_FILES)" in printed_output.out
    assert "dataset:: $(TEST_DATASET_DATASET)" in printed_output.out


def test_pipeline_makerules_process_partial(mocker, capsys):
    mocker.patch(
        "digital_land.makerules.get_processing_option",
        return_value=ProcessingOption.PROCESS_PARTIAL,
    )

    # Initial test with nothing coming through
    # Create the mock with the spec set to 'Collection' and allow extra attributes
    fake_collection = mocker.Mock()
    fake_collection.resource = mocker.Mock()
    fake_collection.resource.entries = [
        {
            "resource": "test1",
            "endpoints": "endpoint1",
            "start-date": "2025-03-17",
            "end-date": "",
        },
        {
            "resource": "test2",
            "endpoints": "endpoint2",
            "start-date": "2025-03-17",
            "end-date": "",
        },
    ]
    fake_collection.resource_endpoints = mocker.Mock(
        side_effect=lambda res: [res + "_endpoint"]
    )
    fake_collection.resource_organisations = mocker.Mock(
        side_effect=lambda res: [res + "_org"]
    )
    fake_collection.old_resource = mocker.Mock()
    fake_collection.old_resource.entries = []
    fake_collection.source = mocker.Mock()
    fake_collection.source.entries = [
        {"endpoint": "endpoint1", "end-date": "", "datasets": "MOCK"},
        {"endpoint": "endpoint2", "end-date": "", "datasets": "MOCK"},
    ]

    # Mock State.load to return the last_updated_date
    mocker.patch.object(State, "load", return_value={"last_updated_date": "2025-03-18"})

    fake_collection.dataset_resource_map = MethodType(
        Collection.dataset_resource_map, fake_collection
    )

    # Make sure the dataset_resource_map method is available on the mocked collection
    state_path = "mock_state.json"
    specification_dir = "specification/"
    pipeline_dir = "pipeline/"
    resource_dir = "resource/"
    incremental_loading_override = False

    pipeline_makerules(
        fake_collection,
        specification_dir,
        pipeline_dir,
        resource_dir,
        incremental_loading_override,
        state_path=state_path,
    )

    printed_output = capsys.readouterr()

    # # PROCESS_FULL is hard coded in. When we get incremental loading to work remove following fours lines
    # # and replace with the tests commented out
    # assert "test1" in printed_output.out
    # assert "test2" in printed_output.out
    # assert "transformed:: $(MOCK_TRANSFORMED_FILES)" in printed_output.out
    # assert "dataset:: $(MOCK_DATASET)" in printed_output.out
    assert (
        'transformed::\n\techo "No state change and no new resources to transform"'
        in printed_output.out
    )
    assert (
        'dataset::\n\techo "No state change so no resources have been transformed"'
        in printed_output.out
    )

    # Change test with one passing
    fake_collection.resource.entries = [
        {
            "resource": "test1",
            "endpoints": "endpoint1",
            "start-date": "2025-03-17",
            "end-date": "",
        },
        {
            "resource": "test2",
            "endpoints": "endpoint2",
            "start-date": "2025-03-20",
            "end-date": "",
        },
    ]
    pipeline_makerules(
        fake_collection,
        specification_dir,
        pipeline_dir,
        resource_dir,
        incremental_loading_override,
        state_path=state_path,
    )
    printed_output = capsys.readouterr()

    # PROCESS_FULL is hard coded in. When we get incremental loading to work replace following line with line after
    # assert "test1" in printed_output.out
    assert "test1" not in printed_output.out
    assert "test2" in printed_output.out
    assert "transformed:: $(MOCK_TRANSFORMED_FILES)" in printed_output.out
    assert "dataset:: $(MOCK_DATASET)" in printed_output.out
