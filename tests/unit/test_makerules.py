from digital_land.makerules import pipeline_makerules


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

    pipeline_makerules(fake_collection)

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

    # mock old rresoures
    fake_collection.old_resource.entries = old_entities

    pipeline_makerules(fake_collection)

    printed_output = capsys.readouterr()
    assert "test1" in printed_output.out, "old resource is not in the output"
    assert "test3" in printed_output.out, "replacement resource is not in the output"
