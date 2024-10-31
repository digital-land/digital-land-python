from digital_land.organisation import Organisation


class TestOrganisation:
    def test_get_orgs_by_dataset(self, mocker):
        # mock the init so nothing is loaded
        mocker.patch(
            "digital_land.organisation.Organisation.__init__", return_value=None
        )
        # create class
        organisations = Organisation()

        # set up mocked org data, can be changed if the data underneath changes
        org_data = {
            "local-authority:test": {
                "entity": 101,
                "name": "test",
                "prefix": "local-authority",
                "reference": "test",
                "dataset": "local-authority",
            }
        }

        organisations.organisation = org_data
        dataset = "local-authority"
        dataset_orgs = organisations.get_orgs_by_dataset(dataset)
        for dataset_org in dataset_orgs:
            # tests the dataset is correct for all and that properties can be access
            # as a dict
            assert dataset_org["dataset"] == dataset
