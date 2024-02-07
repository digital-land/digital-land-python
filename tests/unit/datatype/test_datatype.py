import pytest
from digital_land.datatype.datatype import DataType


class TestDataType:
    @pytest.fixture
    def data_type_instance(self):
        return DataType()

    def test_split_and_capitalize(self, data_type_instance):
        # Test case 1: Normal case
        input_str_1 = "start-date"
        expected_output_1 = "Start date"
        assert data_type_instance.split_and_capitalize(input_str_1) == expected_output_1

        # Test case 2: Special case
        input_str_2 = "url-value"
        expected_output_2 = "URL value"
        assert data_type_instance.split_and_capitalize(input_str_2) == expected_output_2

        # Test case 3: Special case
        input_str_2 = "value-Url"
        expected_output_2 = "Value URL"
        assert data_type_instance.split_and_capitalize(input_str_2) == expected_output_2

        # Test case 4: All lowercase
        input_str_4 = "organisation"
        expected_output_4 = "Organisation"
        assert data_type_instance.split_and_capitalize(input_str_4) == expected_output_4

        # Test case 5: Empty input
        input_str_5 = ""
        expected_output_5 = ""
        assert data_type_instance.split_and_capitalize(input_str_5) == expected_output_5
