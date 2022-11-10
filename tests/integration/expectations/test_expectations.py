import pytest
import spatialite
import os
import pandas as pd

from digital_land.expectations.core import QueryRunner
from digital_land.expectations.expectations import expect_filtered_entities_to_be_as_predicted

@pytest.fixture
def sqlite3_with_entity_table_path(tmp_path):
    dataset_path = os.path.join(tmp_path,'test.sqlite3')

    create_table_sql = """
        CREATE TABLE entity (
            dataset TEXT,
            end_date TEXT,
            entity INTEGER PRIMARY KEY,
            entry_date TEXT,
            geojson JSON,
            geometry TEXT,
            json JSON,
            name TEXT,
            organisation_entity TEXT,
            point TEXT,
            prefix TEXT,
            reference TEXT,
            start_date TEXT,
            typology TEXT
        );
    """
    with spatialite.connect(dataset_path) as con:
        con.execute(create_table_sql)

    return dataset_path

def test_expect_filtered_entities_to_be_as_predicted_runs_for_correct_input(sqlite3_with_entity_table_path):
    
    # load data
    test_data = pd.DataFrame.from_dict({'entity':[1,2],'name':["test1","test2"],'reference':['1','2']})
    with spatialite.connect(sqlite3_with_entity_table_path) as con:
        test_data.to_sql('entity',con,if_exists='append',index=False)

    # build inputs
    query_runner= QueryRunner(sqlite3_with_entity_table_path)
    expected_result = [{'name':'test1'}]
    returned_entity_fields=['name']
    filters = {'reference':'1'}

    # run expectation
    expectation_response = expect_filtered_entities_to_be_as_predicted(
        query_runner= query_runner,
        expected_result= expected_result,
        returned_entity_fields=returned_entity_fields,
        filters=filters
    )
    
    assert expectation_response.result == True,f'Expectation Details: {expectation_response.details}'

def test_expect_filtered_entities_to_be_as_predicted_fails(sqlite3_with_entity_table_path):
    
    # load data
    test_data = pd.DataFrame.from_dict({'entity':[1,2],'name':["test1","test2"],'reference':['1','2']})
    with spatialite.connect(sqlite3_with_entity_table_path) as con:
        test_data.to_sql('entity',con,if_exists='append',index=False)

    # build inputs
    query_runner= QueryRunner(sqlite3_with_entity_table_path)
    expected_result = [{'name':'incorrect value'}]
    returned_entity_fields=['name']
    filters = {'reference':'1'}

    # run expectation
    expectation_response = expect_filtered_entities_to_be_as_predicted(
        query_runner= query_runner,
        expected_result= expected_result,
        returned_entity_fields=returned_entity_fields,
        filters=filters
    )
    
    assert expectation_response.result == False,f'Expectation Details: {expectation_response.details}'