import pytest
import os
import spatialite
import yaml
import pandas as pd
from csv import DictReader
from digital_land.expectations.main import run_expectation_suite
from digital_land.expectations.core import DataQualityException


def test_run_expectation_suite_raises_warning(tmp_path):
    with pytest.warns(UserWarning):
        results_file_path = os.path.join(tmp_path, "results.csv")
        data_path = os.path.join(tmp_path, "data.sqlite3")
        expectation_suite_yaml = os.path.join(tmp_path, "suite.yaml")
        run_expectation_suite(results_file_path, data_path, expectation_suite_yaml)

@pytest.fixture
def sqlite3_with_entity_table_path(tmp_path):
    dataset_path = os.path.join(tmp_path, "test.sqlite3")

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

def test_run_expectation_suite_success(tmp_path,sqlite3_with_entity_table_path):
    # load data
    multipolygon = (
        "MULTIPOLYGON(((-0.4610469722185172 52.947516855690964,"
        "-0.4614606467578964 52.94650314047493,"
        "-0.4598136600343151 52.94695770522492,"
        "-0.4610469722185172 52.947516855690964)))"
    )
    test_data = pd.DataFrame.from_dict(
        {"entity": [1], "name": ["test1"], "geometry": [multipolygon]}
    )
    with spatialite.connect(sqlite3_with_entity_table_path) as con:
        test_data.to_sql("entity", con, if_exists="append", index=False)
    
    # create yaml
    filters = {"geometry": "POINT(-0.460759538145794 52.94701402037683)"}
    yaml_dict = {'expectations':[{'severity':'critical','name':'count entities','expectation':'count_entities','filters':filters,'expected_result':1}]}
    with open(os.path.join(tmp_path,'suite.yaml'), 'w') as yaml_file: 
        yaml.dump(yaml_dict,yaml_file)

    # build inputs
    results_file_path = os.path.join(tmp_path, "results.csv")
    data_path = sqlite3_with_entity_table_path
    expectation_suite_yaml = os.path.join(tmp_path, "suite.yaml")
    run_expectation_suite(results_file_path, data_path, expectation_suite_yaml)

    # get results
    actual = []
    with open(os.path.join(tmp_path,'results.csv')) as file:
        reader = DictReader(file)
        for row in reader:
            actual.append(row)

    # check results true
    assert actual[0]['result']


def test_run_expectation_suite_fails_with_critical_failure(tmp_path,sqlite3_with_entity_table_path):
    # load data
    multipolygon = (
        "MULTIPOLYGON(((-0.4610469722185172 52.947516855690964,"
        "-0.4614606467578964 52.94650314047493,"
        "-0.4598136600343151 52.94695770522492,"
        "-0.4610469722185172 52.947516855690964)))"
    )
    test_data = pd.DataFrame.from_dict(
        {"entity": [1], "name": ["test1"], "geometry": [multipolygon]}
    )
    with spatialite.connect(sqlite3_with_entity_table_path) as con:
        test_data.to_sql("entity", con, if_exists="append", index=False)
    
    # create yaml
    filters = {"geometry": "POINT(-0.460759538145794 52.94701402037683)"}
    yaml_dict = {'expectations':[{'severity':'critical','name':'count entities','expectation':'count_entities','filters':filters,'expected_result':2}]}
    with open(os.path.join(tmp_path,'suite.yaml'), 'w') as yaml_file: 
        yaml.dump(yaml_dict,yaml_file)

    # build inputs
    results_file_path = os.path.join(tmp_path, "results.csv")
    data_path = sqlite3_with_entity_table_path
    expectation_suite_yaml = os.path.join(tmp_path, "suite.yaml")

    try:
        with pytest.warns(UserWarning):
            run_expectation_suite(results_file_path, data_path, expectation_suite_yaml)
        assert False
    except DataQualityException:
        assert True

