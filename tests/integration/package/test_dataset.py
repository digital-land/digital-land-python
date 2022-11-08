import pytest
import csv
import os
import urllib.request 
import logging
import pandas as pd
from pathlib import Path


from digital_land.package.dataset import DatasetPackage
from digital_land.organisation import Organisation

@pytest.fixture
def transformed_fact_resources():
    # csv_path = os.path.join(tmp_path,'transformed.csv')
    # csv header
    fieldnames = ['entity','entry-date','fact','field','value']

# csv data
    input_data = [
        {'entity': '44006677',
        'entry-date': '2021-09-06',
        'fact': '1f90248fd06e49accd42b80e43d58beeac300f942f1a9f71da4b64865356b1f3',
        'field': 'name',
        'value': 'Burghwallis'},
        {'entity': '44006677',
        'entry-date': '2022-11-02',
        'fact': '1f90248fd06e49accd42b80e43d58beeac300f942f1a9f71da4b64865356b1f3',
        'field': 'name',
        'value': 'Burghwallis'},
    ]

    # with open(csv_path, 'w', encoding='UTF8', newline='') as f:
    #     writer = csv.DictWriter(f, fieldnames=fieldnames)
    #     writer.writeheader()
    #     writer.writerows(rows)
    
    return  input_data


@pytest.fixture(scope="session")
def specification_dir(tmp_path_factory):
    specification_dir = tmp_path_factory.mktemp('specification')
    source_url='https://raw.githubusercontent.com/digital-land/'
    specification_csvs =[
        'attribution.csv',
	    'licence.csv',
	    'typology.csv',
	    'theme.csv',
	    'collection.csv',
	    'dataset.csv',
	    'dataset-field.csv',
	    'field.csv',
	    'datatype.csv',
	    'prefix.csv',
	    # deprecated ..
	    'pipeline.csv',
	    'dataset-schema.csv',
	    'schema.csv',
	    'schema-field.csv'
    ]
    for csv in specification_csvs:
        urllib.request.urlretrieve(f'{source_url}/specification/main/specification/{csv}', os.path.join(specification_dir,csv))

    return specification_dir

@pytest.fixture
def organisation_csv(tmp_path):
    organisation_path = os.path.join(tmp_path,'organisation.csv')
    urllib.request.urlretrieve('https://raw.githubusercontent.com/digital-land/organisation-dataset/main/collection/organisation.csv',organisation_path)
    return organisation_path

@pytest.fixture
def blank_patch_csv(tmp_path):
    patch_path = os.path.join(tmp_path,'organisation.csv')
    fieldnames=['dataset','resource','field','pattern','value']
    with open(patch_path, 'w', encoding='UTF8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
    return patch_path

def test_entry_date_upsert_uploads_newest_date(specification_dir,organisation_csv,blank_patch_csv,transformed_fact_resources,tmp_path):
    dataset='conservation-area'
    sqlite3_path = os.path.join(tmp_path,f'{dataset}.sqlite3')

    organisation = Organisation(organisation_csv, Path(os.path.dirname(blank_patch_csv)))
    package = DatasetPackage(
        'conservation-area',
        organisation=organisation,
        path=sqlite3_path,
        specification_dir=specification_dir,  # TBD: package should use this specification object
    )

    # create package
    package.create()

    # run upload to fact table not fact resource for testing the upsert
    package.connect()
    package.create_cursor()
    fact_fields = package.specification.schema["fact"]["fields"]
    fact_conflict_fields=['fact']
    fact_update_fields = [field for field in fact_fields if field not in fact_conflict_fields]
    for row in transformed_fact_resources:
        package.entry_date_upsert("fact", fact_fields, row,fact_conflict_fields,fact_update_fields)
    package.commit()
    package.disconnect()

    # retrieve results
    package.connect()
    package.create_cursor()
    package.cursor.execute("SELECT * FROM fact;")
    cols = [column[0] for column in package.cursor.description]
    actual_result = pd.DataFrame.from_records(package.cursor.fetchall(), columns=cols).to_dict(orient='records')
    expected_result = [{'end_date': '', 'entity': 44006677, 'fact': '1f90248fd06e49accd42b80e43d58beeac300f942f1a9f71da4b64865356b1f3', 'field': 'name', 'entry_date': '2022-11-02', 'reference_entity': '', 'start_date': '', 'value': 'Burghwallis'}]
    
    assert actual_result == expected_result,'actual result does not match query'

