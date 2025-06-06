import pytest
import pandas as pd

from psycopg2.extras import execute_values

from digital_land.package.platform import PlatformPackage
from digital_land.utils.postgres_utils import get_df


def store_as_csv(data, csv_path):
    """
    helper function to store data in a dictionary as a csv
    """
    df = pd.DataFrame.from_dict(data)
    df.to_csv(csv_path, sep="|", index=False)


def insert_df(df, table_name, conn):
    """
    helper function to insert a dataframe into a table
    """
    cur = conn.cursor()
    execute_values(
        cur,
        f"INSERT INTO {table_name} ({','.join(list(df.columns))}) VALUES %s",
        df.values.tolist(),
    )
    cur.close()


@pytest.mark.parametrize(
    "data",
    [
        {
            "fact": [
                "dfdbd3153e0e8529c16a623f10c7770ca0e8a267465a22051c162c1fcd5413fc"
            ],
            "resource": [
                "e51e6f8fd1cadb696d5ca229236b84f2ec67afdea6aad03fc81142b04c67adbd"
            ],
            "entry_date": ["2025-01-01"],
            "entry_number": ["2"],
            "priority": ["1"],
            "reference_entity": ["122"],
            "dataset": ["article-4-direction"],
        }
    ],
)
def test_load_fact_resource_no_data_in_db(data, tmp_path, platfom_db_session_conn):
    """
    test function to check the function loading fact_resource data into the db
    """

    conn = platfom_db_session_conn

    csv_path = tmp_path / "fact_resource.csv"
    input_df = pd.DataFrame.from_dict(data)
    store_as_csv(data, csv_path)

    # create platform object
    platform = PlatformPackage()

    platform.update_fact_resource(input_file=csv_path, conn=conn)

    output_df = get_df("fact_resource", conn)
    assert len(output_df) > 0
    assert len(input_df) == len(output_df)


@pytest.mark.parametrize(
    "data",
    [
        {
            "fact": [
                "dfdbd3153e0e8529c16a623f10c7770ca0e8a267465a22051c162c1fcd5413fc"
            ],
            "resource": [
                "e51e6f8fd1cadb696d5ca229236b84f2ec67afdea6aad03fc81142b04c67adbd"
            ],
            "entry_date": ["2025-01-01"],
            "entry_number": ["2"],
            "priority": ["1"],
            "reference_entity": ["122"],
            "dataset": ["article-4-direction"],
        }
    ],
)
def test_load_fact_resource_same_file_twice(data, tmp_path, platfom_db_session_conn):
    """
    test function to check the function loading fact_resource data into the db
    speccifically if the exact same data then tthe data should be overwritten
    """

    conn = platfom_db_session_conn

    csv_path = tmp_path / "fact_resource.csv"
    input_df = pd.DataFrame.from_dict(data)
    store_as_csv(data, csv_path)

    # create platform object
    platform = PlatformPackage()

    platform.update_fact_resource(input_file=csv_path, conn=conn)

    platform.update_fact_resource(input_file=csv_path, conn=conn)

    output_df = get_df("fact_resource", conn)

    assert len(output_df) > 0
    assert len(input_df) == len(output_df)


@pytest.mark.parametrize(
    "data",
    [
        {
            "fact": [
                "dfdbd3153e0e8529c16a623f10c7770ca0e8a267465a22051c162c1fcd5413fc"
            ],
            "entity": [1],
            "field": ["name"],
            "value": ["test"],
            "resource": [
                "e51e6f8fd1cadb696d5ca229236b84f2ec67afdea6aad03fc81142b04c67adbd"
            ],
            "entry_date": ["2025-01-01"],
            "entry_number": [3],
            "priority": [1],
            "reference_entity": [122],
            "dataset": ["article-4-direction"],
        }
    ],
)
def test_load_fact_no_data_in_db(data, tmp_path, platfom_db_session_conn):
    """
    test function to check the function loading fact_resource data into the db
    """

    # need to add a row into fact resourcce for the values
    conn = platfom_db_session_conn

    # fact_resource_datta
    fact_resourcce_df = pd.DataFrame.from_dict(
        {
            "fact": [
                "dfdbd3153e0e8529c16a623f10c7770ca0e8a267465a22051c162c1fcd5413fc"
            ],
            "resource": [
                "e51e6f8fd1cadb696d5ca229236b84f2ec67afdea6aad03fc81142b04c67adbd"
            ],
            "entry_date": ["2025-01-01"],
            "entry_number": ["2"],
            "priority": ["1"],
            "reference_entity": ["122"],
            "dataset": ["article-4-direction"],
        }
    )

    csv_path = tmp_path / "fact_resource.csv"
    store_as_csv(fact_resourcce_df, csv_path)

    platform = PlatformPackage()
    platform.update_fact_resource(input_file=csv_path, conn=conn)

    fact_csv_path = tmp_path / "fact.csv"
    input_df = pd.DataFrame.from_dict(data)
    store_as_csv(data, fact_csv_path)

    platform.update_fact(input_file=fact_csv_path, conn=conn)

    fact_resource_output_df = get_df("fact_resource", conn)
    output_df = get_df("fact", conn)
    assert len(output_df) > 0
    assert len(input_df) == len(output_df)

    # some fact specific checks we need to make
    # there should be no facts in the fact table which are not in the fact resource table
    for fact in output_df["fact"]:
        assert (
            fact in fact_resource_output_df["fact"].values
        ), f"fact {fact} is not in fat resource"
    # there should be no fact resource combination in fact which isn't in the fact resource table
    # this is aiming to protect us from facts being emoved from rresources but still existing in another
    # get a set of pairs from fact resource
    existing_pairs = set(
        zip(fact_resource_output_df["fact"], fact_resource_output_df["resource"])
    )

    # get a set of pairs from fact
    fact_pairs = set(zip(output_df["fact"], output_df["resource"]))

    # check all fact pairs are in
    for pair in fact_pairs:
        assert (
            pair in existing_pairs
        ), f"fact {pair[0]} and resource {pair[1]} is not in fact resource"

    # test to write
    # facts are removed if they are not in the fact resource table
    # replacing facts from the same resource
    # removing a resource


@pytest.mark.parametrize(
    "fact_data",
    [
        {
            "fact": [
                "dfdbd3153e0e8529c16a623f10c7770ca0e8a267465a22051c162c1fcd5413fc"
            ],
            "entity": [1],
            "field": ["name"],
            "value": ["test"],
            "resource": [
                "e51e6f8fd1cadb696d5ca229236b84f2ec67afdea6aad03fc81142b04c67adbd"
            ],
            "entry_date": ["2025-01-01"],
            "entry_number": [3],
            "priority": [1],
            "reference_entity": [122],
            "dataset": ["article-4-direction"],
        }
    ],
)
def test_update_entity_updates_from_facts(platfom_db_session_conn, fact_data, tmp_path):
    """
    test function to check the function loading fact_resource data into the db
    """

    # need to add a row into fact resourcce for the values
    conn = platfom_db_session_conn

    # Write fact data straight into db, avoids needing anything in the fact_resouce table
    # ather than using the update_fact
    fact_input_df = pd.DataFrame.from_dict(fact_data)
    insert_df(fact_input_df, "fact", conn)

    entity_df = fact_input_df[["entity"]].drop_duplicates()
    store_as_csv(entity_df, tmp_path / "entity.csv")

    # auto get the entities from fact imports
    platform = PlatformPackage()
    platform.update_entity(conn=conn, entity_csv=tmp_path / "entity.csv")

    entity_output_df = get_df("entity", conn)
    assert len(entity_output_df) > 0
    assert len(entity_output_df) == len(set(fact_input_df["entity"]))
    # check that any field values make it into the final df
    # test the name is corect
    for entity in entity_output_df["entity"]:
        assert entity in list(fact_input_df["entity"])

        row = entity_output_df[entity_output_df["entity"] == entity].iloc[0].to_dict()

        # this can be used for several fields that aren't in json
        for col in ["name", "entry_date", "start_date", "end_date", "refeence"]:
            col_value = row.get(col)
            if col_value:
                # check that the name is in the facts going in otherwise whee's it come fom
                # could expand to ode aswell
                assert col_value in list(
                    fact_input_df[
                        (fact_input_df["entity"] == entity)
                        & (fact_input_df["field"] == col)
                    ]["value"]
                )
            else:
                # if name is null then there should be no facts
                assert col not in list(
                    fact_input_df[fact_input_df["entity"] == entity]["field"]
                )
