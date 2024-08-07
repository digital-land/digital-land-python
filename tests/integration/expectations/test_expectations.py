# TODO all the below use on old version of this framework wihch does not focus
# on issues. We need to identify if they're still relevant and remove ones which aren't

# import pytest
# import spatialite
# import os
# import pandas as pd

# from digital_land.expectations.utils import QueryRunner
# from digital_land.expectations.expectation_functions.sqlite import (
# expect_filtered_entities_to_be_as_predicted,
# expect_entities_to_intersect_given_geometry_to_be_as_predicted,
# count_entities,
# compare_entities,
# compare_column_values,
# validate_wkt_values,
# )

# @pytest.fixture
# def sqlite3_with_entity_table_path(tmp_path):
#     dataset_path = os.path.join(tmp_path, "test.sqlite3")

#     create_table_sql = """
#         CREATE TABLE entity (
#             dataset TEXT,
#             end_date TEXT,
#             entity INTEGER PRIMARY KEY,
#             entry_date TEXT,
#             geojson JSON,
#             geometry TEXT,
#             json JSON,
#             name TEXT,
#             organisation_entity TEXT,
#             point TEXT,
#             prefix TEXT,
#             reference TEXT,
#             start_date TEXT,
#             typology TEXT
#         );
#     """
#     with spatialite.connect(dataset_path) as con:
#         con.execute(create_table_sql)

#     return dataset_path

# def test_expect_filtered_entities_to_be_as_predicted_runs_for_correct_input(
#     sqlite3_with_entity_table_path,
# ):
#     # load data
#     test_data = pd.DataFrame.from_dict(
#         {"entity": [1, 2], "name": ["test1", "test2"], "reference": ["1", "2"]}
#     )
#     with spatialite.connect(sqlite3_with_entity_table_path) as con:
#         test_data.to_sql("entity", con, if_exists="append", index=False)

#     # build inputs
#     query_runner = QueryRunner(sqlite3_with_entity_table_path)
#     expected_result = [{"name": "test1"}]
#     columns = ["name"]
#     filters = {"reference": "1"}

#     # run expectation
#     result, msg, details = expect_filtered_entities_to_be_as_predicted(
#         query_runner=query_runner,
#         columns=columns,
#         expected_result=expected_result,
#         filters=filters,
#     )

#     assert result, f"Expectation Details: {details}"


# def test_expect_filtered_entities_to_be_as_predicted_fails(
#     sqlite3_with_entity_table_path,
# ):
#     # load data
#     test_data = pd.DataFrame.from_dict(
#         {"entity": [1, 2], "name": ["test1", "test2"], "reference": ["1", "2"]}
#     )
#     with spatialite.connect(sqlite3_with_entity_table_path) as con:
#         test_data.to_sql("entity", con, if_exists="append", index=False)

#     # build inputs
#     query_runner = QueryRunner(sqlite3_with_entity_table_path)
#     expected_result = [{"name": "incorrect value"}]
#     columns = ["name"]
#     filters = {"reference": "1"}

#     # run expectation
#     result, msg, details = expect_filtered_entities_to_be_as_predicted(
#         query_runner=query_runner,
#         expected_result=expected_result,
#         columns=columns,
#         filters=filters,
#     )

#     assert not result, f"Expectation Details: {details}"


# def test_expect_entities_to_intersect_given_geometry_to_be_as_predicted_passes(
#     sqlite3_with_entity_table_path,
# ):
#     # load test data
#     multipolygon = (
#         "MULTIPOLYGON(((-0.4610469722185172 52.947516855690964,"
#         "-0.4614606467578964 52.94650314047493,"
#         "-0.4598136600343151 52.94695770522492,"
#         "-0.4610469722185172 52.947516855690964)))"
#     )
#     test_data = pd.DataFrame.from_dict(
#         {"entity": [1], "name": ["test1"], "geometry": [multipolygon]}
#     )
#     with spatialite.connect(sqlite3_with_entity_table_path) as con:
#         test_data.to_sql("entity", con, if_exists="append", index=False)

#     # build inputs
#     query_runner = QueryRunner(sqlite3_with_entity_table_path)
#     expected_result = [{"name": "test1"}]
#     returned_entity_fields = ["name"]
#     geometry = "POINT(-0.460759538145794 52.94701402037683)"

#     # run expectation
#     (
#         result,
#         msg,
#         details,
#     ) = expect_entities_to_intersect_given_geometry_to_be_as_predicted(
#         query_runner=query_runner,
#         expected_result=expected_result,
#         returned_entity_fields=returned_entity_fields,
#         geometry=geometry,
#     )

#     assert result, f"Expectation Details: {details}"


# def test_expect_entities_to_intersect_given_geometry_to_be_as_predicted_fails(
#     sqlite3_with_entity_table_path,
# ):
#     # load test data
#     multipolygon = (
#         "MULTIPOLYGON(((-0.4610469722185172 52.947516855690964,"
#         "-0.4614606467578964 52.94650314047493,"
#         "-0.4598136600343151 52.94695770522492,"
#         "-0.4610469722185172 52.947516855690964)))"
#     )
#     test_data = pd.DataFrame.from_dict(
#         {"entity": [1], "name": ["test1"], "geometry": [multipolygon]}
#     )
#     with spatialite.connect(sqlite3_with_entity_table_path) as con:
#         test_data.to_sql("entity", con, if_exists="append", index=False)

#     # build inputs
#     query_runner = QueryRunner(sqlite3_with_entity_table_path)
#     expected_result = [{"name": "test1"}]
#     returned_entity_fields = ["name"]
#     geometry = "POINT(-0.4581196580693358 52.947003722396005)"

#     # run expectation
#     (
#         result,
#         details,
#         msg,
#     ) = expect_entities_to_intersect_given_geometry_to_be_as_predicted(
#         query_runner=query_runner,
#         expected_result=expected_result,
#         returned_entity_fields=returned_entity_fields,
#         geometry=geometry,
#     )

#     assert not result, f"Expectation Details: {details}"


# def test_count_entities_passes(sqlite3_with_entity_table_path):
#     # load test data
#     multipolygon = (
#         "MULTIPOLYGON(((-0.4610469722185172 52.947516855690964,"
#         "-0.4614606467578964 52.94650314047493,"
#         "-0.4598136600343151 52.94695770522492,"
#         "-0.4610469722185172 52.947516855690964)))"
#     )
#     test_data = pd.DataFrame.from_dict(
#         {"entity": [1], "name": ["test1"], "geometry": [multipolygon]}
#     )
#     with spatialite.connect(sqlite3_with_entity_table_path) as con:
#         test_data.to_sql("entity", con, if_exists="append", index=False)

#     # build inputs
#     query_runner = QueryRunner(sqlite3_with_entity_table_path)
#     expected_result = 1
#     filters = {"geometry": "POINT(-0.460759538145794 52.94701402037683)"}

#     # run expectation
#     result, msg, details = count_entities(
#         query_runner=query_runner, expected_result=expected_result, filters=filters
#     )

#     assert result, f"Expectation Details: {details}"


# def test_count_entities_greater_than(sqlite3_with_entity_table_path):
#     # load test data
#     multipolygon = (
#         "MULTIPOLYGON(((-0.4610469722185172 52.947516855690964,"
#         "-0.4614606467578964 52.94650314047493,"
#         "-0.4598136600343151 52.94695770522492,"
#         "-0.4610469722185172 52.947516855690964)))"
#     )
#     test_data = pd.DataFrame.from_dict(
#         {"entity": [1], "name": ["test1"], "geometry": [multipolygon]}
#     )
#     with spatialite.connect(sqlite3_with_entity_table_path) as con:
#         test_data.to_sql("entity", con, if_exists="append", index=False)

#     # build inputs
#     query_runner = QueryRunner(sqlite3_with_entity_table_path)
#     expected_result = 0
#     filters = {"geometry": "POINT(-0.460759538145794 52.94701402037683)"}

#     # run expectation
#     result, msg, details = count_entities(
#         query_runner=query_runner,
#         expected_result=expected_result,
#         filters=filters,
#         assertion_rule="greater_than",
#     )

#     assert result, f"Expectation Details: {details}"


# def test_count_entities_fails(sqlite3_with_entity_table_path):
#     # load test data
#     multipolygon = (
#         "MULTIPOLYGON(((-0.4610469722185172 52.947516855690964,"
#         "-0.4614606467578964 52.94650314047493,"
#         "-0.4598136600343151 52.94695770522492,"
#         "-0.4610469722185172 52.947516855690964)))"
#     )
#     test_data = pd.DataFrame.from_dict(
#         {"entity": [1], "name": ["test1"], "geometry": [multipolygon]}
#     )
#     with spatialite.connect(sqlite3_with_entity_table_path) as con:
#         test_data.to_sql("entity", con, if_exists="append", index=False)

#     # build inputs
#     query_runner = QueryRunner(sqlite3_with_entity_table_path)
#     expected_result = 1
#     filters = {"geometry": "POINT(-0.4581196580693358 52.947003722396005)"}

#     # run expectation
#     result, msg, details = count_entities(
#         query_runner=query_runner, expected_result=expected_result, filters=filters
#     )

#     assert not result, f"Expectation Details: {details}"


# def test_compare_entities_passes(sqlite3_with_entity_table_path):
#     # load test data
#     multipolygon = (
#         "MULTIPOLYGON(((-0.4610469722185172 52.947516855690964,"
#         "-0.4614606467578964 52.94650314047493,"
#         "-0.4598136600343151 52.94695770522492,"
#         "-0.4610469722185172 52.947516855690964)))"
#     )
#     test_data = pd.DataFrame.from_dict(
#         {"entity": [1], "name": ["test1"], "geometry": [multipolygon]}
#     )
#     with spatialite.connect(sqlite3_with_entity_table_path) as con:
#         test_data.to_sql("entity", con, if_exists="append", index=False)

#     # build inputs
#     query_runner = QueryRunner(sqlite3_with_entity_table_path)
#     expected_result = [{"name": "test1"}]
#     columns = ["name"]
#     filters = {"geometry": "POINT(-0.460759538145794 52.94701402037683)"}

#     # run expectation
#     result, msg, details = compare_entities(
#         query_runner=query_runner,
#         expected_result=expected_result,
#         columns=columns,
#         filters=filters,
#     )

#     assert result, f"Expectation Details: {details}"


# def test_compare_entities_fails(sqlite3_with_entity_table_path):
#     # load test data
#     multipolygon = (
#         "MULTIPOLYGON(((-0.4610469722185172 52.947516855690964,"
#         "-0.4614606467578964 52.94650314047493,"
#         "-0.4598136600343151 52.94695770522492,"
#         "-0.4610469722185172 52.947516855690964)))"
#     )
#     test_data = pd.DataFrame.from_dict(
#         {"entity": [1], "name": ["test1"], "geometry": [multipolygon]}
#     )
#     with spatialite.connect(sqlite3_with_entity_table_path) as con:
#         test_data.to_sql("entity", con, if_exists="append", index=False)

#     # build inputs
#     query_runner = QueryRunner(sqlite3_with_entity_table_path)
#     expected_result = [{"name": "test1"}]
#     columns = ["name"]
#     filters = {"geometry": "POINT(-0.4581196580693358 52.947003722396005)"}

#     # run expectation
#     result, msg, details = compare_entities(
#         query_runner=query_runner,
#         expected_result=expected_result,
#         columns=columns,
#         filters=filters,
#     )

#     assert not result, f"Expectation Details: {details}"


# @pytest.fixture
# def sqlite3_with_entity_and_old_entity_table_path(tmp_path):
#     dataset_path = os.path.join(tmp_path, "test.sqlite3")

#     create_table_sql = """
#         CREATE TABLE entity (
#             dataset TEXT,
#             end_date TEXT,
#             entity INTEGER PRIMARY KEY,
#             entry_date TEXT,
#             geojson JSON,
#             geometry TEXT,
#             json JSON,
#             name TEXT,
#             organisation_entity TEXT,
#             point TEXT,
#             prefix TEXT,
#             reference TEXT,
#             start_date TEXT,
#             typology TEXT
#         );
#     """
#     with spatialite.connect(dataset_path) as con:
#         con.execute(create_table_sql)

#     create_table_sql = """
#         CREATE TABLE old_entity (
#             end_date TEXT,
#             entity INTEGER,
#             entry_date TEXT,
#             notes TEXT,
#             old_entity TEXT PRIMARY KEY,
#             start_date TEXT,
#             status TEXT, FOREIGN KEY (entity) REFERENCES entity (entity)
#         )
#         ;
#     """
#     with spatialite.connect(dataset_path) as con:
#         con.execute(create_table_sql)

#     return dataset_path


# def test_compare_column_values_success(sqlite3_with_entity_and_old_entity_table_path):
#     test_entity_data = pd.DataFrame.from_dict(
#         {"entity": [1], "name": ["test1"], "reference": ["1"]}
#     )
#     test_old_entity_data = pd.DataFrame.from_dict(
#         {"old_entity": [2], "entity": [1], "status": ["310"]}
#     )
#     with spatialite.connect(sqlite3_with_entity_and_old_entity_table_path) as con:
#         test_entity_data.to_sql("entity", con, if_exists="append", index=False)
#         test_old_entity_data.to_sql("old_entity", con, if_exists="append", index=False)

#     query_runner = QueryRunner(sqlite3_with_entity_and_old_entity_table_path)
#     expected_result = []
#     col_1 = {"table": "old_entity", "col": "old_entity"}
#     col_2 = {"table": "entity", "col": "entity"}

#     result, msg, details = compare_column_values(
#         query_runner=query_runner,
#         expected_result=expected_result,
#         col_1=col_1,
#         col_2=col_2,
#     )

#     assert result


# def test_validate_wkt_values_success(sqlite3_with_entity_table_path):
#     # load test data
#     multipolygon = (
#         "MULTIPOLYGON(((-0.4610469722185172 52.947516855690964,"
#         "-0.4614606467578964 52.94650314047493,"
#         "-0.4598136600343151 52.94695770522492,"
#         "-0.4610469722185172 52.947516855690964)))"
#     )
#     test_data = pd.DataFrame.from_dict(
#         {"entity": [1], "name": ["test1"], "geometry": [multipolygon]}
#     )
#     with spatialite.connect(sqlite3_with_entity_table_path) as con:
#         test_data.to_sql("entity", con, if_exists="append", index=False)

#     # build inputs
#     query_runner = QueryRunner(sqlite3_with_entity_table_path)

#     # run expectation
#     result, msg, details = validate_wkt_values(
#         query_runner=query_runner,
#         col="geometry",
#         table="entity",
#         include_cols=["entity"],
#     )

#     assert result, f"Expectation Details: {details}"
