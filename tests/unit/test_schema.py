from digital_land.schema import DatasetSchema, FieldSchema, Schema


def test_endpoint_scheama():
    endpoint = Schema("endpoint")
    assert endpoint.key == "endpoint"
    assert endpoint.field["endpoint"].name == "endpoint"
    assert list(endpoint.field) == [
        "endpoint",
        "endpoint-url",
        "parameters",
        "plugin",
        "entry-date",
        "start-date",
        "end-date",
    ]


def test_field_schema_round_trips_attributes():
    field = FieldSchema(
        field="geometry",
        datatype="wkt",
        cardinality="1",
        parent_field="",
        typology="geography",
    )
    assert field.field == "geometry"
    assert field.datatype == "wkt"
    assert field.cardinality == "1"
    assert field.parent_field == ""
    assert field.typology == "geography"


def test_dataset_schema_round_trips_attributes():
    geometry = FieldSchema(field="geometry", datatype="wkt", cardinality="1")
    reference = FieldSchema(field="reference", datatype="string", cardinality="1")

    dataset = DatasetSchema(
        dataset="title-boundary",
        prefix="title-boundary",
        typology="geography",
        entity_minimum=12000000000,
        entity_maximum=12999999999,
        key_field="",
        fields={"geometry": geometry, "reference": reference},
    )

    assert dataset.dataset == "title-boundary"
    assert dataset.prefix == "title-boundary"
    assert dataset.typology == "geography"
    assert dataset.entity_minimum == 12000000000
    assert dataset.entity_maximum == 12999999999
    assert set(dataset.fields) == {"geometry", "reference"}
    assert dataset.fields["geometry"].datatype == "wkt"


def test_dataset_schema_fields_default_is_not_shared_between_instances():
    a = DatasetSchema(
        dataset="a", prefix="a", typology="geography",
        entity_minimum=1, entity_maximum=2, key_field="",
    )
    b = DatasetSchema(
        dataset="b", prefix="b", typology="geography",
        entity_minimum=1, entity_maximum=2, key_field="",
    )

    a.fields["x"] = FieldSchema(field="x", datatype="string", cardinality="1")

    assert list(a.fields) == ["x"]
    assert list(b.fields) == []


def _title_boundary_schema(**overrides):
    defaults = dict(
        dataset="title-boundary",
        prefix="title-boundary",
        typology="geography",
        entity_minimum=12000000000,
        entity_maximum=12999999999,
        key_field="",
        fields={
            "geometry": FieldSchema(field="geometry", datatype="wkt", cardinality="1"),
            "reference": FieldSchema(
                field="reference", datatype="string", cardinality="1"
            ),
        },
    )
    defaults.update(overrides)
    return DatasetSchema(**defaults)


def test_field_schema_hash_changes_when_a_hashed_attribute_changes():
    field = FieldSchema(field="geometry", datatype="wkt", cardinality="1")
    changed = field.model_copy(update={"datatype": "multipolygon"})

    assert field.hash() != changed.hash()


def test_dataset_schema_hash_is_stable_when_dataset_name_changes():
    a = _title_boundary_schema()
    b = _title_boundary_schema(dataset="renamed-dataset")

    assert a.hash() == b.hash()


def test_dataset_schema_hash_changes_when_entity_range_changes():
    a = _title_boundary_schema()
    b = _title_boundary_schema(entity_minimum=12000000001)

    assert a.hash() != b.hash()


def test_dataset_schema_hash_changes_when_a_field_changes():
    a = _title_boundary_schema()
    b = _title_boundary_schema()
    b.fields["geometry"].datatype = "multipolygon"

    assert a.hash() != b.hash()


def test_dataset_schema_hash_is_independent_of_fields_construction_order():
    geometry = FieldSchema(field="geometry", datatype="wkt", cardinality="1")
    reference = FieldSchema(field="reference", datatype="string", cardinality="1")

    a = _title_boundary_schema(fields={"geometry": geometry, "reference": reference})
    b = _title_boundary_schema(fields={"reference": reference, "geometry": geometry})

    assert a.hash() == b.hash()
