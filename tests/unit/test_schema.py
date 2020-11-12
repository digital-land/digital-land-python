from digital_land.schema import Schema


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
