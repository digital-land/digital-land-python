from digital_land.datatype.string import StringDataType


def test_string_format():
    assert StringDataType().format("Hello") == "Hello"


def test_string_normalise():
    string = StringDataType()

    assert string.normalise("Hello folks") == "Hello folks"
    assert string.normalise("""Hi Everyone""") == "Hi Everyone"
