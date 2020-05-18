from digital_land.issues import Issues
from digital_land.datatype.uri import URIDataType


def test_uri_normalise():
    uri = URIDataType()
    assert uri.normalise("https://example.com/foo") == "https://example.com/foo"

    assert (
        uri.normalise("https://example.com/foo\nbar\n/baz")
        == "https://example.com/foobar/baz"
    )

    issues = Issues()
    assert uri.normalise("example.com", issues=issues) == ""

    issue = issues.rows.pop()
    assert issue["issue-type"] == "uri"
    assert issue["value"] == "example.com"
    assert issues.rows == []
