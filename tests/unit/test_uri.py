from digital_land.log import IssueLog
from digital_land.datatype.uri import URIDataType


def test_uri_normalise():
    uri = URIDataType()
    assert uri.normalise("https://example.com/foo") == "https://example.com/foo"

    assert (
        uri.normalise("https://example.com/foo\nbar\n/baz")
        == "https://example.com/foobar/baz"
    )

    issues = IssueLog()
    issues.fieldname = "documentation-url"
    assert uri.normalise("example.com", issues=issues) == ""

    issue = issues.rows.pop()
    assert issue["issue-type"] == "invalid URI"
    assert issue["value"] == "example.com"
    assert issue["message"] == "documentation-url must be a real URL"
    assert issues.rows == []
