import pytest
from digital_land.view_model import ViewModelJsonQuery


def test_expand_columns():
    view_model = ViewModelJsonQuery()

    data = {}
    data["rows"] = [
        {
            "id": 605,
            "slug_id": {
                "value": 7605,
                "label": "/development-policy/local-authority-eng/BUC/sbcs-CP7",
            },
        },
        {
            "id": 822,
            "slug_id": {
                "value": 7822,
                "label": "/development-policy/local-authority-eng/BUC/wdlp-DM41",
            },
        },
    ]

    data["expandable_columns"] = [
        [{"column": "slug_id", "other_table": "slug", "other_column": "id"}, "slug"]
    ]

    data["expanded_columns"] = ["slug_id"]

    data["columns"] = ["id", "slug_id"]

    reader = view_model.expand_columns(data)

    assert list(reader) == [
        {
            "id": 605,
            "slug_id": 7605,
            "slug": "/development-policy/local-authority-eng/BUC/sbcs-CP7",
        },
        {
            "id": 822,
            "slug_id": 7822,
            "slug": "/development-policy/local-authority-eng/BUC/wdlp-DM41",
        },
    ]


def test_expand_columns_name_clash():
    view_model = ViewModelJsonQuery()

    data = {}
    data["rows"] = [
        {
            "id": 605,
            "slug_id": {
                "value": 7605,
                "label": "/development-policy/local-authority-eng/BUC/sbcs-CP7",
            },
            "slug": "blah",
        },
    ]

    data["expandable_columns"] = [
        [{"column": "slug_id", "other_table": "slug", "other_column": "id"}, "slug"]
    ]

    data["expanded_columns"] = ["slug_id"]

    data["columns"] = ["id", "slug_id", "slug"]

    with pytest.raises(ValueError, match="^name clash trying to expand slug label$"):
        list(view_model.expand_columns(data))
