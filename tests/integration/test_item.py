from digital_land.schema import Schema
from digital_land.store.item import CSVItemStore


def test_CSVItemStore_load_items():
    csvItemStore = CSVItemStore(Schema("operational-issue"))

    csvItemStore.load_items(
        directory="tests/data/listed-building/performance/operational_issue/"
    )

    assert len(csvItemStore.entries) == 3
    assert csvItemStore.entries[0]["value"] == "listed-building-outline:2"
    assert csvItemStore.entries[0]["entry-date"] == "2024-09-10"


def test_CSVItemStore_load_items_after():
    csvItemStore = CSVItemStore(Schema("operational-issue"))

    csvItemStore.load_items(
        directory="tests/data/listed-building/performance/operational_issue/",
        after="2024-09-18",
    )

    assert len(csvItemStore.entries) == 2


def test_CSVItemStore_load_items_dataset():
    csvItemStore = CSVItemStore(Schema("operational-issue"))

    csvItemStore.load_items(
        directory="tests/data/listed-building/performance/operational_issue/",
        dataset="listed-building",
    )

    assert len(csvItemStore.entries) == 1


def test_CSVItemStore_add_entry():
    csvItemStore = CSVItemStore(Schema("operational-issue"))

    csvItemStore.add_entry({"dataset": "dataset", "entry-date": "2024-01-01"})

    assert csvItemStore.entries[0]["dataset"] == "dataset"
    assert csvItemStore.latest_entry_date() == "2024-01-01"
