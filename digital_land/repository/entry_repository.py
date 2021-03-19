import logging
import os
import sqlite3
from collections import defaultdict

from ..model.entry import Entry
from ..model.fact import Fact

logger = logging.getLogger(__name__)


SKIP_FACT_FIELDS = ["slug", "resource"]


# TODO: reverse logic of select/insert or ignore stmts


class EntryRepository:
    def __init__(self, db_path, create=False):
        if db_path != ":memory:" and not os.path.isfile(db_path) and not create:
            raise ValueError(f"no database file found at {db_path}")

        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row

        if create:
            self._create_schema()

    def add(self, entry: Entry):
        "adds an Entry to the repository or updates it's entries if the fact already exists"

        if not entry.slug:
            raise ValueError("cannot add entry due to missing slug")

        # context manager handles the DB transaction automatically
        with self.conn as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN")

            self._insert_entity(cursor, entry.slug)
            entry_id = self._insert_entry(cursor, entry)

            for fact in entry.facts:
                if not fact.value:
                    logger.debug(
                        "skipping field %s due to missing value", fact.attribute
                    )
                    continue

                logger.debug("inserting fact %s", fact)
                fact_id = self._insert_fact(cursor, fact)
                logger.debug("\t\tfact_id: %s, entry_id: %s", fact_id, entry_id)
                self._insert_provenance(cursor, entry_id, fact_id)

    def find_by_entity(self, entity: str):
        "returns all Entries associated with the specificed entity ref"

        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT
                fact.*,
                entry.resource AS "__RESOURCE__",
                entry.line_num AS "__LINE_NUM__",
                entry.entry_date AS "__ENTRY_DATE__"
            FROM fact
            JOIN provenance ON provenance.fact = fact.id
            JOIN entry ON provenance.entry = entry.id
            WHERE entry.entity = ?
        """,
            (entity,),
        )
        return self._entries_from_rows(entity, cursor.fetchall())

    def find_by_fact(self, fact):
        "returns all Entries that state the given fact"

        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT
                fact.*,
                entry.resource AS "__RESOURCE__",
                entry.line_num AS "__LINE_NUM__",
                entry.entry_date AS "__ENTRY_DATE__"
            FROM fact
            JOIN provenance ON provenance.fact = fact.id
            JOIN entry ON provenance.entry = entry.id
            WHERE fact.entity = ?
            AND fact.attribute = ?
            AND fact.value = ?
        """,
            (fact.entity, fact.attribute, fact.value),
        )

        result = self._entries_from_rows(fact.entity, cursor.fetchall())
        return result

    def _create_schema(self):
        cursor = self.conn.cursor()
        cursor.execute("""PRAGMA foreign_keys = ON""")

        logger.warning("dropping tables")
        cursor.execute("""DROP TABLE IF EXISTS provenance""")
        cursor.execute("""DROP TABLE IF EXISTS fact""")
        cursor.execute("""DROP TABLE IF EXISTS entry""")
        cursor.execute("""DROP TABLE IF EXISTS entity""")

        logger.warning("creating tables")
        cursor.execute(
            """CREATE TABLE entity (slug TEXT PRIMARY KEY NOT NULL CHECK(LENGTH(slug) > 0))"""
        )
        cursor.execute(
            """CREATE TABLE entry (
                id INTEGER PRIMARY KEY,
                resource TEXT,
                line_num INTEGER,
                entity NOT NULL,
                entry_date TEXT NOT NULL,
                FOREIGN KEY(entity) REFERENCES entity(slug),
                UNIQUE(resource, line_num)
            )"""
        )
        cursor.execute(
            """CREATE TABLE fact (
                id INTEGER PRIMARY KEY,
                entity TEXT,
                attribute TEXT,
                value TEXT,
                FOREIGN KEY(entity) REFERENCES entity(slug),
                UNIQUE(entity, attribute, value)
            )"""
        )
        cursor.execute(
            """CREATE TABLE provenance (
                entry INTEGER,
                fact INTEGER,
                entry_date,
                start_date,
                end_date,
                FOREIGN KEY(entry) REFERENCES entry(id),
                FOREIGN KEY(fact) REFERENCES fact(id),
                UNIQUE(entry, fact)
            )"""
        )

    def _insert_entity(self, cursor, slug):
        cursor.execute("""INSERT OR IGNORE INTO entity VALUES(?)""", (slug,))

    def _insert_entry(self, cursor, entry):
        cursor.execute(
            """INSERT OR IGNORE INTO entry(resource, line_num, entity, entry_date) VALUES(?, ?, ?, ?)""",
            (entry.resource, entry.line_num, entry.slug, entry.entry_date),
        )
        cursor.execute(
            """SELECT rowid FROM entry WHERE resource = ? AND line_num = ? AND entity = ?""",
            (entry.resource, entry.line_num, entry.slug),
        )
        return cursor.fetchone()[0]

    def _insert_fact(self, cursor, fact):
        cursor.execute(
            """INSERT OR IGNORE INTO fact(entity, attribute, value) VALUES(?,?,?)""",
            (
                fact.entity,
                fact.attribute,
                fact.value,
            ),
        )
        cursor.execute(
            """SELECT rowid FROM fact WHERE entity = ? AND attribute = ? AND value = ?""",
            (fact.entity, fact.attribute, fact.value),
        )
        return cursor.fetchone()[0]

    def _insert_provenance(self, cursor, entry_id, fact_id):
        cursor.execute(
            """INSERT INTO provenance(entry, fact) VALUES(?,?)""",
            (
                entry_id,
                fact_id,
            ),
        )

    def _entries_from_rows(self, entity, rows):
        entry_fact = defaultdict(set)
        for row in rows:
            rowdict = dict(row)
            resource = rowdict.pop("__RESOURCE__")
            line_num = rowdict.pop("__LINE_NUM__")
            entry_date = rowdict.pop("__ENTRY_DATE__")
            entry_fact[(resource, line_num, entry_date)].add(
                Fact(entity, rowdict["attribute"], rowdict["value"])
            )

        result = {
            Entry.from_facts(entity, facts, key[0], key[1], key[2])
            for key, facts in entry_fact.items()
        }
        return result
