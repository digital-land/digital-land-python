import logging
import os
import sqlite3
from collections import defaultdict

from ..model.entry import Entry
from ..model.fact import Fact

logger = logging.getLogger(__name__)


SKIP_FACT_FIELDS = ["entity", "slug", "resource"]


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

            if entry.entity:
                self._insert_entity(cursor, entry.entity)
            self._insert_slug(cursor, entry.slug, entry.entity)
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

    def list_entities(self):
        "returns a list of all entities with facts in the repo"

        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT
                entity
            FROM entity
            ORDER BY 1
        """,
        )
        return [row["entity"] for row in cursor.fetchall()]

    def list_slugs(self):
        "returns a list of all entities with facts in the repo"

        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT
                slug
            FROM slug
            ORDER BY 1
        """,
        )
        return [row["slug"] for row in cursor.fetchall()]

    def list_attributes(self):
        "returns a list of all attributes across all facts in the repo"

        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT DISTINCT
                attribute
            FROM fact
            ORDER BY 1
        """,
        )
        return {row["attribute"] for row in cursor.fetchall()}

    def find_by_slug(self, slug: str):
        "returns all Entries associated with the specified slug"

        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT
                fact.*,
                entry.entity AS "__ENTITY__",
                entry.resource AS "__RESOURCE__",
                entry.line_num AS "__LINE_NUM__",
                entry.entry_date AS "__ENTRY_DATE__"
            FROM fact
            JOIN provenance ON provenance.fact = fact.id
            JOIN entry ON provenance.entry = entry.id
            WHERE entry.slug = ?
        """,
            (slug,),
        )
        return self._entries_from_rows(slug, cursor.fetchall())

    def find_by_entity(self, entity: int):
        "returns all Entries associated with the specified entity ref"

        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT
                fact.*,
                entry.slug AS "__SLUG__",
                entry.entity AS "__ENTITY__",
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

        slug_rows = defaultdict(list)
        for row in cursor.fetchall():
            slug_rows[row["__SLUG__"]].append(row)

        result = set()
        for slug, rows in slug_rows.items():
            result.update(self._entries_from_rows(slug, rows))

        return result

    def find_by_fact(self, fact):
        "returns all Entries that state the given fact"

        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT
                fact.*,
                entry.entity AS "__ENTITY__",
                entry.resource AS "__RESOURCE__",
                entry.line_num AS "__LINE_NUM__",
                entry.entry_date AS "__ENTRY_DATE__"
            FROM fact
            JOIN provenance ON provenance.fact = fact.id
            JOIN entry ON provenance.entry = entry.id
            WHERE fact.slug = ?
            AND fact.attribute = ?
            AND fact.value = ?
        """,
            (fact.slug, fact.attribute, fact.value),
        )

        result = self._entries_from_rows(fact.slug, cursor.fetchall())
        return result

    def _create_schema(self):
        cursor = self.conn.cursor()
        cursor.execute("""PRAGMA foreign_keys = ON""")

        logger.warning("dropping tables")
        cursor.execute("""DROP TABLE IF EXISTS provenance""")
        cursor.execute("""DROP TABLE IF EXISTS fact""")
        cursor.execute("""DROP TABLE IF EXISTS entry""")
        cursor.execute("""DROP TABLE IF EXISTS slug""")
        cursor.execute("""DROP TABLE IF EXISTS entity""")

        logger.warning("creating tables")
        cursor.execute(
            """CREATE TABLE entity (
                entity INTEGER PRIMARY KEY NOT NULL
            )"""
        )

        cursor.execute(
            """CREATE TABLE slug (
                slug TEXT PRIMARY KEY NOT NULL CHECK(LENGTH(slug) > 0),
                entity INTEGER,
                FOREIGN KEY(entity) REFERENCES entity(entity)
            )"""
        )

        cursor.execute(
            """CREATE TABLE entry (
                id INTEGER PRIMARY KEY,
                resource TEXT,
                line_num INTEGER,
                entity INTEGER,
                slug TEXT NOT NULL,
                entry_date TEXT NOT NULL,
                FOREIGN KEY(slug) REFERENCES slug(slug),
                FOREIGN KEY(entity) REFERENCES entity(entity),
                UNIQUE(resource, line_num)
            )"""
        )
        cursor.execute("""CREATE INDEX entry_slug ON entry(slug)""")
        cursor.execute("""CREATE INDEX entry_entity ON entry(entity)""")
        cursor.execute("""CREATE INDEX entry_id ON entry(id)""")
        cursor.execute(
            """CREATE TABLE fact (
                id INTEGER PRIMARY KEY,
                entity INTEGER,
                slug TEXT NOT NULL,
                attribute TEXT,
                value TEXT,
                FOREIGN KEY(slug) REFERENCES slug(slug),
                FOREIGN KEY(entity) REFERENCES entity(entity),
                UNIQUE(slug, attribute, value)
            )"""
        )
        cursor.execute("""CREATE INDEX fact_slug ON fact(slug)""")
        cursor.execute("""CREATE INDEX fact_entity ON fact(entity)""")
        cursor.execute("""CREATE INDEX fact_id ON fact(id)""")
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
        cursor.execute("""CREATE INDEX provenance_fact ON provenance(fact)""")
        cursor.execute("""CREATE INDEX provenance_entry ON provenance(entry)""")

    def _insert_entity(self, cursor, entity):
        cursor.execute("""INSERT OR IGNORE INTO entity VALUES(?)""", (entity,))

    def _insert_slug(self, cursor, slug, entity):
        cursor.execute(
            """INSERT OR IGNORE INTO slug VALUES(?, ?)""",
            (
                slug,
                entity,
            ),
        )

    def _insert_entry(self, cursor, entry):
        cursor.execute(
            """INSERT OR IGNORE INTO entry(resource, line_num, entity, slug, entry_date) VALUES(?, ?, ?, ?, ?)""",
            (
                entry.resource,
                entry.line_num,
                entry.entity,
                entry.slug,
                entry.entry_date,
            ),
        )
        cursor.execute(
            """SELECT rowid FROM entry WHERE resource = ? AND line_num = ? AND slug = ?""",
            (entry.resource, entry.line_num, entry.slug),
        )
        return cursor.fetchone()[0]

    def _insert_fact(self, cursor, fact):
        cursor.execute(
            """INSERT OR IGNORE INTO fact(entity, slug, attribute, value) VALUES(?,?,?,?)""",
            (
                fact.entity,
                fact.slug,
                fact.attribute,
                fact.value,
            ),
        )
        cursor.execute(
            """SELECT rowid FROM fact WHERE slug = ? AND attribute = ? AND value = ?""",
            (fact.slug, fact.attribute, fact.value),
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

    def _entries_from_rows(self, slug, rows):
        entry_fact = defaultdict(set)
        for row in rows:
            rowdict = dict(row)
            entity = rowdict.pop("__ENTITY__")
            resource = rowdict.pop("__RESOURCE__")
            line_num = rowdict.pop("__LINE_NUM__")
            entry_date = rowdict.pop("__ENTRY_DATE__")
            entry_fact[(resource, line_num, entry_date)].add(
                Fact(entity, slug, rowdict["attribute"], rowdict["value"])
            )

        result = {
            Entry.from_facts(entity, slug, facts, *key)
            for key, facts in entry_fact.items()
        }
        return result
