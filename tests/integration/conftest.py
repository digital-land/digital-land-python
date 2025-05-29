import pytest

from testcontainers.postgres import PostgresContainer
from testcontainers.core.waiting_utils import wait_container_is_ready
from sqlalchemy_utils import database_exists, create_database, drop_database

from digital_land.utils.postgres_utils import get_pg_connection

# class PostgisContainer(PostgresContainer):
#     def __init__(self, image="postgis/postgis:15-3.3"):
#         super().__init__(image=image)

postgis_container = PostgresContainer(image="postgis/postgis:15-3.3")


@pytest.fixture(scope="session")
def postgres(request):
    postgis_container.start()

    def close_postgres():
        postgis_container.stop()

    request.addfinalizer(close_postgres)
    wait_container_is_ready(postgis_container)
    return postgis_container.get_connection_url()


def create_no_fk_schema(conn):
    """
    Create the schema for the database without foreign keys
    """
    cursor = conn.cursor()
    # Create the table and load the data
    cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis")
    cursor.execute(
        """
        CREATE TABLE entity (
            entity bigint PRIMARY KEY,
            name varchar,
            entry_date varchar,
            start_date varchar,
            end_date varchar,
            dataset varchar,
            json varchar,
            organisation_entity varchar,
            prefix varchar,
            reference varchar,
            typology varchar,
            geojson varchar,
            geometry geometry null,
            point varchar
        )"""
    )
    cursor.execute(
        """
        CREATE TABLE old_entity (
            old_entity bigint,
            entry_date varchar,
            start_date varchar,
            end_date varchar,
            dataset varchar,
            notes varchar,
            status varchar,
            entity varchar null
        )"""
    )

    cursor.execute(
        """
        CREATE TABLE dataset (
        dataset varchar PRIMARY KEY,
        name varchar,
        entry_date varchar,
        start_date varchar,
        end_date varchar,
        collection varchar,
        description varchar,
        key_field varchar,
        paint_options varchar,
        plural varchar,
        prefix varchar,
        text varchar,
        typology varchar,
        wikidata varchar,
        wikipedia varchar,
        themes varchar,
        attribution_id varchar,
        licence_id varchar,
        consideration varchar,
        github_discussion varchar,
        entity_minimum varchar,
        entity_maximum varchar,
        phase varchar,
        realm varchar,
        version varchar,
        replacement_dataset varchar
        )"""
    )

    cursor.execute(
        """
        CREATE TABLE typology (
            typology varchar,
            name varchar,
            description varchar,
            entry_date varchar,
            start_date varchar,
            end_date varchar,
            plural varchar,
            text varchar,
            wikidata varchar,
            wikipedia varchar
        )"""
    )

    cursor.execute(
        """
        CREATE TABLE organisation (
        organisation varchar,
        name varchar,
        combined_authority varchar,
        entry_date varchar,
        start_date varchar,
        end_date varchar,
        entity varchar,
        local_authority_type varchar,
        official_name varchar,
        region varchar,
        statistical_geography varchar,
        website varchar
        )"""
    )

    cursor.execute(
        """
        CREATE TABLE dataset_collection (
            dataset_collection varchar,
            resource varchar,
            resource_end_date varchar,
            resource_entry_date varchar,
            last_updated varchar,
            last_collection_attempt varchar
        )"""
    )

    cursor.execute(
        """
        CREATE TABLE dataset_publication (
            dataset_publication varchar,
            expected_publisher_count varchar,
            publisher_count varchar
        )"""
    )

    cursor.execute(
        """
        CREATE TABLE lookup (
            id varchar,entity varchar,
            prefix varchar,
            reference varchar,
            entry_date varchar,
            start_date varchar,
            value varchar
        )"""
    )

    cursor.execute(
        """
        CREATE TABLE attribution (
            attribution varchar,
            text varchar,
            entry_date varchar,
            start_date varchar,
            end_date varchar
        )"""
    )

    cursor.execute(
        """
        CREATE TABLE licence (
            licence varchar,
            text varchar,
            entry_date varchar,
            start_date varchar,
            end_date varchar
        )"""
    )
    cursor.execute(
        """
            CREATE TABLE fact_resource (
                rowid BIGSERIAL PRIMARY KEY,
                fact VARCHAR(64) NOT NULL,
                resource VARCHAR(64) NOT NULL,
                entry_date DATE NOT NULL,
                entry_number INTEGER NOT NULL,
                priority INTEGER NOT NULL,
                reference_entity BIGINT,
                dataset VARCHAR(64) NOT NULL
            );
        """
    )

    cursor.execute(
        """
            CREATE TABLE fact (
                fact VARCHAR(64) PRIMARY KEY,
                entity INTEGER NOT NULL,
                field VARCHAR(64) NOT NULL,
                value TEXT NOT NULL,
                entry_date DATE NOT NULL,
                entry_number INTEGER NOT NULL,
                resource VARCHAR(64) NOT NULL,
                priority INTEGER,
                reference_entity BIGINT,
                dataset TEXT NOT NULL
            );
        """
    )

    cursor.execute(
        """
            CREATE TABLE issue (
                rowid BIGSERIAL PRIMARY KEY,
                entity BIGINT,
                entry_date DATE,
                entry_number INTEGER NOT NULL,
                field VARCHAR(64),
                issue_type VARCHAR(64),
                line_number INTEGER,
                dataset VARCHAR(64) NOT NULL,
                resource VARCHAR(64) NOT NULL,
                value TEXT,
                message TEXT
            )
        """
    )

    cursor.close()


@pytest.fixture(scope="function")
def platform_db_function_url(postgres, request):
    # instead of using db add some db name to the end of the string
    db_conn_url = postgres + "function"

    # create db
    if database_exists(db_conn_url):
        drop_database(db_conn_url)

    create_database(db_conn_url)
    db_conn = get_pg_connection(db_conn_url)
    create_no_fk_schema(db_conn)
    db_conn.commit()
    db_conn.close()

    def close_db():
        if database_exists(db_conn_url):
            drop_database(db_conn_url)

    # add finalizer to close db
    request.addfinalizer(close_db)
    return db_conn_url


@pytest.fixture(scope="session")
def platform_db_session_url(postgres, request):
    """
    Fixture to be used to create a database that lasts the whole session, conn shouldn't be  commited in tests
    """
    # instead of using db add some db name to the end of the string
    db_conn_url = postgres + "session"

    # create db
    if database_exists(db_conn_url):
        drop_database(db_conn_url)

    create_database(db_conn_url)
    db_conn = get_pg_connection(db_conn_url)
    create_no_fk_schema(db_conn)
    db_conn.commit()
    db_conn.close()

    def close_db():
        if database_exists(db_conn_url):
            drop_database(db_conn_url)

    # add finalizer to close db
    request.addfinalizer(close_db)
    return db_conn_url


@pytest.fixture(scope="function")
def platfom_db_session_conn(platform_db_session_url):
    """
    Fixture to get a connection to the session database that lasts for a function
    """
    conn = get_pg_connection(platform_db_session_url)
    yield conn
    conn.rollback()
    conn.close()
