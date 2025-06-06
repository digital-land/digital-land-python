import logging


from .package import Package


from digital_land.utils.postgres_utils import get_pg_connection


logger = logging.getLogger(__name__)


class PostgresPackage(Package):
    """
    A pckage class for data that is contained in a postgresql database.
    """

    def __init__(self, database_url=None, conn=None):
        # TODO alter init to use parent propeties but need to figure out what to do about the spec
        self.conn = None
        self.database_url = database_url

    def connect(self):
        """create a connection to the postgres database"""

        if self.conn:
            logger.info("Connection already exists, skipping creation.")
            return

        if not self.database_url:
            raise ValueError(
                "Database URL must be set in oder to connect to the database. Please set the 'database_url' attribute."
            )

        self.connection = get_pg_connection(self.database_url)

    def _get_conn(self):
        """get the connection to the postgres database"""
        if not self.conn:
            raise ValueError(
                "Connection not established. Please call connect() first or provide a connection"
            )
        return self.conn
