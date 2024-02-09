from .dataset import DatasetCheckpoint
from . import checkpoint
import functools


@checkpoint("post-load")
class FactChecksCheckpoint(DatasetCheckpoint):
    def __init__(self, *args):
        super().__init__(*args)

    def load(self):
        self.expectations = [
            functools.partial(FactChecksCheckpoint.check_multiple_orgs, self),
        ]

    def check_multiple_orgs(self):
        rows = self.connection.execute(
            "SELECT entity,value from fact f1 WHERE field = 'organisation' AND EXISTS "
            "(SELECT * FROM fact f2 WHERE f1.entity = f2.entity AND f1.field = f2.field AND f1.value != f2.value) "
            "ORDER BY entity"
        ).fetchall()

        if len(rows) == 0:
            return (
                True,
                f"No facts exist for the same entity but more than one organisation.",
                [],
            )
        else:
            return (
                False,
                f"{len(rows)} facts exist for the same entity but more than one organisation.",
                [],
            )
