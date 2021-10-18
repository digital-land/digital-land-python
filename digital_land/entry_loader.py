import logging

from .model.entry import Entry

logger = logging.getLogger(__name__)


class EntryLoader:
    def __init__(self, repo):
        self.repo = repo

    def load(self, reader):
        for stream_data in reader:
            self.load_entry(
                stream_data["row"], stream_data["resource"], stream_data["line_num"]
            )

    def load_entry(self, data, resource, line_num):
        if not resource or not line_num:
            raise ValueError("entry missing resource or line_num")

        if not data["entity"]:
            logger.warning(
                "skipping entry at %s:%s due to missing entity", resource, line_num
            )
            return

        entry = Entry(data, resource, line_num)
        self.repo.add(entry)
