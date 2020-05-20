import responses
import hashlib
from datetime import datetime
from digital_land.collect import Collector, FetchStatus


class SpySaver:
    def __init__(self):
        self.content = ""
        self.log = ""
        self.log_path = ""

    def save_content(self, content):
        self.content = content.decode("utf-8")
        return

    def save_log(self, path, log):
        self.log = log
        self.log_path = path


@responses.activate
def test_fetch():
    collector = Collector()
    saver = SpySaver()
    collector.save_content = saver.save_content
    collector.save_log = saver.save_log

    responses.add(responses.GET, "http://some.url", body="some data")

    status = collector.fetch("http://some.url")

    assert status == FetchStatus.OK
    assert saver.content == "some data"
    assert (
        saver.log_path
        == f"collection/log/{datetime.now().strftime('%Y-%m-%d')}/{hashlib.sha256('http://some.url'.encode('utf-8')).hexdigest()}.json"
    )
