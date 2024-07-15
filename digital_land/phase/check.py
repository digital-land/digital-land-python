from digital_land.phase.phase import Phase
import pandas as pd


class CheckPhase(Phase):
    def __init__(self, issues=None):
        self.issues = issues

    def process(self, stream):
        df = pd.DataFrame.from_records([block["row"] for block in stream])
        df = df[df.duplicated(["reference", "end-date"])]
        df.to_csv("test.csv")
        yield from stream
