from digital_land.phase.phase import Phase
import pandas as pd


class CheckPhase(Phase):
    def __init__(self, harmonised_path=None, issues=None, enabled=False):
        self.issues = issues
        self.harmonised_path = harmonised_path
        self.enabled = enabled

    def process(self, stream):
        print("path in check:", self.harmonised_path)
        csv_path = self.harmonised_path
        # csv_path = "test.csv"
        df = pd.read_csv(csv_path)
        df = df[df.duplicated(["reference", "end-date"])]
        print(df.head(5))
        yield from stream
