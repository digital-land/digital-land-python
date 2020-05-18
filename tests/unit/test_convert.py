import os
from digital_land import load


def test_load_xlsm():
    path = os.path.join(os.getcwd(), "tests/data/brentwood.xlsm")
    iterator = load.load(path)
    iter(iterator)
