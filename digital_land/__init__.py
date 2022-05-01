import sys
import csv


def csv_field_size_limit(field_size_limit=sys.maxsize):
    while True:
        try:
            csv.field_size_limit(field_size_limit)
            break
        except OverflowError:
            field_size_limit = int(field_size_limit / 10)


csv_field_size_limit()
