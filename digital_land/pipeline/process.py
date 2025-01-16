#!/usr/bin/env python
import os
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path

# load in specification


# TODO need to take in the correct data types for the columns
def convert_tranformed_csv_to_pq(input_path, output_path):
    """
    function to convert a transformed resource to a parrquet file.
    """
    input_path = Path(input_path)
    output_path = Path(output_path)
    if output_path.exists():
        os.remove(output_path)

    # Define the chunk size for reading the CSV file
    chunk_size = 1000000  # Number of rows per chunk

    # expand on column names
    # Open a CSV reader with PyArrow
    # csv_reader = pv.open_csv(input_path, read_options=pv.ReadOptions(block_size=chunk_size))
    csv_iterator = pd.read_csv(
        input_path,
        chunksize=chunk_size,
        dtype={
            "entity": int,
            **{
                col: str
                for col in pd.read_csv(input_path, nrows=1).columns
                if col != "entity"
            },
        },
        na_filter=False,
    )

    # Initialize the Parquet writer with the schema from the first chunk
    first_chunk = next(csv_iterator)
    # size = 0
    # size +=len(first_chunk)

    fields = [
        ("end-date", pa.string()),
        ("entity", pa.string()),
        ("entry-date", pa.string()),
        ("entry-number", pa.string()),
        ("fact", pa.string()),
        ("field", pa.string()),
        ("priority", pa.string()),
        ("reference-entity", pa.string()),
        ("resource", pa.string()),
        ("start-date", pa.string()),
        ("value", pa.string()),
    ]
    schema = pa.schema(fields)
    table = pa.Table.from_pandas(first_chunk, schema=schema)

    # rename columns for parquet files to make querying easier in s3
    # Replace '-' with '_' in column names
    new_column_names = [name.replace("-", "_") for name in table.column_names]
    table = table.rename_columns(new_column_names)

    # Create a Parquet writer
    parquet_writer = pq.ParquetWriter(output_path, table.schema)

    # Write the first chunk
    parquet_writer.write_table(table)

    # Process and write the remaining chunks
    while True:
        try:
            chunk = next(csv_iterator)
            table = pa.Table.from_pandas(chunk, schema=schema)
            new_column_names = [name.replace("-", "_") for name in table.column_names]
            table = table.rename_columns(new_column_names)
            parquet_writer.write_table(table)
            # size += len(chunk)
        except StopIteration:
            break

    # Close the Parquet writer
    parquet_writer.close()
    # print(size)
