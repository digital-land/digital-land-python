import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from main.sparkjob.Script.spark.transport-access-node 
import build_schema, transform_data  # Replace with your actual module name

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("PySparkTest").getOrCreate()

def test_build_schema_transport_access_node():
    attributes = [
        {"name": "ATCOCode", "type": "STRING", "nullable": False},
        {"name": "BusStopType", "type": "STRING", "nullable": True},
        {"name": "CommonName", "type": "STRING", "nullable": True},
        {"name": "CreationDateTime", "type": "STRING", "nullable": False},
        {"name": "Easting", "type": "STRING", "nullable": True},
        {"name": "Latitude", "type": "STRING", "nullable": True},
        {"name": "Longitude", "type": "STRING", "nullable": False},
        {"name": "NaptanCode", "type": "STRING", "nullable": True},
        {"name": "Northing", "type": "STRING", "nullable": True},
        {"name": "Notes", "type": "STRING", "nullable": True},
        {"name": "StopType", "type": "STRING", "nullable": True}
    ]

    # Normalize type names to match your build_schema logic
    for attr in attributes:
        attr["type"] = attr["type"].capitalize() + "Type"

    schema = build_schema(attributes)

    expected_fields = [
        StructField("ATCOCode", StringType(), False),
        StructField("BusStopType", StringType(), True),
        StructField("CommonName", StringType(), True),
        StructField("CreationDateTime", StringType(), False),
        StructField("Easting", StringType(), True),
        StructField("Latitude", StringType(), True),
        StructField("Longitude", StringType(), False),
        StructField("NaptanCode", StringType(), True),
        StructField("Northing", StringType(), True),
        StructField("Notes", StringType(), True),
        StructField("StopType", StringType(), True)
    ]

    expected_schema = StructType(expected_fields)
    assert schema == expected_schema

def test_transform_data_removes_nulls(spark):
    data = [
        ("123", "TimingPoint", "Main Street", "2023-01-01", "123456", "51.5074", "-0.1278", "123ABC", "654321", "Note", "BCT"),
        (None, "TimingPoint", "Main Street", "2023-01-01", "123456", "51.5074", "-0.1278", "123ABC", "654321", "Note", "BCT")
    ]
    schema = StructType([
        StructField("ATCOCode", StringType(), False),
        StructField("BusStopType", StringType(), True),
        StructField("CommonName", StringType(), True),
        StructField("CreationDateTime", StringType(), False),
        StructField("Easting", StringType(), True),
        StructField("Latitude", StringType(), True),
        StructField("Longitude", StringType(), False),
        StructField("NaptanCode", StringType(), True),
        StructField("Northing", StringType(), True),
        StructField("Notes", StringType(), True),
        StructField("StopType", StringType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    from your_module import transform_data  # Replace with your actual module name
    cleaned_df = transform_data(df)
    assert cleaned_df.count() == 1
