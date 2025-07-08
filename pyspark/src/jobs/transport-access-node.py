from logging import config
import sys
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql import SparkSession
import configparser
import os
from pyspark.sql.functions import first, collect_list, concat_ws
import yaml
import logging
from logging.config import dictConfig


# -------------------- Logging Configuration --------------------
LOGGING_CONFIG = {
    "version": 1,
    "formatters": {
        "default": {
            "format": "[%(asctime)s] %(levelname)s - %(message)s",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "default",
            "level": "INFO",
        },
    },
    "root": {
        "handlers": ["console"],
        "level": "INFO",
    },
}

dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)
 
# -------------------- Configuration Loader --------------------

#laod this for aws.properties 
def load_config():
    """
    Loads AWS configuration from pyspark/config/aws.properties
    relative to this script's location.
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.normpath(os.path.join(current_dir, '..', '..', 'config', 'aws.properties'))

    config = configparser.ConfigParser()
    config.read(config_path)

    logger.info(f"Loaded configuration from {config_path}")
    return config['DEFAULT']

# -------------------- Spark Session --------------------
def create_spark_session(app_name="EMRToAurora"):
    logger.info(f"Creating Spark session with app name: {app_name}")
    return SparkSession.builder.appName(app_name).getOrCreate()

# -------------------- Metadata Loader --------------------
def load_metadata(yaml_path):
    logger.info(f"Loading metadata from {yaml_path}")
    with open(yaml_path, "r") as file:
        return yaml.safe_load(file)

PG_JDBC = config.get('DEFAULT', 'PG_JDBC')
PG_URL =  config.get('DEFAULT', 'PG_URL')
TABLE_NAME = config.get('DEFAULT', 'TABLE_NAME')
USER_NAME = config.get('DEFAULT', 'USER_NAME')
PASSWORD = config.get('DEFAULT', 'PASSWORD')
DRIVER= config.get('DEFAULT', 'DRIVER')

# -------------------- Schema Builder --------------------
# Map string type names to PySpark types
def build_schema(attributes):
    logger.info("Building schema from metadata attributes")
    type_mapping = {
        "StringType": StringType,
        "IntegerType": IntegerType,
        "DoubleType": DoubleType,
        "TimestampType": TimestampType,
        "BooleanType": BooleanType
    } 
# Build StructType schema from YAML
    schema = StructType([
        StructField(attr["name"], type_mapping[attr["type"]](), attr["nullable"])
        for attr in attributes
    ])
    return schema

# -------------------- Data Reader --------------------
def read_data(spark, input_path):
    logger.info(f"Reading data from {input_path}")
    return spark.read.csv(input_path, header=True, inferSchema=True)

# -------------------- Data Transformer --------------------
def transform_data(df):  
    logger.info("Transforming data")  
    # Define grouping columns (excluding 'field' and 'value')
    #todo: need to add a generic method for this which works for all data
    group_cols = ["end-date", "entity", "entry-date", "entry-number", "priority", "reference-entity", "resource", "start-date"]
    # Pivot the data: each 'field' becomes a column, 'value' becomes the cell value
    pivoted_df = df.groupBy(*group_cols).pivot("field").agg(first("value"))
    # Concatenate all 'fact' values into a single string
    fact_concat_df = df.groupBy(*group_cols).agg(concat_ws(",", collect_list("fact")).alias("fact"))
    # Join the pivoted data with the concatenated fact values
    final_df = fact_concat_df.join(pivoted_df, on=group_cols, how="inner")
    # Show the result
    final_df.show(truncate=False)
    return final_df

# -------------------- S3 Writer --------------------
def write_to_s3(df, output_path):   
    logger.info(f"Writing data to S3 at {output_path}") 
# Convert entry-date to date type and extract year, month, day
    df = df.withColumn("start_date", to_date("start_date", "yyyy-MM-dd")) \
    .withColumn("year", year("start_date")) \
    .withColumn("month", month("start_date")) \
    .withColumn("day", dayofmonth("start_date"))


# -------------------- PostgreSQL Writer --------------------
# Write to S3 partitioned by year, month, day
    df.write \
    .partitionBy("year", "month", "day") \
    .mode("overwrite") \
    .option("header", "true") \
    .parquet(output_path)

##writing to postgres db
def write_to_postgres(df, config):
    logger.info(f"Writing data to PostgreSQL table {config['TABLE_NAME']}")
    df.write \
        .format(config['PG_JDBC']) \
        .option("url", config['PG_URL']) \
        .option("dbtable", config['TABLE_NAME']) \
        .option("user", config['USER_NAME']) \
        .option("password", config['PASSWORD']) \
        .option("driver", config['DRIVER']) \
        .mode("overwrite") \
        .save()
# -------------------- Main --------------------
def main():
    logger.info("Starting main ETL process")
    config = load_config()
    spark = create_spark_session()
    #metadata = load_metadata("main/Spark-Job-Script/entity-metadata/meta-transport-access-node.yaml")
    # Choose the entity you want to load
    #entity_name = "transport-access-node"
    #attributes = metadata[entity_name]["attributes"]
    #schema = build_schema(attributes)    

    # Read CSV using the dynamic schema
    df = read_data(spark,  config['S3_INPUT_PATH'])
    df.printSchema() 
    processed_df = transform_data(df)
    # Show schema and sample data
    processed_df.show()
    #write_to_postgres(processed_df, config)
    write_to_s3(processed_df, config['S3_OUTPUT_PATH'])
    spark.stop()

if __name__ == "__main__":
    main()
