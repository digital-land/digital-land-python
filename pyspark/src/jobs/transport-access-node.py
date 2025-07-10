from dataclasses import fields
from logging import config
import sys
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql import SparkSession
import configparser
import os
#from pyspark.sql.functions import first, collect_list, concat_ws
from pyspark.sql.functions import first, collect_list, concat_ws, expr, to_date, year, month, dayofmonth, coalesce

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
    config_path = os.path.normpath(os.path.join(current_dir, '..', '..', 'pyspark/config', 'aws.properties'))

    print(config_path)

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

#PG_JDBC = config.get('DEFAULT', 'PG_JDBC')
#TABLE_NAME = config.get('DEFAULT', 'TABLE_NAME')
#USER_NAME = config.get('DEFAULT', 'USER_NAME')
#PASSWORD = config.get('DEFAULT', 'PASSWORD')
#DRIVER= config.get('DEFAULT', 'DRIVER')
#PG_URL =  config.get('DEFAULT', 'PG_URL')

# -------------------- Schema Builder --------------------
# Map string type names to PySpark types
# def build_schema(attributes):
#     logger.info("Building schema from metadata attributes")
#     type_mapping = {
#         "StringType": StringType,
#         "IntegerType": IntegerType,
#         "DoubleType": DoubleType,
#         "TimestampType": TimestampType,
#         "BooleanType": BooleanType
#     } 

# Build StructType schema from YAML
    # schema = StructType([
    #     StructField(attr["name"], type_mapping[attr["type"]](), attr["nullable"])
    #     for attr in attributes
    # ])
    # return schema

# -------------------- Data Reader --------------------
def read_data(spark, input_path):
    logger.info(f"Reading data from {input_path}")
    return spark.read.csv(input_path, header=True, inferSchema=True)

# -------------------- Data Transformer --------------------
def transform_data(df):
    #pivoted_df = df.groupBy("entity").pivot("field", [
    #"start_date", "bus_stop_type", "transport_access_node_type", "point",
    #"name", "organisation", "reference", "prefix", "entry_date", "naptan_code"
    #]).agg(expr("max(value)"))

    # logger.info("Transforming data")  
    # # Define grouping columns (excluding 'field' and 'value')
    # #todo: need to add a generic method for this which works for all data
    # group_cols = ["end-date", "entity", "entry-date", "entry-number", "priority", "reference-entity", "resource", "start-date"]
    # # Pivot the data: each 'field' becomes a column, 'value' becomes the cell value
    # pivoted_df = df.groupBy(*group_cols).pivot("field").agg(first("value"))
    # # Concatenate all 'fact' values into a single string
    # fact_concat_df = df.groupBy(*group_cols).agg(concat_ws(",", collect_list("fact")).alias("fact"))
    # # Join the pivoted data with the concatenated fact values
    # final_df = fact_concat_df.join(pivoted_df, on=group_cols, how="inner")
    # # Show the result
    # final_df.show(truncate=False)    
    
    # Load YAML file
    with open("/mnt/c/Users/399182/MHCLG-Repo/digital-land-python/pyspark/config/transformed-source.yaml", "r") as file:
        yaml_data = yaml.safe_load(file)

    # Extract the list of fields
    fields = yaml_data.get("transport-access-node", [])
    
    # Replace hyphens with underscores in column names
    for col in df.columns:
        if "-" in col:
            df = df.withColumnRenamed(col, col.replace("-", "_"))

    # Get actual DataFrame columns
    df_columns = df.columns

    # Find fields that are present in both DataFrame and YAML    
    if set(fields) == set(df.columns):
        logger.info("All fields are present in the DataFrame")
    else:
        logger.warning("Some fields are missing from the DataFrame")
    
    return df

def populate_tables(df, table_name):
    with open("/mnt/c/Users/399182/MHCLG-Repo/digital-land-python/pyspark/config/transformed-target.yaml", "r") as file:
        yaml_data = yaml.safe_load(file)

    # Extract the list of fields
    fields = yaml_data.get(table_name, [])
    
    # Select only those columns from the DataFrame that are for fact_resource table
    df_selected = df[fields]
    df_selected.show(5)
    return df_selected


# -------------------- S3 Writer --------------------
def write_to_s3(df, output_path):   
    logger.info(f"Writing data to S3 at {output_path}") 
# Convert entry-date to date type and extract year, month, day
    #df.show()
    #spark.stop()clear
    df = df.withColumn("start_date_parsed", to_date(coalesce("start_date", "entry_date"), "yyyy-MM-dd")) \
    .withColumn("year", year("start_date_parsed")) \
    .withColumn("month", month("start_date_parsed")) \
    .withColumn("day", dayofmonth("start_date_parsed"))

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
    df = spark.read.option("header", "true").csv('/mnt/c/Users/399182/MHCLG-Repo/SourceFiles/AWS-S3/transform-access-node/*.csv')
    #df = read_data(spark,  config['S3_INPUT_PATH'])
    df.printSchema() 
    df.show()
    processed_df = transform_data(df)
    populate_tables(processed_df, 'transport-access-node-fact-res')
    # Show schema and sample data
    
    #write_to_postgres(processed_df, config)
    logger.info("Writing to output path")

    #write_to_s3(processed_df, '/mnt/c/users/desktop/lakshmi/spark-output/output-parquet')
    write_to_s3(processed_df, '/home/lakshmi/spark-output/output-parquet-fact-res')
    #write_to_s3(processed_df, config['S3_OUTPUT_PATH'])
    populate_tables(processed_df, 'transport-access-node-fact')
    write_to_s3(processed_df, '/home/lakshmi/spark-output/output-parquet-fact')
    spark.stop()

if __name__ == "__main__":
    main()
