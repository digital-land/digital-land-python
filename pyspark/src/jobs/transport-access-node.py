import sys
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql import SparkSession
import configparser
import os
from pyspark.sql.functions import first, collect_list, concat_ws
import yaml
 
def load_config(config_path="config.ini"):
    config = configparser.ConfigParser()
    config.read(config_path)
    return config['DEFAULT']

def create_spark_session(app_name="EMRToAurora"):
    return SparkSession.builder.appName(app_name).getOrCreate()

def load_metadata(yaml_path):
    with open(yaml_path, "r") as file:
        return yaml.safe_load(file)

PG_JDBC = config.get('DEFAULT', 'PG_JDBC')
PG_URL =  config.get('DEFAULT', 'PG_URL')
TABLE_NAME = config.get('DEFAULT', 'TABLE_NAME')
USER_NAME = config.get('DEFAULT', 'USER_NAME')
PASSWORD = config.get('DEFAULT', 'PASSWORD')
DRIVER= config.get('DEFAULT', 'DRIVER')
 
# Map string type names to PySpark types
def build_schema(attributes):
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

def read_data(spark, input_path):
    return spark.read.csv(input_path, header=True, inferSchema=True)

def transform_data(df):    
    # Define grouping columns (excluding 'field' and 'value')
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

def write_to_s3(df, output_path):    
# Convert entry-date to date type and extract year, month, day
    df = df.withColumn("entry_date", to_date("entry-date", "yyyy-MM-dd")) \
    .withColumn("year", year("entry_date")) \
    .withColumn("month", month("entry_date")) \
    .withColumn("day", dayofmonth("entry_date"))

# Write to S3 partitioned by year, month, day
    df.write \
    .partitionBy("year", "month", "day") \
    .mode("overwrite") \
    .option("header", "true") \
    .parquet(output_path)


##writing to postgres db
def write_to_postgres(df, config):
    df.write \
        .format(config['PG_JDBC']) \
        .option("url", config['PG_URL']) \
        .option("dbtable", config['TABLE_NAME']) \
        .option("user", config['USER_NAME']) \
        .option("password", config['PASSWORD']) \
        .option("driver", config['DRIVER']) \
        .mode("overwrite") \
        .save()

def main():
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
