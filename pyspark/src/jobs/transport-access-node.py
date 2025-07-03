import sys
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql import SparkSession
import configparser
import os
 
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
    return df.dropna()

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


def write_to_parquet(df, output_path):    
    df.write.mode("overwrite").parquet(output_path)

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
    write_to_parquet(processed_df, config['S3_OUTPUT_PATH'])
    spark.stop()

if __name__ == "__main__":
    main()
