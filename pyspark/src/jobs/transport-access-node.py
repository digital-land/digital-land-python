
import configparser
import logging
import os
import sqlite3
import sys
from dataclasses import fields
from logging import config
from logging.config import dictConfig
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import (coalesce,collect_list,concat_ws,dayofmonth,expr,first,month,to_date,year)
from pyspark.sql.types import (StringType,StructField,StructType,TimestampType)

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
    try:
        logger.info(f"Creating Spark session with app name: {app_name}")
        return SparkSession.builder.appName(app_name) \
            .config("spark.jars", "/home/MHCLG-Repo/sqlite-jdbc-3.36.0.3.jar") \
            .getOrCreate()
    
    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}", exc_info=True)
        return None


# -------------------- Metadata Loader --------------------
def load_metadata(yaml_path):
    logger.info(f"Loading metadata from {yaml_path}")
    with open(yaml_path, "r") as file:
        return yaml.safe_load(file)


# -------------------- Data Reader --------------------
def read_data(spark, input_path):
    logger.info(f"Reading data from {input_path}")
    return spark.read.csv(input_path, header=True, inferSchema=True)

# -------------------- Data Transformer --------------------
def transform_data(df):      
    
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
    df.drop("start_date_parsed")
# -------------------- PostgreSQL Writer --------------------
    #Write to S3 partitioned by year, month, day
    df.write \
    .partitionBy("year", "month", "day") \
    .mode("overwrite") \
    .option("header", "true") \
    .parquet(output_path)
    df.write \
    .partitionBy("year", "month", "day") \
    .mode("append") \
    .option("header", "true") \
    .csv(output_path)

def generate_sqlite(df):
    # Step 4: Write to SQLite
    # Write to SQLite using JDBC
    try:
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:sqlite:/home/lakshmi/spark-output/output-sqlite/transport_access_node.db") \
            .option("dbtable", "fact_resource") \
            .option("driver", "org.sqlite.JDBC") \
            .mode("overwrite") \
            .save()
        logger.info('sqlite data inserted successfully')

    except Exception as e:
        logger.error(f"Failed to write to SQLite: {e}", exc_info=True)

# -------------------- PostgreSQL Writer --------------------

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
    try:
        logger.info("Starting main ETL process")
        config = load_config()
        spark = create_spark_session()     

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
        generate_sqlite(processed_df)
        write_to_s3(processed_df, '/home/lakshmi/spark-output/output-parquet-fact-res')
        populate_tables(processed_df, 'transport-access-node-fact')
        write_to_s3(processed_df, '/home/lakshmi/spark-output/output-parquet-fact')
    
    except Exception as e:
        logger.exception("An error occurred during the ETL process: %s", str(e))
    finally:
        try:
            spark.stop()
        except Exception as e:
            logger.info("Spark session stopped.")
        except Exception as stop_err:
            logger.exception("An error occurred while stopping Spark: %s", str(e))

if __name__ == "__main__":
    main()
