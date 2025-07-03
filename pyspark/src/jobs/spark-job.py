import sys
from pyspark.sql import SparkSession
import configparser
import os

PG_JDBC = config.get('DEFAULT', 'PG_JDBC')
PG_URL = config.get('DEFAULT', 'PG_URL')
TABLE_NAME = config.get('DEFAULT', 'TABLE_NAME')
USER_NAME = config.get('DEFAULT', 'USER_NAME')
PASSWORD = config.get('DEFAULT', 'PASSWORD')
DRIVER= config.get('DEFAULT', 'DRIVER')
 
spark = SparkSession.builder.appName("EMRToAurora").getOrCreate()
 
df = spark.read.csv(input_path, header=True, inferSchema=True)
processed_df = df.dropna()  # Example transformation
 
processed_df.write \
    .format(PG_JDBC) \
    .option("url", PG_URL) \
    .option("dbtable", TABLE_NAME) \
    .option("user", USER_NAME) \
    .option("password", PASSWORD) \
    .option("driver", "DRIVER") \
    .mode("overwrite") \
    .save()
 
spark.stop()