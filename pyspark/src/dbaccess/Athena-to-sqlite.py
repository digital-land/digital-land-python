import logging
from logging.config import dictConfig
from pyspark.sql import SparkSession

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

# Start Spark session with JDBC driver
spark = SparkSession.builder \
    .appName("AthenaToSQLite") \
    .config("spark.jars", "/path/to/sqlite-jdbc-<version>.jar") \
    .getOrCreate()

# Load data from Athena (or S3)
df = spark.read.format("csv").option("header", "true").load("s3://your-athena-output.csv")

# Write to SQLite using JDBC
df.write \
    .format("jdbc") \
    .option("url", "jdbc:sqlite:/path/to/output.db") \
    .option("dbtable", "your_table") \
    .option("driver", "org.sqlite.JDBC") \
    .mode("overwrite") \
    .save()
