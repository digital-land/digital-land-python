
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
