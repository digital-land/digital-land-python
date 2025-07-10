
import sqlite3

# Connect to the database
conn = sqlite3.connect('/home/lakshmi/spark-output/output-sqlite/transport_access_node.db')
cursor = conn.cursor()

# List tables
#cursor.execute("SELECT name FROM fact_resource WHERE type='table';")
print(cursor.fetchall())

# View contents of a specific table
cursor.execute("SELECT * FROM fact_resource;")
rows = cursor.fetchall()
for row in rows:
    print(row)

conn.close()
