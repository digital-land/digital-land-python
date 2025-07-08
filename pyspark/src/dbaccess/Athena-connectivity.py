
import boto3
import time
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

# -------------------- Athena Client --------------------
athena = boto3.client('athena', region_name='your-region')


# -------------------- Query Runner --------------------
# are we going to insert in same table or everyday different table

def run_athena_query(query, database, output_location):
    
    logger.info("Starting Athena query execution")

    response = athena.start_query_execution(
 QueryString=query,
QueryExecutionContext={'Database': database},
ResultConfiguration={'OutputLocation': output_location}
 )
    query_execution_id = response['QueryExecutionId']
    
    logger.info(f"Query submitted. Execution ID: {query_execution_id}")


    while True:
        result = athena.get_query_execution(QueryExecutionId=query_execution_id)
    state = result['QueryExecution']['Status']['State']
    
    logger.info(f"Query state: {state}")

    if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
        break
        time.sleep(2)

if state != 'SUCCEEDED':
    raise Exception(f"Athena query failed: {state}")
    logger.info(f"Query succeeded: {query_execution_id}")
    return query_execution_id
