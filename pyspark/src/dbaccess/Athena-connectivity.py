
import boto3
import time

athena = boto3.client('athena', region_name='your-region')
# are we going to insert in same table or everyday different table

def run_athena_query(query, database, output_location):
    response = athena.start_query_execution(
 QueryString=query,
QueryExecutionContext={'Database': database},
ResultConfiguration={'OutputLocation': output_location}
 )
    query_execution_id = response['QueryExecutionId']

    while True:
        result = athena.get_query_execution(QueryExecutionId=query_execution_id)
    state = result['QueryExecution']['Status']['State']
    if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
        break
        time.sleep(2)

if state != 'SUCCEEDED':
    raise Exception(f"Athena query failed: {state}")

print(f"Query succeeded: {query_execution_id}")
