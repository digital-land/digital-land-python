import boto3
import json
from botocore.exceptions import ClientError

def set_emr_secrets():
    secret_name = "airflow/emr_serverless"
    region_name = "eu-west-2"

    client = boto3.client("secretsmanager", region_name=region_name)

    try:
        response = client.get_secret_value(SecretId=secret_name)
        secret_value = json.loads(response["SecretString"])

        application_id = secret_value.get("application_id")
        execution_role_arn = secret_value.get("execution_role_arn")
        entry_point = secret_value.get("entry_point")
        log_uri = secret_value.get("log_uri")

        print("Retrieved EMR Serverless configuration from Secrets Manager:")
        print(f"application_id: {application_id}")
        print(f"execution_role_arn: {execution_role_arn}")
        print(f"entry_point: {entry_point}")
        print(f"log_uri: {log_uri}")

    except ClientError as e:
        print(f"Error retrieving secret '{secret_name}': {e}")

# Call the function immediately to set the secrets
set_emr_secrets()


# Example usage
# from set_emr_secrets import set_emr_secrets
# import boto3
# import json

# # Step 1: Call the function to retrieve and print the secrets
# set_emr_secrets()

# # Step 2: Retrieve the secrets again and assign to local variables
# def get_emr_config():
#     secret_name = "airflow/emr_serverless"
#     region_name = "eu-west-2"
#     client = boto3.client("secretsmanager", region_name=region_name)

#     response = client.get_secret_value(SecretId=secret_name)
#     secret_value = json.loads(response["SecretString"])

#     return (
#         secret_value.get("application_id"),
#         secret_value.get("execution_role_arn"),
#         secret_value.get("entry_point"),
#         secret_value.get("log_uri")
#     )

# # Step 3: Use the values
# application_id, execution_role_arn, entry_point, log_uri = get_emr_config()

# print("\nUsing EMR Serverless configuration:")
# print(f"application_id: {application_id}")
# print(f"execution_role_arn: {execution_role_arn}")
# print(f"entry_point: {entry_point}")
# print(f"log_uri: {log_uri}")
