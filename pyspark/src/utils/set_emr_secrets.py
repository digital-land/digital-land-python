import boto3
import json
import os
from botocore.exceptions import ClientError

def set_emr_secrets():
    secret_name = "airflow/emr_serverless"
    region_name = "eu-west-2"

    # Collect secret values from environment variables
    secret_value = {
        "application_id": os.getenv("EMR_APPLICATION_ID"),
        "execution_role_arn": os.getenv("EMR_EXECUTION_ROLE_ARN"),
        "entry_point": os.getenv("EMR_ENTRY_POINT"),
        "log_uri": os.getenv("EMR_LOG_URI")
    }

    # Check for missing values
    missing = [k for k, v in secret_value.items() if not v]
    if missing:
        raise ValueError(f"Missing environment variables: {', '.join(missing)}")

    # Create Secrets Manager client
    client = boto3.client("secretsmanager", region_name=region_name)

    try:
        client.create_secret(
            Name=secret_name,
            SecretString=json.dumps(secret_value)
        )
        print(f"Secret '{secret_name}' created successfully.")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceExistsException":
            client.update_secret(
                SecretId=secret_name,
                SecretString=json.dumps(secret_value)
            )
            print(f"Secret '{secret_name}' updated successfully.")
        else:
            raise

# Example usage
# import boto3
# import json
# from botocore.exceptions import ClientError

# def get_emr_secrets():
#     secret_name = "airflow/emr_serverless"
#     region_name = "eu-west-2"

#     client = boto3.client("secretsmanager", region_name=region_name)

#     try:
#         response = client.get_secret_value(SecretId=secret_name)
#         secret = json.loads(response["SecretString"])
#         return secret
#     except ClientError as e:
#         print(f"Error retrieving secret: {e}")
#         return None

# # Example usage
# if __name__ == "__main__":
#     secrets = get_emr_secrets()
#     if secrets:
#         print("Retrieved EMR Serverless Secrets:")
#         for key, value in secrets.items():
#             print(f"{key}: {value}")
