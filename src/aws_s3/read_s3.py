from boto3 import client
import os
import boto3
import json
from dotenv import load_dotenv
import pandas as pd
from io import StringIO
# TODO: Update, change in AWS
load_dotenv()

aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

# Correctly initialize the S3 resource using your credentials
s3_resource = boto3.resource('s3',
                             aws_access_key_id=aws_access_key_id,
                             aws_secret_access_key=aws_secret_access_key)
s3_client = boto3.client('s3',
                         aws_access_key_id=aws_access_key_id,
                         aws_secret_access_key=aws_secret_access_key)


def update_device_json(updated_dict):
    BUCKET = 'c2sr-config'
    FILE_KEY = "device-infra/device.json"
    obj = s3_resource.Object(BUCKET, FILE_KEY)
    obj.put(Body=json.dumps(updated_dict, indent=4).encode('utf-8'))


def get_device_json():
    BUCKET = 'c2sr-config'
    FILE_KEY = "device-infra/device.json"
    response = s3_client.get_object(Bucket=BUCKET, Key=FILE_KEY)
    content = response['Body'].read()
    json_data = json.loads(content)
    return json_data

def get_csv_for_stream(filename):
    BUCKET = 'c2sr-config'
    FILE_KEY = f"device-infra/{filename}.csv"
    response = s3_client.get_object(Bucket=BUCKET, Key=FILE_KEY)
    content = response['Body'].read().decode('utf-8')
    csv_data = pd.read_csv(StringIO(content))
    return csv_data

if __name__ == "__main__":
    df = get_csv_for_stream("494654")
    print(df)