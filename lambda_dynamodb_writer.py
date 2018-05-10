"""This script contain the code that a lambda runs in stream mode within a kinesis stream"""

import base64
import json
import boto3

client = boto3.client('dynamodb')


def lambda_handler(event, context):
    records = event["Records"]
    for record in records:
        user = json.loads(base64.b64decode(record['kinesis']['data']))
        user_id = user['firstname']
        user_age = user['age']
        response = client.put_item(
            TableName='kinesis-experiment',
            Item={
                'id': {
                    'S': user_id,
                },
                'age': {
                    'S': str(user_age)
                }
            }
        )
        print response
