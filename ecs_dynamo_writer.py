"""Script that simulates a kinesis stream writer that will be executed in ECS environment
to smooth the DynamoDB traffic input"""

import boto3
import time
import json

# Target WCU = 5 items/second
# This means that we will get roughly those numbers if the reader consume 5 items and rest 1 second
BATCH_SIZE = 5
SLEEP_TIME = 0.5
TABLE_NAME = "kinesis-experiment-2"


def set_parameters():
    print '**** Reading and setting parameters ****'
    with open("parameter.json", "r") as parameters:
        parameters_json = json.load(parameters)
        global BATCH_SIZE, SLEEP_TIME
        BATCH_SIZE = int(parameters_json["BATCH_SIZE"])
        SLEEP_TIME = float(parameters_json["SLEEP_TIME"])


stream_name = "test-streaming"
kinesis_cli = boto3.client('kinesis')
dynamodb_cli = boto3.client('dynamodb')

shard_id = 'shardId-000000000000'  # we only have one shard!
shard_it = kinesis_cli.get_shard_iterator(StreamName=stream_name, ShardId=shard_id, ShardIteratorType='LATEST')[
    'ShardIterator']
iterations = 0
while True:
    iterations += 1
    out = kinesis_cli.get_records(ShardIterator=shard_it, Limit=BATCH_SIZE)
    records = out["Records"]
    for record in records:
        user = json.loads(record['Data'])
        user_id = user['firstname']
        user_age = user['age']
        response = dynamodb_cli.put_item(
            TableName=TABLE_NAME,
            Item={
                'id': {
                    'S': user_id,
                },
                'age': {
                    'S': str(user_age)
                }
            }
        )
        #print response
    shard_it = out['NextShardIterator']
    time.sleep(SLEEP_TIME)
    print iterations
    if iterations % 10 == 0:
        set_parameters()
