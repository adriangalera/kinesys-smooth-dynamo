"""Script that simulates a kinesis stream writer that will be executed in ECS environment
to smooth the DynamoDB traffic input"""

import boto3
import time
import json
import threading

# Target WCU = 5 items/second
# This means that we will get roughly those numbers if the reader consume 5 items and rest 1 second
BATCH_SIZE = 5
SLEEP_TIME = 0.5
TABLE_NAME = "kinesis-experiment-2"
DYNAMO_DB_MAX_BATCH = 25

from botocore.config import Config


def set_parameters():
    print '**** Reading and setting parameters ****'
    with open("parameter.json", "r") as parameters:
        parameters_json = json.load(parameters)
        global BATCH_SIZE, SLEEP_TIME
        BATCH_SIZE = int(parameters_json["BATCH_SIZE"])
        SLEEP_TIME = float(parameters_json["SLEEP_TIME"])


set_parameters()
stream_name = "test-streaming"
kinesis_cli = boto3.client('kinesis')
dynamodb_cli = boto3.client('dynamodb')


def store_users_batching(users, threaded=False):
    """Split the user list into batches of 25, later insert them sequentially.

    If more performance is required, each batch can be inserted in a separate thread

    """

    def sequential_insert(batches):
        """Insert the batches one after the other"""
        for batch in batches:
            try:
                response = dynamodb_cli.batch_write_item(RequestItems=batch)
                while response['UnprocessedItems']:
                    response = dynamodb_cli.batch_write_item(RequestItems=response['UnprocessedItems'])
            except Exception, err:
                print err
                pass

    def threaded_insert(batches):
        """Insert the batches simultaneously (one thread per batch)"""
        threads = []
        for batch in batches:
            th = threading.Thread(target=sequential_insert, args=([batch],))
            threads.append(th)
        for thread in threads:
            thread.start()
            thread.join()

    if users:
        batch_list = []
        for user_batch in split_into_batches(users):
            items_db = []
            for user in user_batch:
                user_id = user['firstname']
                user_age = user['age']
                item = {
                    'id': {
                        'S': user_id,
                    },
                    'age': {
                        'S': str(user_age)
                    }
                }
                dyn_batch_item = {'PutRequest': {'Item': item}}
                items_db.append(dyn_batch_item)
            batch_list.append({TABLE_NAME: items_db})
        if threaded:
            threaded_insert(batch_list)
        else:
            sequential_insert(batch_list)


def split_into_batches(users):
    grouped = list(zip(*[iter(users)] * DYNAMO_DB_MAX_BATCH))
    non_batched = users[len(grouped) * DYNAMO_DB_MAX_BATCH:]
    if non_batched:
        grouped.append(non_batched)
    return grouped


shard_id = 'shardId-000000000000'  # we only have one shard!
shard_it = kinesis_cli.get_shard_iterator(StreamName=stream_name, ShardId=shard_id, ShardIteratorType='LATEST')[
    'ShardIterator']
iterations = 0
while True:
    iterations += 1
    time_get_records_1 = time.time()
    out = kinesis_cli.get_records(ShardIterator=shard_it, Limit=BATCH_SIZE)
    time_get_records_2 = time.time()
    gather_time = time_get_records_2 - time_get_records_1
    records = out["Records"]

    users = []
    tjson1 = time.time()
    for record in records:
        users.append(json.loads(record['Data']))
    tjson2 = time.time()
    tjson = tjson2 - tjson1
    dynamo_time_1 = time.time()
    store_users_batching(users, threaded=True)
    dynamo_time_2 = time.time()
    shard_it = out['NextShardIterator']
    dynamo_time = dynamo_time_2 - dynamo_time_1
    loop_time = SLEEP_TIME + gather_time + +tjson + dynamo_time
    wcu_output = len(records) / loop_time
    # print gather_time, tjson, dynamo_time
    print "Insert %d records in %s seconds. Writing Items/seconds: %f" % (len(records), loop_time, wcu_output)
    if records:
        time.sleep(SLEEP_TIME)
    else:
        time.sleep(1)

    if iterations % 10 == 0:
        set_parameters()
