"""Script that simulates a kinesis stream writer that will be executed in ECS environment
to smooth the DynamoDB traffic input"""

import boto3
import time
import json
import threading
import argparse

parser = argparse.ArgumentParser("Script that reads from a multi-sharded kinesis stream and stores in the DynamoDB")
parser.add_argument("stream_name", help="Stream name")
args = parser.parse_args()
stream_name = args.stream_name

# If we set a target WCU of 5 items/second
# This means that we will get roughly those numbers if the reader consume 5 items and rest 1 second
# However, we need to take into account that there's some offset applied because of the data processing
# (JSON deserialization) and some offset because of data gathering from the AWS Kinesis

# Since the target value will be a maximum value it is OK to use it to calculate the parameters
BATCH_SIZE = 5
SLEEP_TIME = 0.5
DYNAMO_DB_MAX_BATCH = 25


def set_parameters():
    print '**** Reading and setting parameters ****'
    with open("parameter.json", "r") as parameters:
        parameters_json = json.load(parameters)
        global BATCH_SIZE, SLEEP_TIME
        BATCH_SIZE = int(parameters_json["BATCH_SIZE"])
        SLEEP_TIME = float(parameters_json["SLEEP_TIME"])


set_parameters()

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
        table = None
        for user_batch in split_into_batches(users):
            items_db = []
            for user in user_batch:
                if not table:
                    table = user['table']
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
            batch_list.append({table: items_db})
        if threaded:
            threaded_insert(batch_list)
        else:
            sequential_insert(batch_list)

        return table


def split_into_batches(users):
    grouped = list(zip(*[iter(users)] * DYNAMO_DB_MAX_BATCH))
    non_batched = users[len(grouped) * DYNAMO_DB_MAX_BATCH:]
    if non_batched:
        grouped.append(non_batched)
    return grouped


def process_shard(shard_id):
    """Function that process a shard"""
    shard_it = kinesis_cli.get_shard_iterator(StreamName=stream_name, ShardId=shard_id, ShardIteratorType='LATEST')[
        'ShardIterator']
    iterations = 0
    while True:
        try:
            iterations += 1
            # Obtain the records from the Kinesis stream shard
            time_get_records_1 = time.time()
            out = kinesis_cli.get_records(ShardIterator=shard_it, Limit=BATCH_SIZE)
            records = out["Records"]
            time_get_records_2 = time.time()
            gather_time = time_get_records_2 - time_get_records_1
            # Process the data
            users = []
            tjson1 = time.time()
            for record in records:
                users.append(json.loads(record['Data']))
            tjson2 = time.time()
            tjson = tjson2 - tjson1

            # Store the obtained data in DynamoDB by batches (one thread per batch for max performance)
            dynamo_time_1 = time.time()
            table = store_users_batching(users, threaded=True)
            dynamo_time_2 = time.time()
            dynamo_time = dynamo_time_2 - dynamo_time_1

            # Get next shard iterator
            shard_it = out['NextShardIterator']

            # Print debugging info
            loop_time = SLEEP_TIME + gather_time + +tjson + dynamo_time
            wcu_output = len(records) / loop_time
            info_tup = (table, len(records), loop_time, wcu_output)
            if records:
                print "[Table %s] Insert %d records in %s seconds. WCU = %f" % info_tup
                time.sleep(SLEEP_TIME)
            else:
                # Make sure to sleep even when there are no records, otherwise the provisioned throughput of the shard
                # will be consumed instantaneously
                time.sleep(1)

            if iterations % 10 == 0:
                set_parameters()
        except Exception, err:
            print err
            pass


# Create one thread per shard. The data is partitioned in the stream by its table,
# so all the data inside a shard will be stored into the same dynamo DB table
shard_list = kinesis_cli.describe_stream(StreamName=stream_name)['StreamDescription']['Shards']
shard_threads = []
for shard in shard_list:
    shard_id = shard['ShardId']
    print "Creating worker for shard %s ..." % shard_id
    th = threading.Thread(target=process_shard, args=(shard_id,))
    th.daemon = True
    shard_threads.append(th)

for shard_thread in shard_threads:
    shard_thread.start()

while True:
    time.sleep(1)
