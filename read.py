import boto3
import time

stream_name = "test-streaming"
client = boto3.client('kinesis')
shard_id = 'shardId-000000000000'  # we only have one shard!
shard_it = client.get_shard_iterator(StreamName=stream_name, ShardId=shard_id, ShardIteratorType='LATEST')[
    'ShardIterator']
while True:
    out = client.get_records(ShardIterator=shard_it, Limit=2)
    records = out["Records"]
    for record in records:
        print record['Data']
    shard_it = out['NextShardIterator']
    time.sleep(0.2)
    if records:
        print "****************"
