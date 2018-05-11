import boto3
import argparse

parser = argparse.ArgumentParser("Describe the shards of a given stream")
parser.add_argument("stream_name", help="Stream name")
args = parser.parse_args()

stream_name = args.stream_name
client = boto3.client('kinesis')
shards = client.describe_stream(StreamName=stream_name)['StreamDescription']['Shards']
for shard in shards:
    print shard
