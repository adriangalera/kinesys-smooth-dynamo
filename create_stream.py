import boto3
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("stream-name", help="Stream name")
parser.add_argument("table", help="DynamoDB table where the data should be stored")
parser.add_argument("--batch", help="Put Records batch size")

stream_name = "test-multiple-shards"
client = boto3.client('kinesis')

try:
    stream = client.create_stream(StreamName=stream_name, ShardCount=5)
except Exception:
    pass
