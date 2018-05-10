import boto3

stream_name = "test-streaming"
client = boto3.client('kinesis')

try:
    stream = client.create_stream(StreamName=stream_name, ShardCount=1)
except Exception:
    pass
print client.describe_stream(StreamName=stream_name)
print client.list_streams()
