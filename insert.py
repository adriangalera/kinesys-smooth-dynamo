import boto3
import testdata
import json
import time
import argparse

stream_name = "test-multiple-shards"
client = boto3.client('kinesis')

parser = argparse.ArgumentParser()
parser.add_argument("items", help="Number of items to generate / seconds")
parser.add_argument("table", help="DynamoDB table where the data should be stored")
parser.add_argument("sleep", help="Sleep seconds between sends")

args = parser.parse_args()
items = int(args.items)
table = args.table
batch_size = 5
sleep_time = float(args.sleep)


class Users(testdata.DictFactory):
    firstname = testdata.FakeDataFactory('firstName')
    lastname = testdata.FakeDataFactory('lastName')
    age = testdata.RandomInteger(100, 300)
    gender = testdata.RandomSelection(['female', 'male'])


records = items
while True:
    users = []
    gen_records = []
    i = 0
    for user in Users().generate(records):
        user['table'] = table
        record = {'Data': json.dumps(user), 'PartitionKey': str(hash(table))}
        i += 1
        gen_records.append(record)
        if i % batch_size == 0:
            put_record_result = client.put_records(StreamName=stream_name, Records=gen_records)
            gen_records = []
            print "Insert batch of %d records" % batch_size
    print "Insert %d records" % records
    time.sleep(sleep_time)
