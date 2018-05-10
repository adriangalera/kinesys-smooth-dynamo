import boto3
import testdata
import json
import time

stream_name = "test-streaming"
client = boto3.client('kinesis')


class Users(testdata.DictFactory):
    firstname = testdata.FakeDataFactory('firstName')
    lastname = testdata.FakeDataFactory('lastName')
    age = testdata.RandomInteger(100, 300)
    gender = testdata.RandomSelection(['female', 'male'])


records = 50
while True:
    for user in Users().generate(records):
        put_record_result = client.put_record(StreamName=stream_name, Data=json.dumps(user),
                                              PartitionKey="partitionkey")
    print "Insert %d records" % records
    time.sleep(1)
