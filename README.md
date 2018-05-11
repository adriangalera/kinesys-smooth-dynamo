# kinesys-smooth-dynamo

Experiments to implement a queue of messages to be stored in DynamoDB at a controlled speed

## Lambda

Kinesis streams can be configured as an event source to a Lambda. However this does not provide any pace control.
AWS polls the stream and when new items appear the Lambda function is called and items are passed to the Lambda by batches of configured size

This option does not provide any speed control, as we don't have any control about the stream pooling

Furthermore, when boto3 apply the exponential backoff mechanism (when writes are faster than the configured capacity), lambda functions will fail
because of timeout. The backoff mechanism waits every iteration a higher time until a max number of retries are tried. 
This waiting time can be so big that it exhaust the lambda timeout time.

So, Lambda is not recommended when there's a variable flow of input data for DynamoDB.

## ECS

Running the task with a server approach can make the input smooth at dynamo DB at the cost 
of incrementing the iterator age.

This could provide an efficient way to buffer the data incoming to the DynamoDB.

The data is consumed ib batches from the stream by a while True loop. After each iteration, 
Kinesis recommends to sleep in order to don't exhaust the provisioned throughput. Hence, we hace
two parameters that can control the data ingestion:

- Batch size
- Sleep time

Controlling the number of items fetched from the stream and the sleep time we can control the insertion speed on dynamo DB.

Since in this scenario, we don't have any time limitation, we delegate the smoothering to boto3
and its exponential backoff mechanism: the input traffic will adapt the provisioned capacity on the long run.

### Performance Boost

If the insert items are grouped in batches of 25 (DynamoDB Batch limitation), we can speed up the 
insertion process a lot. 

Furthermore, we can delegate the insertion of each batch to a separate thread and the same
exponential backoff mechanism will still applied while achiving a higher performance since each batch is treated
independently.

### Writing to multiple tables 

Since a stream can have multiple shards, we can create as many shards in the stream as tables we want to populate.

Then, the consumer application will treat each shard in a separate thread, with this feature
we are able even to configure different insert performance for different tables