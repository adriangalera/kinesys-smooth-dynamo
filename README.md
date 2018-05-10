# kinesys-smooth-dynamo

Experiments to implement a queue of messages to be stored in DynamoDB at a controlled speed

## Lambda

Kinesis streams can be configured as an event source to a Lambda. However this does not provide any pace control.
AWS polls the stream and when new items appear the Lambda function is called and items are passed to the Lambda by batches of configured size

This option does not provide any speed control, as we don't have any control about the stream pooling

## ECS

Running the task as a server can make the input smooth at dynamo DB at the cost of incrementing the iterator age.
This could provide an efficient way to buffer the data incoming to the DynamoDB.
Controlling the number of items fetched from the stream and the sleep time we can control the insertion speed on dynamo DB.