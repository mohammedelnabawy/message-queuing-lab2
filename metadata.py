import boto3
import json

# Replace with your SQS queue URL
queue_url = 'https://sqs.us-east-1.amazonaws.com/044963436109/queue-2'

sqs = boto3.client('sqs')
# Receive a message from the SQS queue
response = sqs.receive_message(
    QueueUrl=queue_url,
    MaxNumberOfMessages=1,
    WaitTimeSeconds=20
)

# # Check if a message was received
if 'Messages' in response:
    # Process the received message
    message = response['Messages'][0]
    print(f"Received message: {message['Body']}")

    # Delete the received message from the SQS queue
    receipt_handle = message['ReceiptHandle']
    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
else:
    print("No messages in the queue.")

json_data = json.loads(json.loads(message['Body'])['Message'])

# Print the JSON data
object_name = json_data['Records'][0]['s3']['object']
object_data = json.dumps(object_name)
path= './metadata.csv'

try:
    file1 = open(path, "a")
    file1.writelines(object_data)
    file1.write("\n")
except Exception:
    print("unable to open file")
