import boto3
import json

# Replace with your SQS queue URL
queue_url = 'https://sqs.us-east-1.amazonaws.com/044963436109/queue-1'

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
object_name = json_data['Records'][0]['s3']['object']['key'].split('/')[-1]
print(object_name)


# create a client for S3
s3 = boto3.client('s3')

bucket_name = 'message-queue-lab'
object_key = 'original/'+object_name
download_path = '/home/el-nabawy/' + object_name

s3.download_file(bucket_name, object_key, download_path)


try:
    file1 = open(download_path, "r")
except Exception:
    print ('unable to open file')
else:
    users = file1.readlines()

users = users[0].split(',')
users_str = '\n'.join(users[::])

try:
    file1 = open(download_path, "w")
    file1.writelines(users_str)
except Exception:
    print("unable to open file")


file_path = '/home/el-nabawy/' + object_name
s3_key = 'replication/' + object_name # optional: specify a folder and/or a different file name

with open(file_path, 'rb') as file:
    s3.upload_fileobj(file, bucket_name, s3_key)