import json
import urllib.parse
import boto3
import csv

print('Loading function')

s3 = boto3.client('s3')


def lambda_handler(event, context):
#print("Received event: " + json.dumps(event, indent=2))

# Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    csvfile = s3.get_object(Bucket=bucket, Key=key)
    data = csv.reader(csvfile.splitlines())

        # Iterate through the rows in the CSV
    for row in data:
        # Process each row as needed
        print(row)