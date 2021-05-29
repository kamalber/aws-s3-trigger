import boto3
import json
import urllib.parse
import csv
import io
from botocore.exceptions import NoCredentialsError
import botocore
from boto3.session import Session

CSV_PATH ="/tmp/data_file.csv"

S3_ACCESS_KEY = 'aws_key'
S3_SECRET_KEY = 'aws_secret'
S3_BUCKET='bucket_name'

s3 = boto3.client('s3', aws_access_key_id=S3_ACCESS_KEY, aws_secret_access_key=S3_SECRET_KEY)

def uploadFileToS3(file_name,object_name):

    object_name=object_name.replace(".json",".csv")
    try:
      s3.upload_file(file_name, S3_BUCKET, object_name)
    except Exception as e:
        print(e)

def getFileFromS3(bucket,key):
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        print("CONTENT TYPE: " + response['ContentType'])
        data=[]

        for i,line in enumerate(response['Body'].iter_lines()):
           object = line.decode('utf-8')
           data.append(object)

        return json.dumps(data)
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

"""
def getAllObjects():
    session = Session(aws_access_key_id=S3_ACCESS_KEY,
                  aws_secret_access_key=S3_SECRET_KEY)
    s3 = session.resource('s3')
    my_bucket = s3.Bucket(S3_BUCKET)
    files=[]

    for s3_file in my_bucket.objects.all():
        files.append(s3_file.key)
        print(s3_file.key)
    return files
"""


def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    # get the json file from s3
    data=getFileFromS3(bucket,key)
    data=json.loads(data)

# now we will open a file for writing
    data_file = open(CSV_PATH, 'w',newline='')

# create the csv writer object
    csv_writer = csv.writer(data_file)

    flag = False
    for record in data:
      record_data=json.loads(record)
      if flag == False:
        header = record_data.keys()
        # Writing headers of CSV file
        csv_writer.writerow(header)
        flag=True

    # Writing every record to the CSV file
      csv_writer.writerow(record_data.values())

    data_file.close()
    # upload csv to S3
    uploadFileToS3(CSV_PATH,key)


