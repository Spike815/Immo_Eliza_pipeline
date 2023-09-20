import logging
import boto3
from botocore.exceptions import ClientError
import os
from pathlib import Path
from dotenv import load_dotenv

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def configure():
    load_dotenv("../env")

def s3_obj():
    configure()
    s3 = boto3.client(
        service_name='s3',
        region_name='eu-west-3',
        aws_access_key_id=os.getenv('ACCESS'),
        aws_secret_access_key=os.getenv('SECRET')
    )
    return s3


# s3 =s3_obj()
# file = Path("working/utls/Belgium_Postalcode.csv")
# s3.upload_file(Bucket = "",Filename=file, Key=f"csv_files/{file.name}")

# s3.Bucket('immostudy-temp').download_file(f'csv_files/{file.name}', 'test.csv')