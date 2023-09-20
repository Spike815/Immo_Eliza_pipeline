import logging
import boto3
from botocore.exceptions import ClientError
import os
from pathlib import Path
from dotenv import load_dotenv
from keys import get_access, get_secret

# def configure():
#     load_dotenv("../env")

def s3_obj():
    # configure()
    s3 = boto3.client(
        service_name='s3',
        region_name='eu-west-3',
        aws_access_key_id=get_access(),
        aws_secret_access_key=get_secret()
    )
    return s3


# s3 =s3_obj()
# file = Path("working/utls/Belgium_Postalcode.csv")
# s3.upload_file(Bucket = "",Filename=file, Key=f"csv_files/{file.name}")

# s3.Bucket('immostudy-temp').download_file(f'csv_files/{file.name}', 'test.csv')