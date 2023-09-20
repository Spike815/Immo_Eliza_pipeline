import pandas as pd
from io import StringIO
import logging
import boto3
from botocore.exceptions import ClientError
from keys import get_access,get_secret
from dotenv import load_dotenv
import os

def configure():
    load_dotenv("../.env")


def s3_obj():
    # configure()
    s3 = boto3.client(
        service_name='s3',
        region_name='eu-west-3',
        aws_access_key_id=get_access(),
        aws_secret_access_key=get_secret()
    )
    return s3


def data_to_csv(data, extention):
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    s3=s3_obj()
    s3.put_object(Bucket='immostudy-temp',Key=f"csv_files/{extention}",Body=csv_buffer.getvalue())

def csv_to_data(path):
    s3=s3_obj()
    s3_csv_obj = s3.get_object(Bucket='immostudy-temp', Key=path)
    return s3_csv_obj

