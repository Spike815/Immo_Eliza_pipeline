import pandas as pd
from io import StringIO
import logging
import boto3
from botocore.exceptions import ClientError
import os
from pathlib import Path
from dotenv import load_dotenv

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


def data_to_csv(data_list, extention):
    df = pd.DataFrame(data_list)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3=s3_obj()
    s3.put_object(Bucket='immostudy-temp',Key=f"csv_files/{extention}",Body=csv_buffer.getvalue())

def csv_to_data(path):
    s3=s3_obj()
    s3_csv_obj = s3.get_object(Bucket='immostudy-temp', Key=path)
    return s3_csv_obj

