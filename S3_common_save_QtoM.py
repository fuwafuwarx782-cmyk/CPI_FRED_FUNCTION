import boto3
import os
from datetime import datetime

S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")

def S3_common_save(csv_string: str, file_name: str, s3_prefix: str): 
   
    try:
        s3 = boto3.client('s3') 
        s3_key = f"{s3_prefix}/{file_name}" 
        
        s3.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=csv_string.encode('utf-8')
        )
        
    except Exception as e:
        raise