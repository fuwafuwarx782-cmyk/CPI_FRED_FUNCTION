import boto3
import os
import pandas as pd
from io import StringIO

S3_BUCKET_NAME = os.environ.get("lambda-athena-shoefuwa20251023")

def S3_common_load(file_key: str):
    try:
        s3 = boto3.client('s3')
        
        response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=file_key)
        csv_content = response['Body'].read().decode('utf-8')
        
        df = pd.read_csv(StringIO(csv_content))
        print("S3からデータを読み込み完了！")
        return df
        
    except Exception as e:
        print(f"S3からの読み込み中にエラー発生！: {e}")
        raise